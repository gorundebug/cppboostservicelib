#pragma once

#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <future>
#include <limits>
#include <list>
#include <map>
#include <memory>
#include <optional>
#include <stop_token>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include <servicelib/runtime/detail/asio_dispatch.hpp>
#include <servicelib/runtime/environment/environment.hpp>
#include <servicelib/runtime/pool/pool.hpp>

namespace servicelib::pool::detail_pool {

// Only queue management runs on the strand. User callbacks are posted to the
// service executor independently, with a slot held until completion.
template <bool Priority>
class QueuedPool {
  static constexpr std::uint64_t kClosed = std::uint64_t{1} << 63;
  static constexpr std::uint64_t kStarted = std::uint64_t{1} << 62;
  static constexpr std::uint64_t kCount = kStarted - 1;
  using CancelCallback = std::stop_callback<std::function<void()>>;
  struct Task;
  using Queue = std::conditional_t<
      Priority, std::map<std::pair<int, std::uint64_t>, std::shared_ptr<Task>>,
      std::list<std::shared_ptr<Task>>>;
  using Deadlines =
      std::multimap<std::chrono::steady_clock::time_point, std::weak_ptr<Task>>;

  struct State {
    State(std::string poolName, IServiceEnvironment& environment)
        : name(std::move(poolName)),
          env(environment),
          executor(detail::ParallelExecutorRegistry::Get()),
          strand(boost::asio::make_strand(executor)),
          managerTimer(strand),
          lifecycleDeadline(strand),
          deadlineTimer(strand),
          drained(drainPromise.get_future().share()) {
      const auto config = env.getRuntimeConfigSnapshot();
      const auto* pool = config ? config->GetPoolByName(name) : nullptr;
      if (!pool)
        throw std::invalid_argument("task pool configuration not found: " +
                                    name);
      fallbackExecutors = pool->executorsCount;
      target.store(resolveExecutors(fallbackExecutors));
      const auto serviceSnapshot = env.getServiceConfigSnapshot();
      const auto* service = serviceSnapshot.get();
      metricsEnabled = env.getMetrics().enabled();
      auto scope = env.getMetrics().scope(
          Priority ? "priority_task_pool" : "task_pool",
          metrics::Labels{{"service", service ? service->name : std::string()},
                          {"name", name}});
      gaugeQueueLength =
          scope->gauge("queue_length", "Task pool wait queue length");
      gaugeExecutorsTarget = scope->gauge(
          "executors_target", "Desired number of task pool executors");
      gaugeExecutorsAllocated = scope->gauge(
          "executors_allocated", "Number of live task pool executors");
      gaugeExecutorsBusy = scope->gauge(
          "executors_busy", "Number of task pool executors running callbacks");
      tasksTotal = scope->counter(
          "tasks_total", "Total number of tasks executed by task pool");
      executionDuration =
          scope->histogram("task_execution_duration_seconds",
                           "Task execution duration in seconds");
      stopTimeoutCounter =
          scope->counter("events_total", "Total number of events in task pool",
                         {{"event", "stop_timeout"}});
      taskRejectedCounter =
          scope->counter("events_total", "Total number of events in task pool",
                         {{"event", "task_rejected"}});
      taskCancelledCounter = scope->counter(
          "events_total", "Total number of events in task pool",
          {{"event", Priority ? "task_expired" : "task_cancelled"}});
    }
    std::string name;
    IServiceEnvironment& env;
    boost::asio::any_io_executor executor;
    boost::asio::strand<boost::asio::any_io_executor> strand;
    boost::asio::steady_timer managerTimer, lifecycleDeadline, deadlineTimer;
    std::atomic<std::uint64_t> activity{0};
    std::atomic<int> target{0};
    int fallbackExecutors{};
    // All fields below, except immutable metrics/drain future, are
    // strand-owned.
    bool started{}, stopping{}, managerActive{true}, completed{};
    std::size_t busy{};
    std::uint64_t nextSequence{}, deadlineGeneration{};
    Queue queue;
    Deadlines deadlines;
    std::optional<std::chrono::steady_clock::time_point> armedAt;
    std::vector<std::unique_ptr<CancelCallback>> lifecycleCancellations;
    std::promise<void> drainPromise;
    std::shared_future<void> drained;
    bool metricsEnabled{};
    std::unique_ptr<metrics::Int64Gauge> gaugeQueueLength, gaugeExecutorsTarget,
        gaugeExecutorsAllocated, gaugeExecutorsBusy;
    std::unique_ptr<metrics::Int64Counter> tasksTotal, stopTimeoutCounter,
        taskRejectedCounter, taskCancelledCounter;
    std::unique_ptr<metrics::Float64Histogram> executionDuration;
  };

  struct Task {
    std::function<void()> function;
    std::vector<std::unique_ptr<CancelCallback>> cancellations;
    std::optional<std::chrono::steady_clock::time_point> deadline;
    std::optional<typename Queue::iterator> position;
    std::optional<typename Deadlines::iterator> deadlinePosition;
    std::atomic<bool> cancelled{false};
    bool promoted{};
  };

 public:
  QueuedPool(std::string name, IServiceEnvironment& env)
      : state_(std::make_shared<State>(std::move(name), env)) {}

  ~QueuedPool() {
    const auto activity = state_->activity.load(std::memory_order_acquire);
    if ((activity & kCount) || ((activity & kStarted) && !(activity & kClosed)))
      std::terminate();
  }

  const std::string& getName() const noexcept { return state_->name; }
  int getExecutorsCount() const {
    return state_->target.load(std::memory_order_acquire);
  }

  void start(Context context) {
    const auto state = state_;
    const auto count = configuredExecutors(*state);
    auto activity = state->activity.load(std::memory_order_acquire);
    for (;;) {
      if (activity & kClosed) throw PoolStoppedError();
      if (activity & kStarted) throw PoolAlreadyStartedError();
      if (state->activity.compare_exchange_weak(activity, activity | kStarted,
                                                std::memory_order_acq_rel))
        break;
    }
    state->target.store(count, std::memory_order_release);
    boost::asio::post(state->strand, [state, context = std::move(context)] {
      if (state->stopping) return;
      state->started = true;
      installLifecycle(state, context);
      if (state->managerActive) scheduleManager(state);
      dispatch(state);
    });
  }

  void stop(Context context) {
    const auto state = state_;
    if (currentExecutingPool_ == state.get()) throw PoolSelfStopError();
    if (!(state->activity.fetch_or(kClosed, std::memory_order_acq_rel) &
          kClosed)) {
      boost::asio::post(state->strand, [state] {
        state->stopping = true;
        state->started = true;  // drain accepted pre-start work too
        stopManager(state);
        state->lifecycleCancellations.clear();
        dispatch(state);
      });
    }
    // Synchronous lifecycle API; ServiceApp calls this off the reactor.
    // All stop callers wait for the same completion.
    if (context.deadline() && state->drained.wait_until(*context.deadline()) ==
                                  std::future_status::timeout) {
      bestEffort([&] {
        state->stopTimeoutCounter->inc();
        state->env.getLogger().warn("task pool stopped by timeout",
                                    {log::Field::Str("pool", state->name)});
      });
    }
    state->drained.wait();
  }

  void addTask(Context context, int priority, std::function<void()> function) {
    const auto state = state_;
    if (context.cancelled()) {
      bestEffort([&] { state->taskRejectedCounter->inc(); });
      throw PoolCancelledError();
    }
    auto task = std::make_shared<Task>();
    task->function = std::move(function);
    task->deadline = context.deadline();
    const std::weak_ptr<State> weakState(state);
    const std::weak_ptr<Task> weakTask(task);
    const auto cancel = [weakState, weakTask] {
      const auto task = weakTask.lock();
      const auto state = weakState.lock();
      if (!task || !state || task->cancelled.exchange(true)) return;
      boost::asio::post(state->strand, [weakState, weakTask] {
        const auto state = weakState.lock();
        const auto task = weakTask.lock();
        if (state && task && task->position) {
          promote(state, task);
          dispatch(state);
        }
      });
    };
    if (context.stopToken().stop_possible())
      task->cancellations.push_back(
          std::make_unique<CancelCallback>(context.stopToken(), cancel));
    for (const auto& token : context.externalStopTokens()) {
      if (token.stop_possible())
        task->cancellations.push_back(
            std::make_unique<CancelCallback>(token, cancel));
    }
    // Reserve queue/deadline nodes before admission. Strand insertion then
    // transfers ownership without allocating after accepting the callback.
    Queue pending;
    if constexpr (Priority)
      pending.emplace(std::pair{priority, std::uint64_t{0}}, task);
    else
      pending.push_back(task);
    Deadlines pendingDeadline;
    if (task->deadline) pendingDeadline.emplace(*task->deadline, task);
    auto activity = state->activity.load(std::memory_order_acquire);
    for (;;) {
      if (activity & kClosed) {
        bestEffort([&] { state->taskRejectedCounter->inc(); });
        throw PoolStoppedError();
      }
      if ((activity & kCount) == kCount)
        throw std::overflow_error("too many queued tasks");
      if (state->activity.compare_exchange_weak(activity, activity + 1,
                                                std::memory_order_acq_rel))
        break;
    }
    try {
      boost::asio::post(
          state->strand,
          [state, task, pending = std::move(pending),
           pendingDeadline = std::move(pendingDeadline)]() mutable {
            if constexpr (Priority) {
              auto node = pending.extract(pending.begin());
              node.key().second = state->nextSequence++;
              task->position = state->queue.insert(std::move(node)).position;
            } else {
              state->queue.splice(state->queue.end(), pending);
              task->position = std::prev(state->queue.end());
            }
            if (task->cancelled.load() ||
                (task->deadline &&
                 *task->deadline <= std::chrono::steady_clock::now())) {
              promote(state, task);
            } else if (task->deadline) {
              task->deadlinePosition = state->deadlines.insert(
                  pendingDeadline.extract(pendingDeadline.begin()));
            }
            dispatch(state);
          });
    } catch (...) {
      state->activity.fetch_sub(1, std::memory_order_acq_rel);
      boost::asio::post(state->strand, [state] { checkDrain(state); });
      throw;
    }
  }

 private:
  template <typename F>
  static void bestEffort(F&& fn) noexcept {
    try {
      fn();
    } catch (...) {
    }
  }
  static int resolveExecutors(int count) {
    if (count < 0) throw std::invalid_argument("negative executor count");
    return count ? count
                 : static_cast<int>(
                       std::max(1u, std::thread::hardware_concurrency()));
  }
  static int configuredExecutors(const State& state) {
    const auto config = state.env.getRuntimeConfigSnapshot();
    const auto* pool = config ? config->GetPoolByName(state.name) : nullptr;
    return resolveExecutors(pool ? pool->executorsCount
                                 : state.fallbackExecutors);
  }
  static void publish(State& state) {
    if (!state.metricsEnabled) return;
    bestEffort([&] {
      state.gaugeQueueLength->set(
          static_cast<std::int64_t>(state.queue.size()));
      const auto target = state.target.load();
      state.gaugeExecutorsTarget->set(target);
      state.gaugeExecutorsAllocated->set(
          state.completed ? 0
          : state.started ? std::max<std::size_t>(target, state.busy)
                          : 0);
      state.gaugeExecutorsBusy->set(static_cast<std::int64_t>(state.busy));
    });
  }
  static void checkDrain(const std::shared_ptr<State>& state) {
    if (state->stopping && !state->completed &&
        (state->activity.load(std::memory_order_acquire) & kCount) == 0) {
      state->completed = true;
      publish(*state);
      state->drainPromise.set_value();
    }
  }
  static void stopManager(const std::shared_ptr<State>& state) {
    state->managerActive = false;
    state->managerTimer.cancel();
    state->lifecycleDeadline.cancel();
  }
  static void installLifecycle(const std::shared_ptr<State>& state,
                               const Context& context) {
    // Go's Start context controls the resize manager, not task admission.
    if (context.cancelled()) {
      stopManager(state);
      return;
    }
    const std::weak_ptr<State> weak(state);
    const auto cancel = [weak] {
      if (const auto state = weak.lock())
        boost::asio::post(state->strand, [weak] {
          if (const auto state = weak.lock()) stopManager(state);
        });
    };
    if (context.stopToken().stop_possible())
      state->lifecycleCancellations.push_back(
          std::make_unique<CancelCallback>(context.stopToken(), cancel));
    for (const auto& token : context.externalStopTokens()) {
      if (token.stop_possible())
        state->lifecycleCancellations.push_back(
            std::make_unique<CancelCallback>(token, cancel));
    }
    if (context.deadline()) {
      state->lifecycleDeadline.expires_at(*context.deadline());
      state->lifecycleDeadline.async_wait(
          [cancel](const boost::system::error_code& error) {
            if (!error) cancel();
          });
    }
  }
  static void scheduleManager(const std::shared_ptr<State>& state) {
    state->managerTimer.expires_after(std::chrono::seconds(1));
    const std::weak_ptr<State> weak(state);
    state->managerTimer.async_wait(
        [weak](const boost::system::error_code& error) {
          const auto state = weak.lock();
          if (error || !state || !state->managerActive || state->stopping)
            return;
          bestEffort([&] { state->target.store(configuredExecutors(*state)); });
          dispatch(state);
          scheduleManager(state);
        });
  }
  static void removeDeadline(const std::shared_ptr<State>& state,
                             const std::shared_ptr<Task>& task) {
    if (task->deadlinePosition) {
      state->deadlines.erase(*task->deadlinePosition);
      task->deadlinePosition.reset();
    }
  }
  static void promote(const std::shared_ptr<State>& state,
                      const std::shared_ptr<Task>& task) {
    if (!task->position || task->promoted) return;
    task->promoted = true;
    removeDeadline(state, task);
    if constexpr (Priority) {
      auto node = state->queue.extract(*task->position);
      node.key().first = std::numeric_limits<int>::min();
      task->position = state->queue.insert(std::move(node)).position;
    } else {
      if (*task->position == state->queue.begin()) return;
      state->queue.splice(state->queue.begin(), state->queue, *task->position);
      task->position = state->queue.begin();
    }
    bestEffort([&] { state->taskCancelledCounter->inc(); });
  }
  static void armDeadline(const std::shared_ptr<State>& state) {
    const auto next =
        state->deadlines.empty()
            ? std::optional<std::chrono::steady_clock::time_point>{}
            : std::optional{state->deadlines.begin()->first};
    if (next == state->armedAt) return;
    state->armedAt = next;
    const auto generation = ++state->deadlineGeneration;
    if (!next) {
      state->deadlineTimer.cancel();
      return;
    }
    state->deadlineTimer.expires_at(*next);
    const std::weak_ptr<State> weak(state);
    state->deadlineTimer.async_wait(
        [weak, generation](const boost::system::error_code& error) {
          const auto state = weak.lock();
          if (!state || error || generation != state->deadlineGeneration)
            return;
          state->armedAt.reset();
          const auto now = std::chrono::steady_clock::now();
          while (!state->deadlines.empty() &&
                 state->deadlines.begin()->first <= now) {
            auto task = state->deadlines.begin()->second.lock();
            state->deadlines.erase(state->deadlines.begin());
            if (task) {
              task->deadlinePosition.reset();
              promote(state, task);
            }
          }
          dispatch(state);
        });
  }
  static void dispatch(const std::shared_ptr<State>& state) {
    while (state->started &&
           state->busy < static_cast<std::size_t>(state->target.load()) &&
           !state->queue.empty()) {
      std::shared_ptr<Task> task;
      if constexpr (Priority)
        task = state->queue.begin()->second;
      else
        task = state->queue.front();
      state->queue.erase(state->queue.begin());
      task->position.reset();
      removeDeadline(state, task);
      ++state->busy;
      publish(*state);
      boost::asio::post(state->executor, [state, task] {
        task->cancellations.clear();
        const auto previous = currentExecutingPool_;
        currentExecutingPool_ = state.get();
        const auto started = state->metricsEnabled
                                 ? std::chrono::steady_clock::now()
                                 : std::chrono::steady_clock::time_point{};
        try {
          task->function();
        } catch (const std::exception& error) {
          bestEffort([&] {
            state->env.getLogger().warn(
                "task pool task error",
                {log::Field::Str("pool", state->name), log::Field::Err(error)});
          });
        } catch (...) {
          bestEffort(
              [&] { state->env.getLogger().warn("task pool task error"); });
        }
        currentExecutingPool_ = previous;
        task->function = nullptr;
        if (state->metricsEnabled)
          bestEffort([&] {
            state->tasksTotal->inc();
            state->executionDuration->observe(
                std::chrono::duration<double>(std::chrono::steady_clock::now() -
                                              started)
                    .count());
          });
        boost::asio::post(state->strand, [state] {
          --state->busy;
          state->activity.fetch_sub(1, std::memory_order_acq_rel);
          dispatch(state);
        });
      });
    }
    publish(*state);
    armDeadline(state);
    checkDrain(state);
  }
  inline static thread_local const State* currentExecutingPool_{};
  std::shared_ptr<State> state_;
};
}  // namespace servicelib::pool::detail_pool

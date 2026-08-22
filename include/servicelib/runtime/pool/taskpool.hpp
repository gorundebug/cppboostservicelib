/*
 * taskpool.hpp
 * C++ streams API — FIFO task pool on the service Asio executor.
 */
#pragma once

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <stop_token>
#include <string>
#include <utility>
#include <vector>

#include <servicelib/runtime/detail/asio_dispatch.hpp>
#include <servicelib/runtime/environment/environment.hpp>
#include <servicelib/runtime/pool/pool.hpp>

namespace servicelib::pool {

class TaskPoolImpl final : public ITaskPool {
 private:
  enum class PoolState { kCreated, kRunning, kDraining, kStopping, kStopped };
  using CancelCallback = std::stop_callback<std::function<void()>>;
  struct PoolTask;

  struct SharedState final {
    SharedState(std::string poolName, IServiceEnvironment& environment)
        : name(std::move(poolName)),
          env(environment),
          executor(detail::ParallelExecutorRegistry::Get()),
          managerTimer(executor),
          lifecycleDeadline(executor) {
      const auto config = env.getRuntimeConfigSnapshot();
      const auto* pool = config ? config->GetPoolByName(name) : nullptr;
      if (!pool) {
        throw std::invalid_argument("task pool configuration named '" + name +
                                    "' not found");
      }
      const auto serviceSnapshot = env.getServiceConfigSnapshot();
      const auto* service = serviceSnapshot.get();
      metricsEnabled = env.getMetrics().enabled();
      auto scope = env.getMetrics().scope(
          "task_pool",
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
      executionDuration = scope->histogram(
          "task_execution_duration_seconds", "Task execution duration in seconds");
      stopTimeoutCounter =
          scope->counter("events_total", "Total number of events in task pool",
                         {{"event", "stop_timeout"}});
      taskRejectedCounter =
          scope->counter("events_total", "Total number of events in task pool",
                         {{"event", "task_rejected"}});
      taskCancelledCounter =
          scope->counter("events_total", "Total number of events in task pool",
                         {{"event", "task_cancelled"}});
    }

    std::string name;
    IServiceEnvironment& env;
    boost::asio::any_io_executor executor;
    mutable std::mutex mutex;
    std::condition_variable drained;
    PoolState state{PoolState::kCreated};
    std::list<std::shared_ptr<PoolTask>> queue;
    std::size_t busy{};
    std::size_t targetExecutors{};
    bool metricsEnabled{};
    boost::asio::steady_timer managerTimer;
    boost::asio::steady_timer lifecycleDeadline;
    std::vector<std::unique_ptr<CancelCallback>> lifecycleCancellations;
    std::unique_ptr<metrics::Int64Gauge> gaugeQueueLength;
    std::unique_ptr<metrics::Int64Gauge> gaugeExecutorsTarget;
    std::unique_ptr<metrics::Int64Gauge> gaugeExecutorsAllocated;
    std::unique_ptr<metrics::Int64Gauge> gaugeExecutorsBusy;
    std::unique_ptr<metrics::Int64Counter> tasksTotal;
    std::unique_ptr<metrics::Float64Histogram> executionDuration;
    std::unique_ptr<metrics::Int64Counter> stopTimeoutCounter;
    std::unique_ptr<metrics::Int64Counter> taskRejectedCounter;
    std::unique_ptr<metrics::Int64Counter> taskCancelledCounter;
  };

  struct PoolTask final {
    std::function<void()> function;
    std::vector<std::unique_ptr<CancelCallback>> cancellations;
    std::unique_ptr<boost::asio::steady_timer> deadlineTimer;
    std::list<std::shared_ptr<PoolTask>>::iterator position;
    bool queued{};
    bool promoted{};
  };

 public:
  TaskPoolImpl(std::string name, IServiceEnvironment& env)
      : state_(std::make_shared<SharedState>(std::move(name), env)) {}

  ~TaskPoolImpl() override {
    const auto state = state_;
    std::lock_guard lock(state->mutex);
    if (state->state != PoolState::kCreated &&
        state->state != PoolState::kStopped) {
      std::terminate();
    }
  }

  const std::string& getName() const noexcept override { return state_->name; }

  int getExecutorsCount() const override {
    std::lock_guard lock(state_->mutex);
    return static_cast<int>(state_->targetExecutors);
  }

  void start(Context ctx) override {
    const auto state = state_;
    std::lock_guard lock(state->mutex);
    if (state->state == PoolState::kDraining ||
        state->state == PoolState::kStopping ||
        state->state == PoolState::kStopped) {
      throw PoolStoppedError();
    }
    if (state->state != PoolState::kCreated) throw PoolAlreadyStartedError();
    if (ctx.cancelled()) throw PoolCancelledError();

    const auto config = state->env.getRuntimeConfigSnapshot();
    const auto* pool = config ? config->GetPoolByName(state->name) : nullptr;
    if (!pool || pool->executorsCount <= 0) {
      throw std::runtime_error("failed to start task pool '" + state->name +
                               "'");
    }
    state->targetExecutors = static_cast<std::size_t>(pool->executorsCount);
    state->state = PoolState::kRunning;
    publishExecutorGaugesLocked(*state);
    installLifecycleCancellationLocked(state, ctx);
    scheduleManagerLocked(state);
    dispatchLocked(state);
  }

  void stop(Context ctx) override {
    const auto state = state_;
    if (currentExecutingPool_ == state.get()) throw PoolSelfStopError();

    std::unique_lock lock(state->mutex);
    if (state->state == PoolState::kStopped) return;
    if (state->state == PoolState::kStopping) {
      state->drained.wait(lock,
                          [state] { return state->state == PoolState::kStopped; });
      return;
    }
    if (state->state == PoolState::kCreated) {
      state->state = PoolState::kStopped;
      return;
    }
    state->state = PoolState::kStopping;
    state->managerTimer.cancel();
    state->lifecycleDeadline.cancel();
    state->lifecycleCancellations.clear();
    dispatchLocked(state);

    bool timedOut = false;
    if (const auto& deadline = ctx.deadline(); deadline) {
      timedOut = !state->drained.wait_until(lock, *deadline, [state] {
        return state->queue.empty() && state->busy == 0;
      });
    }
    if (timedOut) {
      const auto tasksLeft = static_cast<std::int64_t>(state->queue.size() +
                                                       state->busy);
      lock.unlock();
      bestEffort([state, tasksLeft] {
        state->env.getLogger().warn(
            "task pool stopped by timeout",
            {log::Field::Str("pool", state->name),
             log::Field::Int64("tasks_count", tasksLeft)});
      });
      bestEffort([state] { state->stopTimeoutCounter->inc(); });
      lock.lock();
    }
    state->drained.wait(lock, [state] {
      return state->queue.empty() && state->busy == 0;
    });
    state->state = PoolState::kStopped;
    state->targetExecutors = 0;
    publishExecutorGaugesLocked(*state);
    state->drained.notify_all();
  }

  void addTask(Context ctx, std::function<void()> function) override {
    const auto state = state_;
    if (ctx.cancelled() ||
        (ctx.deadline() &&
         *ctx.deadline() <= std::chrono::steady_clock::now())) {
      bestEffort([state] { state->taskRejectedCounter->inc(); });
      throw PoolCancelledError();
    }

    auto task = std::make_shared<PoolTask>();
    task->function = std::move(function);
    const std::weak_ptr<SharedState> weakState(state);
    const std::weak_ptr<PoolTask> weakTask(task);
    const auto promote = [weakState, weakTask]() noexcept {
      try {
        if (const auto lockedState = weakState.lock()) {
          boost::asio::post(lockedState->executor,
                            [weakState, weakTask] {
                              promoteToFront(weakState, weakTask);
                            });
        }
      } catch (...) {
      }
    };

    std::lock_guard lock(state->mutex);
    if (state->state != PoolState::kRunning) {
      bestEffort([state] { state->taskRejectedCounter->inc(); });
      if (state->state == PoolState::kDraining ||
          state->state == PoolState::kStopping ||
          state->state == PoolState::kStopped) {
        throw PoolStoppedError();
      }
      throw PoolNotStartedError();
    }
    if (ctx.cancelled() ||
        (ctx.deadline() &&
         *ctx.deadline() <= std::chrono::steady_clock::now())) {
      bestEffort([state] { state->taskRejectedCounter->inc(); });
      throw PoolCancelledError();
    }

    if (ctx.stopToken().stop_possible()) {
      task->cancellations.push_back(
          std::make_unique<CancelCallback>(ctx.stopToken(), promote));
    }
    for (const auto& token : ctx.externalStopTokens()) {
      if (token.stop_possible()) {
        task->cancellations.push_back(
            std::make_unique<CancelCallback>(token, promote));
      }
    }
    if (ctx.deadline()) {
      task->deadlineTimer = std::make_unique<boost::asio::steady_timer>(
          state->executor, *ctx.deadline());
      task->deadlineTimer->async_wait(
          [promote](const boost::system::error_code& error) {
            if (!error) promote();
          });
    }

    state->queue.push_back(task);
    task->position = std::prev(state->queue.end());
    task->queued = true;
    publishQueueGaugeLocked(*state);
    dispatchLocked(state);
  }

 private:
  template <typename Callback>
  static void bestEffort(Callback&& callback) noexcept {
    try {
      std::forward<Callback>(callback)();
    } catch (...) {
    }
  }

  static void installLifecycleCancellationLocked(
      const std::shared_ptr<SharedState>& state, const Context& ctx) {
    const std::weak_ptr<SharedState> weakState(state);
    const auto cancel = [weakState]() noexcept {
      try {
        if (const auto locked = weakState.lock()) {
          boost::asio::post(locked->executor,
                            [weakState] { beginDrain(weakState); });
        }
      } catch (...) {
      }
    };
    if (ctx.stopToken().stop_possible()) {
      state->lifecycleCancellations.push_back(
          std::make_unique<CancelCallback>(ctx.stopToken(), cancel));
    }
    for (const auto& token : ctx.externalStopTokens()) {
      if (token.stop_possible()) {
        state->lifecycleCancellations.push_back(
            std::make_unique<CancelCallback>(token, cancel));
      }
    }
    if (ctx.deadline()) {
      state->lifecycleDeadline.expires_at(*ctx.deadline());
      state->lifecycleDeadline.async_wait(
          [cancel](const boost::system::error_code& error) {
            if (!error) cancel();
          });
    }
  }

  static void beginDrain(const std::weak_ptr<SharedState>& weakState) {
    const auto state = weakState.lock();
    if (!state) return;
    std::lock_guard lock(state->mutex);
    if (state->state != PoolState::kRunning) return;
    state->state = PoolState::kDraining;
    state->managerTimer.cancel();
    dispatchLocked(state);
  }

  static void scheduleManagerLocked(const std::shared_ptr<SharedState>& state) {
    state->managerTimer.expires_after(std::chrono::seconds(1));
    const std::weak_ptr<SharedState> weakState(state);
    state->managerTimer.async_wait(
        [weakState](const boost::system::error_code& error) {
          if (!error) managerTick(weakState);
        });
  }

  static void managerTick(const std::weak_ptr<SharedState>& weakState) {
    const auto state = weakState.lock();
    if (!state) return;
    std::lock_guard lock(state->mutex);
    if (state->state != PoolState::kRunning) return;
    try {
      const auto config = state->env.getRuntimeConfigSnapshot();
      const auto* pool = config ? config->GetPoolByName(state->name) : nullptr;
      const auto target =
          pool && pool->executorsCount > 0
              ? static_cast<std::size_t>(pool->executorsCount)
              : state->targetExecutors;
      state->targetExecutors = target;
      publishExecutorGaugesLocked(*state);
      dispatchLocked(state);
    } catch (const std::exception& error) {
      bestEffort([state, &error] {
        state->env.getLogger().warn(
            "task pool executor resize failed",
            {log::Field::Str("pool", state->name), log::Field::Err(error)});
      });
    }
    scheduleManagerLocked(state);
  }

  static void promoteToFront(const std::weak_ptr<SharedState>& weakState,
                             const std::weak_ptr<PoolTask>& weakTask) {
    const auto state = weakState.lock();
    const auto task = weakTask.lock();
    if (!state || !task) return;
    std::lock_guard lock(state->mutex);
    if (!task->queued || task->promoted) return;
    state->queue.erase(task->position);
    state->queue.push_front(task);
    task->position = state->queue.begin();
    task->promoted = true;
    bestEffort([state] { state->taskCancelledCounter->inc(); });
  }

  static void publishQueueGaugeLocked(SharedState& state) noexcept {
    if (!state.metricsEnabled) return;
    bestEffort([&state] {
      state.gaugeQueueLength->set(
          static_cast<std::int64_t>(state.queue.size()));
    });
  }

  static void publishExecutorGaugesLocked(SharedState& state) noexcept {
    if (!state.metricsEnabled) return;
    bestEffort([&state] {
      state.gaugeExecutorsTarget->set(
          static_cast<std::int64_t>(state.targetExecutors));
      state.gaugeExecutorsAllocated->set(static_cast<std::int64_t>(
          std::max(state.targetExecutors, state.busy)));
      state.gaugeExecutorsBusy->set(static_cast<std::int64_t>(state.busy));
    });
  }

  static void dispatchLocked(const std::shared_ptr<SharedState>& state) {
    while (state->busy < state->targetExecutors && !state->queue.empty()) {
      auto task = state->queue.front();
      state->queue.pop_front();
      task->queued = false;
      if (task->deadlineTimer) task->deadlineTimer->cancel();
      task->cancellations.clear();
      ++state->busy;
      publishQueueGaugeLocked(*state);
      publishExecutorGaugesLocked(*state);
      boost::asio::post(state->executor,
                        [state, task = std::move(task)] {
                          execute(state, task);
                        });
    }
    if ((state->state == PoolState::kDraining ||
         state->state == PoolState::kStopping) &&
        state->queue.empty() && state->busy == 0) {
      state->drained.notify_all();
    }
  }

  static void execute(const std::shared_ptr<SharedState>& state,
                      const std::shared_ptr<PoolTask>& task) {
    const auto* previous = currentExecutingPool_;
    currentExecutingPool_ = state.get();
    const auto started = state->metricsEnabled
                             ? std::chrono::steady_clock::now()
                             : std::chrono::steady_clock::time_point{};
    try {
      task->function();
    } catch (const std::exception& error) {
      bestEffort([state, &error] {
        state->env.getLogger().warn(
            "task pool task error",
            {log::Field::Str("pool", state->name), log::Field::Err(error)});
      });
    } catch (...) {
      bestEffort([state] {
        state->env.getLogger().warn(
            "task pool task error",
            {log::Field::Str("pool", state->name),
             log::Field::Str("error", "<unknown>")});
      });
    }
    currentExecutingPool_ = previous;
    if (state->metricsEnabled) {
      bestEffort([state] { state->tasksTotal->inc(); });
      const double elapsed = std::chrono::duration<double>(
                                 std::chrono::steady_clock::now() - started)
                                 .count();
      bestEffort(
          [state, elapsed] { state->executionDuration->observe(elapsed); });
    }

    std::lock_guard lock(state->mutex);
    --state->busy;
    publishExecutorGaugesLocked(*state);
    dispatchLocked(state);
  }

  inline static thread_local const SharedState* currentExecutingPool_{};
  std::shared_ptr<SharedState> state_;
};

inline std::unique_ptr<ITaskPool> makeTaskPool(std::string name,
                                               IServiceEnvironment& env) {
  return std::make_unique<TaskPoolImpl>(std::move(name), env);
}

}  // namespace servicelib::pool

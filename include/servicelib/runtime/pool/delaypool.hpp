/*
 * delaypool.hpp
 * C++ streams API — service-wide delayed task scheduler.
 */
#pragma once

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <exception>
#include <functional>
#include <future>
#include <map>
#include <memory>
#include <optional>
#include <stop_token>
#include <utility>
#include <vector>

#include <servicelib/runtime/detail/asio_dispatch.hpp>
#include <servicelib/runtime/environment/environment.hpp>
#include <servicelib/runtime/pool/pool.hpp>

namespace servicelib::pool {

class DelayPoolImpl final : public IDelayPool {
 private:
  static constexpr std::uint64_t kClosed = std::uint64_t{1} << 63;
  static constexpr std::uint64_t kStarted = std::uint64_t{1} << 62;
  static constexpr std::uint64_t kCountMask = kStarted - 1;
  struct SharedState;
  struct DelayTask;
  using TimerQueue = std::multimap<std::chrono::steady_clock::time_point,
                                    std::shared_ptr<DelayTask>>;
  using CancelCallback = std::stop_callback<std::function<void()>>;

  struct DelayTask final {
    std::shared_ptr<SharedState> state;
    Context ctx;
    std::function<void()> fn;
    std::optional<TimerQueue::iterator> queued;  // strand only
    bool expeditedByDeadline{};
    std::atomic<bool> claimed{false};
    std::atomic<bool> cancelRequested{false};
    std::optional<CancelCallback> cancelCallback;
    std::vector<std::unique_ptr<CancelCallback>> externalCancelCallbacks;
  };

  struct SharedState final {
    explicit SharedState(IServiceEnvironment& environment)
        : env(environment),
          executor(detail::ParallelExecutorRegistry::Get()),
          strand(boost::asio::make_strand(executor)), timer(strand),
          drained(drainPromise.get_future().share()) {
      const auto serviceSnapshot = env.getServiceConfigSnapshot();
      const auto* service = serviceSnapshot.get();
      metricsEnabled = env.getMetrics().enabled();
      auto scope = env.getMetrics().scope(
          "delay_pool", metrics::Labels{{"service", service ? service->name
                                                          : std::string()}});
      gaugeWaitQueueLength =
          scope->gauge("wait_queue_length", "Delay pool wait queue length");
      tasksTotal = scope->counter(
          "tasks_total", "Total number of tasks executed by delay pool");
      executionDuration =
          scope->histogram("task_execution_duration_seconds",
                           "Task execution duration in seconds");
      stopTimeoutCounter =
          scope->counter("events_total", "Total number of events in delay pool",
                         {{"event", "stop_timeout"}});
      taskCancelledCounter =
          scope->counter("events_total", "Total number of events in delay pool",
                         {{"event", "task_cancelled"}});
      taskRejectedCounter =
          scope->counter("events_total", "Total number of events in delay pool",
                         {{"event", "task_rejected"}});
    }

    IServiceEnvironment& env;
    boost::asio::any_io_executor executor;
    // Queue/timer ownership is confined to this strand, never user callbacks.
    boost::asio::strand<boost::asio::any_io_executor> strand;
    boost::asio::steady_timer timer;
    TimerQueue timers;
    std::optional<std::chrono::steady_clock::time_point> armedAt;
    std::uint64_t generation{};
    // Admission and shutdown are linearized without blocking reactor threads.
    std::atomic<std::uint64_t> activity{0};
    std::atomic<bool> drainSignalled{false};
    std::promise<void> drainPromise;
    std::shared_future<void> drained;
    bool metricsEnabled{};
    std::unique_ptr<metrics::Int64Gauge> gaugeWaitQueueLength;
    std::unique_ptr<metrics::Int64Counter> tasksTotal;
    std::unique_ptr<metrics::Float64Histogram> executionDuration;
    std::unique_ptr<metrics::Int64Counter> stopTimeoutCounter;
    std::unique_ptr<metrics::Int64Counter> taskCancelledCounter;
    std::unique_ptr<metrics::Int64Counter> taskRejectedCounter;
  };

 public:
  explicit DelayPoolImpl(IServiceEnvironment& env)
      : state_(std::make_shared<SharedState>(env)) {}

  ~DelayPoolImpl() override {
    const auto activity = state_->activity.load(std::memory_order_acquire);
    if ((activity & kCountMask) != 0 ||
        ((activity & kStarted) && !(activity & kClosed))) std::terminate();
  }

  void start([[maybe_unused]] Context ctx) override {
    auto activity = state_->activity.load(std::memory_order_acquire);
    for (;;) {
      if (activity & kClosed) throw PoolStoppedError();
      if (activity & kStarted) throw PoolAlreadyStartedError();
      if (state_->activity.compare_exchange_weak(activity, activity | kStarted,
                                                std::memory_order_acq_rel)) return;
    }
  }

  void stop(Context ctx) override {
    const auto state = state_;
    if (currentExecutingPool_ == state.get()) throw PoolSelfStopError();
    const auto activity = state->activity.fetch_or(kClosed, std::memory_order_acq_rel);
    if ((activity & kCountMask) == 0) signalDrained(state);
    // This synchronous lifecycle boundary is called off-reactor by ServiceApp.
    // Neither scheduling nor executing callbacks waits on a future or OS lock.
    if (ctx.deadline() && state->drained.wait_until(*ctx.deadline()) == std::future_status::timeout) {
      recordStopTimeout(state);
    }
    state->drained.wait();
  }

  void delay(Context ctx, Duration delayDuration,
             std::function<void()> fn) override {
    const auto state = state_;
    const auto now = std::chrono::steady_clock::now();
    if (ctx.cancelled()) rejectCancelled(state);

    auto runAt = saturatedAdd(now, delayDuration);
    bool expeditedByDeadline = false;
    if (const auto& deadline = ctx.deadline(); deadline && *deadline < runAt) {
      runAt = *deadline;
      expeditedByDeadline = true;
    }
    if (runAt <= now && ctx.deadline() && *ctx.deadline() <= now) {
      rejectCancelled(state);
    }

    auto task = std::make_shared<DelayTask>();
    task->state = state;
    task->ctx = std::move(ctx);
    task->fn = std::move(fn);
    task->expeditedByDeadline = expeditedByDeadline;

    const std::weak_ptr<DelayTask> weakTask(task);
    const auto onCancel = [weakTask] {
      if (const auto task = weakTask.lock()) {
        if (task->cancelRequested.exchange(true, std::memory_order_acq_rel)) return;
        boost::asio::post(task->state->strand, [weakTask] {
          if (const auto task = weakTask.lock(); task && task->queued) {
            auto state = task->state;
            state->timers.erase(*task->queued);
            task->queued.reset();
            dispatch(task, true);
            armNext(state);
          }
        });
      }
    };
    if (runAt > now) {
      if (task->ctx.stopToken().stop_possible()) {
        task->cancelCallback.emplace(task->ctx.stopToken(), onCancel);
      }
      task->externalCancelCallbacks.reserve(
          task->ctx.externalStopTokens().size());
      for (const auto& token : task->ctx.externalStopTokens()) {
        if (token.stop_possible()) {
          task->externalCancelCallbacks.push_back(
              std::make_unique<CancelCallback>(token, onCancel));
        }
      }
    }

    // Allocate the queue node before admission so allocation failures cannot
    // silently discard accepted work. The strand inserts this node allocation-free.
    TimerQueue staging;
    auto node = staging.extract(staging.emplace(runAt, task));
    auto activity = state->activity.load(std::memory_order_acquire);
    for (;;) {
      if (activity & kClosed) {
        bestEffort([state] { state->taskRejectedCounter->inc(); });
        throw PoolStoppedError();
      }
      if ((activity & kCountMask) == kCountMask) throw std::overflow_error("too many delay tasks");
      if (state->activity.compare_exchange_weak(activity, activity + 1,
                                               std::memory_order_acq_rel)) break;
    }
    if (state->metricsEnabled) bestEffort([state] { state->gaugeWaitQueueLength->inc(); });
    try {
      boost::asio::post(state->strand, [state, task, node = std::move(node)]() mutable {
        if (task->cancelRequested.load(std::memory_order_acquire) ||
            node.key() <= std::chrono::steady_clock::now()) {
          dispatch(task, task->expeditedByDeadline || task->cancelRequested.load(std::memory_order_acquire));
        } else {
          task->queued = state->timers.insert(std::move(node));
          armNext(state);
        }
      });
    } catch (...) {
      retire(state);
      throw;
    }
  }

  [[nodiscard]] std::int64_t activeTasksApprox() const noexcept {
    return state_->activity.load(std::memory_order_acquire) & kCountMask;
  }

 private:
  template <typename Callback>
  static void bestEffort(Callback&& callback) noexcept {
    try {
      std::forward<Callback>(callback)();
    } catch (...) {
    }
  }

  static void signalDrained(const std::shared_ptr<SharedState>& state) {
    if (!state->drainSignalled.exchange(true, std::memory_order_acq_rel)) state->drainPromise.set_value();
  }

  static void retire(const std::shared_ptr<SharedState>& state) {
    if (state->metricsEnabled) bestEffort([state] { state->gaugeWaitQueueLength->dec(); });
    const auto previous = state->activity.fetch_sub(1, std::memory_order_acq_rel);
    if ((previous & kClosed) && (previous & kCountMask) == 1) signalDrained(state);
  }

  static void dispatch(const std::shared_ptr<DelayTask>& task, bool expedited) {
    // Independent executor task, never execute user code on the timer strand.
    boost::asio::post(task->state->executor, [task, expedited] { execute(task, expedited); });
  }

  static void armNext(const std::shared_ptr<SharedState>& state) {
    const auto next = state->timers.empty()
        ? std::optional<std::chrono::steady_clock::time_point>{}
        : std::optional{state->timers.begin()->first};
    if (next == state->armedAt) return;
    const auto generation = ++state->generation;
    state->armedAt = next;
    if (!next) { state->timer.cancel(); return; }
    state->timer.expires_at(*next);
    state->timer.async_wait([state, generation](const boost::system::error_code& error) {
      if (generation != state->generation || error == boost::asio::error::operation_aborted) return;
      state->armedAt.reset();
      const auto now = std::chrono::steady_clock::now();
      unsigned batch = 0;
      while (!state->timers.empty() && state->timers.begin()->first <= now && batch++ < 64) {
        auto node = state->timers.extract(state->timers.begin());
        auto task = std::move(node.mapped());
        task->queued.reset();
        dispatch(task, task->expeditedByDeadline || task->cancelRequested.load(std::memory_order_acquire));
      }
      armNext(state);
    });
  }

  static void recordStopTimeout(const std::shared_ptr<SharedState>& state) {
    bestEffort([state] {
      state->env.getLogger().warn("delay pool stopped by timeout");
    });
    bestEffort([state] { state->stopTimeoutCounter->inc(); });
  }

  static std::chrono::steady_clock::time_point saturatedAdd(
      std::chrono::steady_clock::time_point now, Duration duration) {
    if (duration <= Duration::zero()) return now;
    const auto maximum = std::chrono::steady_clock::time_point::max() - now;
    return duration >= maximum
               ? std::chrono::steady_clock::time_point::max()
               : now + duration;
  }

  [[noreturn]] static void rejectCancelled(
      const std::shared_ptr<SharedState>& state) {
    bestEffort([state] { state->taskRejectedCounter->inc(); });
    throw PoolCancelledError();
  }

  static void execute(const std::shared_ptr<DelayTask>& task,
                      bool expedited) {
    bool expected = false;
    if (!task->claimed.compare_exchange_strong(expected, true,
                                               std::memory_order_acq_rel)) {
      return;
    }
    task->cancelCallback.reset();
    task->externalCancelCallbacks.clear();
    const auto state = task->state;
    const auto* previous = currentExecutingPool_;
    currentExecutingPool_ = state.get();
    const auto startedAt = state->metricsEnabled
                               ? std::chrono::steady_clock::now()
                               : std::chrono::steady_clock::time_point{};
    try {
      task->fn();
    } catch (const std::exception& error) {
      bestEffort([state, &error] {
        state->env.getLogger().warn(
            "delay pool task error",
            {log::Field::Str("pool", "delay"), log::Field::Err(error)});
      });
    } catch (...) {
      bestEffort([state] {
        state->env.getLogger().warn(
            "delay pool task error",
            {log::Field::Str("pool", "delay"),
             log::Field::Str("error", "<unknown>")});
      });
    }
    currentExecutingPool_ = previous;
    task->fn = nullptr;
    if (state->metricsEnabled) {
      bestEffort([state] { state->tasksTotal->inc(); });
      const double elapsed = std::chrono::duration<double>(
                                 std::chrono::steady_clock::now() - startedAt)
                                 .count();
      bestEffort(
          [state, elapsed] { state->executionDuration->observe(elapsed); });
      if (expedited) {
        bestEffort([state] { state->taskCancelledCounter->inc(); });
      }
    }

    retire(state);
  }

  inline static thread_local const SharedState* currentExecutingPool_{};
  std::shared_ptr<SharedState> state_;
};

inline std::unique_ptr<IDelayPool> makeDelayPool(IServiceEnvironment& env) {
  return std::make_unique<DelayPoolImpl>(env);
}

}  // namespace servicelib::pool

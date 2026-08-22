/*
 * delaypool.hpp
 * C++ streams API — service-wide delayed task scheduler.
 */
#pragma once

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
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
  enum class PoolState { kCreated, kRunning, kStopping, kStopped, kFailed };
  struct SharedState;
  struct DelayTask;
  using CancelCallback = std::stop_callback<std::function<void()>>;

  struct DelayTask final {
    std::shared_ptr<SharedState> state;
    Context ctx;
    std::function<void()> fn;
    std::unique_ptr<boost::asio::steady_timer> timer;
    std::atomic<bool> claimed{false};
    std::atomic<bool> admitted{false};
    std::atomic<bool> cancelRequested{false};
    std::optional<CancelCallback> cancelCallback;
    std::vector<std::unique_ptr<CancelCallback>> externalCancelCallbacks;
  };

  struct SharedState final {
    explicit SharedState(IServiceEnvironment& environment)
        : env(environment),
          executor(detail::ParallelExecutorRegistry::Get()) {
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
    std::mutex mu;
    std::condition_variable cv;
    PoolState poolState = PoolState::kCreated;
    std::int64_t pending = 0;
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
    const auto state = state_;
    std::lock_guard lock(state->mu);
    const bool unused =
        state->poolState == PoolState::kCreated && state->pending == 0;
    if (!unused && state->poolState != PoolState::kStopped) {
      std::terminate();
    }
  }

  void start([[maybe_unused]] Context ctx) override {
    const auto state = state_;
    std::lock_guard lock(state->mu);
    switch (state->poolState) {
      case PoolState::kCreated:
        state->poolState = PoolState::kRunning;
        return;
      case PoolState::kStopping:
      case PoolState::kStopped:
        throw PoolStoppedError();
      case PoolState::kRunning:
      case PoolState::kFailed:
        throw PoolAlreadyStartedError();
    }
  }

  void stop(Context ctx) override {
    const auto state = state_;
    if (currentExecutingPool_ == state.get()) {
      throw PoolSelfStopError();
    }

    bool ownsStop = false;
    bool timedOut = false;
    {
      std::unique_lock lock(state->mu);
      if (state->poolState == PoolState::kStopped) return;
      if (state->poolState != PoolState::kStopping) {
        state->poolState = PoolState::kStopping;
        ownsStop = true;
      }
      timedOut = !waitWithContext(*state, lock, ctx, [state] {
        return state->pending == 0 || state->poolState == PoolState::kStopped;
      });
    }
    if (timedOut) recordStopTimeout(state);

    // A shutdown deadline is diagnostic, not permission for accepted work to
    // outlive the graph captured by its callback. Keep the graph owner blocked
    // until all previously admitted tasks have retired.
    std::unique_lock lock(state->mu);
    if (ownsStop) {
      state->cv.wait(lock, [state] { return state->pending == 0; });
      state->poolState = PoolState::kStopped;
      state->cv.notify_all();
    } else {
      state->cv.wait(
          lock, [state] { return state->poolState == PoolState::kStopped; });
    }
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
    task->timer = std::make_unique<boost::asio::steady_timer>(state->executor);

    const std::weak_ptr<DelayTask> weakTask(task);
    const auto onCancel = [weakTask] {
      if (const auto locked = weakTask.lock()) {
        locked->cancelRequested.store(true, std::memory_order_release);
        if (locked->admitted.load(std::memory_order_acquire)) {
          locked->timer->cancel();
        }
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

    {
      std::lock_guard lock(state->mu);
      if (state->poolState == PoolState::kStopping ||
          state->poolState == PoolState::kStopped) {
        bestEffort([state] { state->taskRejectedCounter->inc(); });
        throw PoolStoppedError();
      }
      if (state->poolState == PoolState::kFailed) {
        bestEffort([state] { state->taskRejectedCounter->inc(); });
        throw PoolNotStartedError();
      }
      ++state->pending;
      publishPendingGaugeLocked(*state);
    }

    try {
      if (runAt <= now) {
        boost::asio::post(state->executor,
                          [task] { execute(task, false); });
      } else {
        task->timer->expires_at(runAt);
        task->timer->async_wait(
            [task, expeditedByDeadline](const boost::system::error_code& ec) {
              if (!ec || ec == boost::asio::error::operation_aborted) {
                execute(task,
                        expeditedByDeadline ||
                            task->cancelRequested.load(
                                std::memory_order_acquire));
              }
            });
      }
      task->admitted.store(true, std::memory_order_release);
      if (task->cancelRequested.load(std::memory_order_acquire)) {
        task->timer->cancel();
      }
    } catch (...) {
      std::lock_guard lock(state->mu);
      --state->pending;
      publishPendingGaugeLocked(*state);
      state->cv.notify_all();
      throw;
    }
  }

  [[nodiscard]] std::int64_t activeTasksApprox() const noexcept {
    const auto state = state_;
    std::lock_guard lock(state->mu);
    return state->pending;
  }

 private:
  template <typename Callback>
  static void bestEffort(Callback&& callback) noexcept {
    try {
      std::forward<Callback>(callback)();
    } catch (...) {
    }
  }

  template <typename Predicate>
  static bool waitWithContext(SharedState& state,
                              std::unique_lock<std::mutex>& lock,
                              const Context& ctx, Predicate&& predicate) {
    if (const auto& deadline = ctx.deadline(); deadline) {
      return state.cv.wait_until(lock, *deadline,
                                 std::forward<Predicate>(predicate));
    }
    state.cv.wait(lock, std::forward<Predicate>(predicate));
    return true;
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

  static void publishPendingGaugeLocked(SharedState& state) noexcept {
    if (!state.metricsEnabled) return;
    bestEffort([&state] { state.gaugeWaitQueueLength->set(state.pending); });
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

    std::lock_guard lock(state->mu);
    --state->pending;
    publishPendingGaugeLocked(*state);
    if (state->pending == 0) state->cv.notify_all();
  }

  inline static thread_local const SharedState* currentExecutingPool_{};
  std::shared_ptr<SharedState> state_;
};

inline std::unique_ptr<IDelayPool> makeDelayPool(IServiceEnvironment& env) {
  return std::make_unique<DelayPoolImpl>(env);
}

}  // namespace servicelib::pool

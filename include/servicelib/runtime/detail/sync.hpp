#pragma once

#include <atomic>
#include <future>
#include <condition_variable>
#include <chrono>
#include <cstddef>
#include <deque>
#include <list>
#include <functional>
#include <mutex>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/experimental/concurrent_channel.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <servicelib/runtime/detail/asio_dispatch.hpp>
#include <servicelib/runtime/detail/cooperative_execution.hpp>
#include <servicelib/runtime/context.hpp>

namespace servicelib::detail {

// One-shot notification. A mutex protects registration and list detachment;
// waiters are notified only after releasing the mutex.
// synchronous waiters allocate their blocking future only at the sync boundary.
class SingleUseEvent final {
  using AsyncSignal = boost::asio::experimental::concurrent_channel<
      void(boost::system::error_code)>;
  struct Waiter {
    Waiter() noexcept : next(nullptr) {}
    Waiter* next;
    virtual ~Waiter() = default;
    virtual void Wake() noexcept {}
  };
  struct AsyncWaiter final : Waiter {
    explicit AsyncWaiter(const std::shared_ptr<AsyncSignal>& value) : signal(value) {}
    void Wake() noexcept override {
      if (auto value = signal.lock())
        static_cast<void>(value->try_send(boost::system::error_code{}));
    }
    std::weak_ptr<AsyncSignal> signal;
  };
  struct SyncWaiter final : Waiter {
    explicit SyncWaiter(const std::shared_ptr<std::promise<void>>& value) : signal(value) {}
    void Wake() noexcept override {
      if (auto value = signal.lock()) value->set_value();
    }
    std::weak_ptr<std::promise<void>> signal;
  };

 public:
  SingleUseEvent() = default;
  SingleUseEvent(const SingleUseEvent&) = delete;
  SingleUseEvent& operator=(const SingleUseEvent&) = delete;
  ~SingleUseEvent() {
    auto* waiter = waiters_;
    while (waiter && waiter != &readyMarker_) {
      auto* next = waiter->next;
      delete waiter;
      waiter = next;
    }
  }

  void Send() noexcept {
    Waiter* waiter;
    {
      std::lock_guard lock(waitersMutex_);
      waiter = std::exchange(waiters_, &readyMarker_);
    }
    while (waiter && waiter != &readyMarker_) {
      auto* next = waiter->next;
      waiter->Wake();
      delete waiter;
      waiter = next;
    }
  }

  void Wait() {
    if (IsReady()) return;
    if (CooperativeExecution::Active()) {
      CooperativeExecution::Await([this] { return AsyncWait(); });
      return;
    }
    auto signal = std::make_shared<std::promise<void>>();
    auto ready = signal->get_future();
    if (Register(std::make_unique<SyncWaiter>(signal))) ready.wait();
  }

  template <typename Clock, typename Duration>
  [[nodiscard]] bool WaitUntil(
      const std::chrono::time_point<Clock, Duration>& deadline) {
    if (IsReady()) return true;
    if (CooperativeExecution::Active()) {
      const auto remaining = deadline - Clock::now();
      Context context;
      context = std::move(context).withDeadline(
          std::chrono::steady_clock::now() + remaining);
      CooperativeExecution::Await([&] { return AsyncWait(context); });
      return IsReady();
    }
    auto signal = std::make_shared<std::promise<void>>();
    auto ready = signal->get_future();
    if (!Register(std::make_unique<SyncWaiter>(signal))) return true;
    return ready.wait_until(deadline) == std::future_status::ready || IsReady();
  }

  [[nodiscard]] bool IsReady() const noexcept {
    std::lock_guard lock(waitersMutex_);
    return waiters_ == &readyMarker_;
  }

  boost::asio::awaitable<void> AsyncWait() { return AsyncWaitImpl(nullptr); }
  boost::asio::awaitable<void> AsyncWait(const Context& context) {
    return AsyncWaitImpl(&context);
  }

 private:
  bool Register(std::unique_ptr<Waiter> waiter) noexcept {
    std::lock_guard lock(waitersMutex_);
    if (waiters_ == &readyMarker_) return false;
    waiter->next = waiters_;
    waiters_ = waiter.release();
    return true;
  }

  boost::asio::awaitable<void> AsyncWaitImpl(const Context* context) {
    if (IsReady()) co_return;
    if (context && context->cancelled()) co_return;
    const auto executor = co_await boost::asio::this_coro::executor;
    auto signal = std::make_shared<AsyncSignal>(executor, 1);
    if (!Register(std::make_unique<AsyncWaiter>(signal))) co_return;
    auto cancel = [signal] {
      static_cast<void>(signal->try_send(boost::system::error_code{}));
    };
    using StopCallback = std::stop_callback<decltype(cancel)>;
    std::optional<StopCallback> stopCallback;
    std::optional<StopCallback> firstExternalCallback;
    std::vector<std::unique_ptr<StopCallback>> additionalExternalCallbacks;
    std::optional<boost::asio::steady_timer> deadlineTimer;
    if (context) {
      if (context->deadline()) {
        deadlineTimer.emplace(executor, *context->deadline());
        deadlineTimer->async_wait(
            [weak = std::weak_ptr<AsyncSignal>{signal}](const boost::system::error_code& error) {
              if (error) return;
              if (auto pending = weak.lock()) {
                static_cast<void>(pending->try_send(boost::system::error_code{}));
              }
            });
      }
      if (context->stopToken().stop_possible()) {
        stopCallback.emplace(context->stopToken(), cancel);
      }
      for (const auto& token : context->externalStopTokens()) {
        if (!token.stop_possible()) continue;
        if (!firstExternalCallback) {
          firstExternalCallback.emplace(token, cancel);
        } else {
          additionalExternalCallbacks.push_back(
              std::make_unique<StopCallback>(token, cancel));
        }
      }
    }
    co_await signal->async_receive(boost::asio::use_awaitable);
  }

  inline static Waiter readyMarker_;
  mutable std::mutex waitersMutex_;
  Waiter* waiters_{};
};

// Contended callbacks cannot hold an executor thread while their owner waits
// on I/O. There is no allocation on an uncontended lock.
class CooperativeMutex final {
 public:
  void lock() {
    std::shared_ptr<SingleUseEvent> ready;
    {
      std::lock_guard lock(mutex_);
      if (!locked_) { locked_ = true; return; }
      ready = std::make_shared<SingleUseEvent>();
      waiters_.push_back(ready);
    }
    ready->Wait();
  }
  void unlock() noexcept {
    std::shared_ptr<SingleUseEvent> ready;
    {
      std::lock_guard lock(mutex_);
      if (waiters_.empty()) { locked_ = false; return; }
      ready = std::move(waiters_.front());
      waiters_.pop_front();
    }
    ready->Send();
  }
 private:
  std::mutex mutex_;
  bool locked_{};
  std::deque<std::shared_ptr<SingleUseEvent>> waiters_;
};

// Shared callback admission with cooperative exclusive retirement. Readers
// remain concurrent; a queued writer prevents new readers from starving it.
class CooperativeSharedMutex final {
  struct Waiter {
    std::shared_ptr<SingleUseEvent> ready;
    bool writer;
  };
 public:
  void lock() { Acquire(true); }
  void lock_shared() { Acquire(false); }
  void unlock() noexcept { Release(true); }
  void unlock_shared() noexcept { Release(false); }

 private:
  void Acquire(bool writer) {
    std::shared_ptr<SingleUseEvent> ready;
    {
      std::lock_guard lock(mutex_);
      if (!writer_ && waiters_.empty() && (!writer || readers_ == 0)) {
        if (writer) writer_ = true;
        else ++readers_;
        return;
      }
      ready = std::make_shared<SingleUseEvent>();
      waiters_.push_back({ready, writer});
    }
    ready->Wait();
  }

  void Release(bool writer) noexcept {
    std::list<Waiter> ready;
    {
      std::lock_guard lock(mutex_);
      if (writer) writer_ = false;
      else --readers_;
      if (writer_ || readers_ != 0 || waiters_.empty()) return;
      if (waiters_.front().writer) {
        writer_ = true;
        ready.splice(ready.end(), waiters_, waiters_.begin());
      } else {
        do {
          ++readers_;
          ready.splice(ready.end(), waiters_, waiters_.begin());
        } while (!waiters_.empty() && !waiters_.front().writer);
      }
    }
    for (const auto& waiter : ready) waiter.ready->Send();
  }

  std::mutex mutex_;
  std::list<Waiter> waiters_;
  std::size_t readers_{};
  bool writer_{};
};

// Lifecycle callbacks may block or join their own executors. Keep that work on
// a separate control thread, but suspend a cooperative caller while it runs.
// This is a shutdown boundary, not a scheduler for stream FunctionCall edges.
template <typename Result>
class ControlTask final {
 public:
  template <typename Function>
  explicit ControlTask(Function&& function)
      : ready_(std::make_shared<SingleUseEvent>()),
        result_(std::async(std::launch::async,
            [ready = ready_,
             function = std::optional<std::decay_t<Function>>{
                 std::in_place, std::forward<Function>(function)}]() mutable -> Result {
              struct Completion final {
                decltype(function)& callback;
                SingleUseEvent& ready;
                ~Completion() {
                  // Release callback captures before waking the caller. Their
                  // destructors can themselves need progress on the reactor.
                  callback.reset();
                  ready.Send();
                }
              } completion{function, *ready};
              return std::invoke(std::move(*function));
            })) {}

  ControlTask(ControlTask&&) noexcept = default;
  ControlTask& operator=(ControlTask&&) = delete;
  ControlTask(const ControlTask&) = delete;
  ControlTask& operator=(const ControlTask&) = delete;
  ~ControlTask() {
    // Also preserve ownership when starting a later control task throws.
    if (result_.valid()) wait();
  }

  void wait() const {
    ready_->Wait();
    result_.wait();
  }

  Result get() {
    wait();
    return result_.get();
  }

  template <typename Clock, typename Duration>
  std::future_status wait_until(
      const std::chrono::time_point<Clock, Duration>& deadline) const {
    if (!ready_->WaitUntil(deadline)) return std::future_status::timeout;
    return result_.wait_until(deadline);
  }

  template <typename Rep, typename Period>
  std::future_status wait_for(
      const std::chrono::duration<Rep, Period>& timeout) const {
    if (timeout <= timeout.zero()) return result_.wait_for(timeout);
    return wait_until(std::chrono::steady_clock::now() + timeout);
  }

 private:
  std::shared_ptr<SingleUseEvent> ready_;
  std::future<Result> result_;
};

class TaskStorage final {
 public:
  TaskStorage() = default;
  TaskStorage(const TaskStorage&) = delete;
  TaskStorage& operator=(const TaskStorage&) = delete;
  ~TaskStorage() { CancelAndWait(); }

  template <typename Function>
  void CriticalAsyncDetach(std::string_view, Function&& function) {
    {
      std::lock_guard lock(mutex_);
      if (!accepting_) {
        throw std::runtime_error("task storage is stopped");
      }
      ++active_;
    }
    try {
      BlockingExecutorRegistry::Post(
          [this, function = std::forward<Function>(function)]() mutable {
            try {
              std::invoke(std::move(function));
            } catch (...) {
            }
            std::lock_guard lock(mutex_);
            if (--active_ == 0) drained_.notify_all();
          });
    } catch (...) {
      std::lock_guard lock(mutex_);
      if (--active_ == 0) drained_.notify_all();
      throw;
    }
  }

  void CancelAndWait() noexcept {
    std::unique_lock lock(mutex_);
    accepting_ = false;
    drained_.wait(lock, [this] { return active_ == 0; });
  }

 private:
  std::mutex mutex_;
  std::condition_variable drained_;
  std::size_t active_{};
  bool accepting_{true};
};

}  // namespace servicelib::detail

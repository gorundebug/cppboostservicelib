#pragma once

#include <atomic>
#include <future>
#include <condition_variable>
#include <chrono>
#include <cstddef>
#include <functional>
#include <mutex>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <utility>
#include <vector>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/experimental/concurrent_channel.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <servicelib/runtime/detail/asio_dispatch.hpp>
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
    auto signal = std::make_shared<std::promise<void>>();
    auto ready = signal->get_future();
    if (Register(std::make_unique<SyncWaiter>(signal))) ready.wait();
  }

  template <typename Clock, typename Duration>
  [[nodiscard]] bool WaitUntil(
      const std::chrono::time_point<Clock, Duration>& deadline) {
    if (IsReady()) return true;
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
    if (context) {
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

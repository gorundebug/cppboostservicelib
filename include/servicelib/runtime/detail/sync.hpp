#pragma once

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

// Blocking boundary primitive used only by the synchronous ServiceLib endpoint
// contract. asio-grpc registration adapters remain coroutine based.
class SingleUseEvent final {
  using AsyncSignal = boost::asio::experimental::concurrent_channel<
      void(boost::system::error_code)>;

 public:
  void Send() noexcept {
    std::shared_ptr<AsyncSignal> firstWaiter;
    std::vector<std::shared_ptr<AsyncSignal>> additionalWaiters;
    {
      std::lock_guard lock(mutex_);
      ready_ = true;
      firstWaiter = asyncWaiter_.lock();
      asyncWaiter_.reset();
      for (auto& waiter : additionalAsyncWaiters_) {
        if (auto signal = waiter.lock()) {
          additionalWaiters.push_back(std::move(signal));
        }
      }
      additionalAsyncWaiters_.clear();
    }
    readyCondition_.notify_all();
    if (firstWaiter) {
      static_cast<void>(firstWaiter->try_send(boost::system::error_code{}));
    }
    for (auto& waiter : additionalWaiters) {
      static_cast<void>(waiter->try_send(boost::system::error_code{}));
    }
  }

  void Wait() {
    std::unique_lock lock(mutex_);
    readyCondition_.wait(lock, [this] { return ready_; });
  }

  template <typename Clock, typename Duration>
  [[nodiscard]] bool WaitUntil(
      const std::chrono::time_point<Clock, Duration>& deadline) {
    std::unique_lock lock(mutex_);
    return readyCondition_.wait_until(lock, deadline,
                                      [this] { return ready_; });
  }

  [[nodiscard]] bool IsReady() const noexcept {
    std::lock_guard lock(mutex_);
    return ready_;
  }

  boost::asio::awaitable<void> AsyncWait() {
    return AsyncWaitImpl(nullptr);
  }

  boost::asio::awaitable<void> AsyncWait(const Context& context) {
    return AsyncWaitImpl(&context);
  }

 private:
  boost::asio::awaitable<void> AsyncWaitImpl(const Context* context) {
    {
      std::lock_guard lock(mutex_);
      if (ready_) co_return;
    }
    const auto executor = co_await boost::asio::this_coro::executor;
    auto signal = std::make_shared<AsyncSignal>(executor, 1);
    {
      std::lock_guard lock(mutex_);
      if (ready_) co_return;
      if (asyncWaiter_.expired()) {
        asyncWaiter_ = signal;
      } else {
        additionalAsyncWaiters_.push_back(signal);
      }
    }
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

  mutable std::mutex mutex_;
  std::condition_variable readyCondition_;
  std::weak_ptr<AsyncSignal> asyncWaiter_;
  std::vector<std::weak_ptr<AsyncSignal>> additionalAsyncWaiters_;
  bool ready_{false};
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

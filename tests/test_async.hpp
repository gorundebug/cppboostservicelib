#pragma once

#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/thread_pool.hpp>

#include <servicelib/runtime/detail/asio_dispatch.hpp>

#include <chrono>
#include <condition_variable>
#include <future>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

namespace test_async {

inline constexpr auto kMaxTestWaitTime = std::chrono::seconds{5};
using Mutex = std::mutex;

class Event final {
 public:
  void Send() {
    std::lock_guard lock(mutex_);
    signalled_ = true;
    condition_.notify_all();
  }

  bool WaitForEvent() { return WaitForEventFor(kMaxTestWaitTime); }

  template <typename Rep, typename Period>
  bool WaitForEventFor(std::chrono::duration<Rep, Period> timeout) {
    std::unique_lock lock(mutex_);
    return condition_.wait_for(lock, timeout, [this] { return signalled_; });
  }

 private:
  std::mutex mutex_;
  std::condition_variable condition_;
  bool signalled_{};
};

template <typename Rep, typename Period>
void SleepFor(std::chrono::duration<Rep, Period> duration) {
  std::this_thread::sleep_for(duration);
}

template <typename Result>
class TaskWithResult;

template <>
class TaskWithResult<void> final {
 public:
  template <typename Function>
  explicit TaskWithResult(Function&& function)
      : future_(std::async(std::launch::async,
                           std::forward<Function>(function))) {}
  void Get() { future_.get(); }

 private:
  std::future<void> future_;
};

template <typename Function>
auto Async([[maybe_unused]] std::string name, Function&& function) {
  return TaskWithResult<void>{std::forward<Function>(function)};
}

class AsioRuntime final {
 public:
  AsioRuntime() : work_(boost::asio::make_work_guard(context_)) {
    servicelib::detail::ParallelExecutorRegistry::Set(context_.get_executor());
    servicelib::detail::BlockingExecutorRegistry::Set(
        blockingPool_.get_executor());
    for (int index = 0; index < 4; ++index) {
      threads_.emplace_back([this] { context_.run(); });
    }
  }

  ~AsioRuntime() {
    work_.reset();
    context_.stop();
    for (auto& thread : threads_) thread.join();
    blockingPool_.stop();
    blockingPool_.join();
    servicelib::detail::BlockingExecutorRegistry::Clear();
    servicelib::detail::ParallelExecutorRegistry::Clear();
  }

 private:
  boost::asio::io_context context_{4};
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type>
      work_;
  boost::asio::thread_pool blockingPool_{4};
  std::vector<std::thread> threads_;
};

}  // namespace test_async

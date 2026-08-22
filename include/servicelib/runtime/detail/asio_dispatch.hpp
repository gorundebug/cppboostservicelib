#pragma once

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/post.hpp>

#include <functional>
#include <mutex>
#include <stdexcept>
#include <utility>

namespace servicelib::detail {

// One generated ServiceLib service is hosted per process, matching the
// canonical runtime assumption. ServiceApp installs its io_context executor
// here before streams are initialized and clears it after graph shutdown.
class ParallelExecutorRegistry final {
 public:
  static void Set(boost::asio::any_io_executor executor) {
    std::lock_guard lock(mutex_);
    executor_ = std::move(executor);
  }

  static void Clear() {
    std::lock_guard lock(mutex_);
    executor_ = {};
  }

  static boost::asio::any_io_executor Get() {
    std::lock_guard lock(mutex_);
    if (!executor_) {
      throw std::logic_error("ServiceLib Asio executor is not initialized");
    }
    return executor_;
  }

  static void Post(std::function<void()> task) {
    boost::asio::post(Get(), std::move(task));
  }

 private:
  inline static std::mutex mutex_;
  inline static boost::asio::any_io_executor executor_;
};

// Executor for unavoidable synchronous foreign/runtime boundaries. Work sent
// here may block an OS thread, but can never consume an Asio or gRPC reactor
// worker. Request-path operations that have an asynchronous API must use
// co_await instead of this executor.
class BlockingExecutorRegistry final {
 public:
  static void Set(boost::asio::any_io_executor executor) {
    std::lock_guard lock(mutex_);
    executor_ = std::move(executor);
  }

  static void Clear() {
    std::lock_guard lock(mutex_);
    executor_ = {};
  }

  static boost::asio::any_io_executor Get() {
    std::lock_guard lock(mutex_);
    if (!executor_) {
      throw std::logic_error(
          "ServiceLib blocking executor is not initialized");
    }
    return executor_;
  }

  static void Post(std::function<void()> task) {
    boost::asio::post(Get(), std::move(task));
  }

 private:
  inline static std::mutex mutex_;
  inline static boost::asio::any_io_executor executor_;
};

}  // namespace servicelib::detail

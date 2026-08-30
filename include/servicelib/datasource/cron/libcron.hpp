/*
 * Copyright (c) 2026 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 * Licensed under the MIT License. See the
 * [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */
#pragma once

#include <functional>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <stdexcept>
#include <unordered_map>
#include <utility>

#include <servicelib/runtime/context.hpp>
#include <servicelib/runtime/environment.hpp>
#include <servicelib/runtime/payload.hpp>
#include <servicelib/runtime/schedule.hpp>

namespace servicelib {
template <typename T, typename R, typename E, typename Context>
class InputStream;
}

namespace servicelib::datasource::cron {

namespace detail {

class ResultWaiter final {
 public:
  enum class Completion { kCompleted, kMissing, kDuplicate };

  struct Pending final {
    std::mutex mutex;
    std::condition_variable completed;
    bool done{};
  };

  std::shared_ptr<Pending> begin(const std::string& streamId) {
    if (streamId.empty()) throw std::invalid_argument("cron activation has no stream ID");
    auto pending = std::make_shared<Pending>();
    std::lock_guard lock(mutex_);
    if (!pending_.emplace(streamId, pending).second) {
      throw std::logic_error("duplicate active cron stream ID: " + streamId);
    }
    return pending;
  }

  Completion complete(std::string_view streamId) {
    std::shared_ptr<Pending> pending;
    {
      std::lock_guard lock(mutex_);
      const auto found = pending_.find(std::string{streamId});
      if (found == pending_.end()) return Completion::kMissing;
      pending = found->second;
    }
    {
      std::lock_guard lock(pending->mutex);
      if (pending->done) return Completion::kDuplicate;
      pending->done = true;
    }
    pending->completed.notify_all();
    return Completion::kCompleted;
  }

  void wait(const std::string& streamId,
            const std::shared_ptr<Pending>& pending) {
    {
      std::unique_lock lock(pending->mutex);
      pending->completed.wait(lock, [&pending] { return pending->done; });
    }
    erase(streamId, pending);
  }

  void cancel(const std::string& streamId,
              const std::shared_ptr<Pending>& pending) noexcept {
    erase(streamId, pending);
  }

 private:
  void erase(const std::string& streamId,
             const std::shared_ptr<Pending>& pending) noexcept {
    std::lock_guard lock(mutex_);
    const auto found = pending_.find(streamId);
    if (found != pending_.end() && found->second == pending) pending_.erase(found);
  }

  std::mutex mutex_;
  std::unordered_map<std::string, std::shared_ptr<Pending>> pending_;
};

template <typename T, typename InputStreamType>
class ScheduleCollector final {
 public:
  explicit ScheduleCollector(InputStreamType& input) noexcept : input_(input) {}

  void out(MessageContext context, T&& value) {
    input_.consume(std::move(context), Payload<T>::make(std::move(value)));
  }

  void out(MessageContext context, const T& value) {
    input_.consume(std::move(context), Payload<T>::make(value));
  }

 private:
  InputStreamType& input_;
};

}  // namespace detail

// Converts the portable five-field UTC expression to libcron's six-field
// syntax. libcron remains the parser and occurrence evaluator.
std::string ToLibcronExpression(const std::string& expression);

class Endpoint final {
 public:
  using Output = std::function<void(MessageContext, Payload<ScheduleTrigger>)>;

  template <typename T, typename R, typename E, typename StreamContext,
            typename Function>
  static std::shared_ptr<Endpoint> make(IServiceEnvironment& environment,
                                        InputStream<T, R, E, StreamContext>& input,
                                        Function& function) {
    using InputStreamType = InputStream<T, R, E, StreamContext>;
    const bool hasResult = input.getResultStream() != nullptr;
    auto waiter = std::make_shared<detail::ResultWaiter>();
    auto endpoint = std::shared_ptr<Endpoint>(new Endpoint(
        environment, input.getEndpointId(), hasResult, waiter,
        [input = &input, function = &function, waiter, hasResult](
            MessageContext context, Payload<ScheduleTrigger> trigger) {
          const std::string streamId{context.streamId()};
          auto pending = hasResult ? waiter->begin(streamId) : nullptr;
          detail::ScheduleCollector<T, InputStreamType> out(*input);
          try {
            (*function)(std::move(context), trigger.get(), std::move(out));
            if (pending) waiter->wait(streamId, pending);
          } catch (...) {
            if (pending) waiter->cancel(streamId, pending);
            throw;
          }
        }));
    if (hasResult) {
      input.setResultConsumer(
          [endpoint = std::weak_ptr<Endpoint>{endpoint}](MessageContext context,
                                                         Payload<R>) {
            if (auto locked = endpoint.lock()) {
              locked->completeResult(context.streamId());
            }
          });
    }
    return endpoint;
  }

  Endpoint(const Endpoint&) = delete;
  Endpoint& operator=(const Endpoint&) = delete;
  ~Endpoint();

  [[nodiscard]] int id() const noexcept;
  [[nodiscard]] const std::string& name() const noexcept;

 private:
  friend class LibcronDataSource;
  struct Impl;

  Endpoint(IServiceEnvironment& environment, int endpointId, bool hasResult,
           std::shared_ptr<detail::ResultWaiter> waiter, Output output);
  void completeResult(std::string_view streamId) noexcept;

  std::unique_ptr<Impl> impl_;
};

class LibcronDataSource final {
 public:
  static std::shared_ptr<LibcronDataSource> make(
      IServiceEnvironment& environment, int connectorId);

  LibcronDataSource(const LibcronDataSource&) = delete;
  LibcronDataSource& operator=(const LibcronDataSource&) = delete;
  ~LibcronDataSource();

  void addEndpoint(std::shared_ptr<Endpoint> endpoint);
  void start(Context context);
  void stop(Context context);

  [[nodiscard]] int id() const noexcept;
  [[nodiscard]] const std::string& getName() const noexcept;

 private:
  struct Impl;

  LibcronDataSource(IServiceEnvironment& environment, int connectorId);

  std::unique_ptr<Impl> impl_;
};

}  // namespace servicelib::datasource::cron

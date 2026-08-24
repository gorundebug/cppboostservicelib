/*
 * Copyright (c) 2026 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 * Licensed under the MIT License. See the
 * [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */
#pragma once

#include <functional>
#include <memory>
#include <string>
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
    return std::shared_ptr<Endpoint>(new Endpoint(
        environment, input.getEndpointId(),
        [input = &input, function = &function](
            MessageContext context, Payload<ScheduleTrigger> trigger) {
          detail::ScheduleCollector<T, InputStreamType> out(*input);
          (*function)(std::move(context), trigger.get(), std::move(out));
        }));
  }

  Endpoint(const Endpoint&) = delete;
  Endpoint& operator=(const Endpoint&) = delete;
  ~Endpoint();

  [[nodiscard]] int id() const noexcept;
  [[nodiscard]] const std::string& name() const noexcept;

 private:
  friend class LibcronDataSource;
  struct Impl;

  Endpoint(IServiceEnvironment& environment, int endpointId, Output output);

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

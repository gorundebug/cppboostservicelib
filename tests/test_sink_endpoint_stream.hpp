#pragma once

#include <cstddef>
#include <exception>
#include <functional>
#include <utility>

#include <servicelib/runtime/datasink.hpp>

template <typename T, typename R, typename E = std::exception_ptr>
class TestSinkEndpointStream final
    : public servicelib::SinkEndpointStream<T, R, E> {
 public:
  using ResultOutput =
      std::function<void(servicelib::MessageContext, servicelib::Payload<R>)>;
  using ErrorOutput =
      std::function<void(servicelib::MessageContext, servicelib::Payload<E>)>;

  TestSinkEndpointStream(servicelib::IServiceEnvironment& environment, int id,
                         ResultOutput result = {}, ErrorOutput error = {},
                         std::size_t stream_config_id = 0)
      : environment_(environment),
        id_(id),
        stream_config_id_(stream_config_id),
        result_(std::move(result)),
        error_(std::move(error)) {}

  servicelib::IServiceEnvironment& environment() const override {
    return environment_;
  }
  int endpointId() const noexcept override { return id_; }
  std::size_t streamConfigId() const noexcept override {
    return stream_config_id_;
  }

  void collectResult(servicelib::MessageContext context,
                     servicelib::Payload<R> value) override {
    if (result_) result_(std::move(context), std::move(value));
  }
  void collectError(servicelib::MessageContext context,
                    servicelib::Payload<E> value) override {
    if (error_) error_(std::move(context), std::move(value));
  }

 private:
  servicelib::IServiceEnvironment& environment_;
  int id_;
  std::size_t stream_config_id_;
  ResultOutput result_;
  ErrorOutput error_;
};

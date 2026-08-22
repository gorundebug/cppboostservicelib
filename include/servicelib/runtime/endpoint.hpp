#pragma once

#include <servicelib/runtime/caller.hpp>

#include <utility>

namespace servicelib {

template <typename State>
struct BeginResult final {
  MessageContext context;
  State state;
};

template <typename T, typename R, typename E>
class SinkStreamContext final {
 public:
  using ResultOutput = Consumer<R>;
  using ErrorOutput = Consumer<E>;

  SinkStreamContext(ResultOutput result = {}, ErrorOutput error = {})
      : result_(std::move(result)), error_(std::move(error)) {}

  void result(MessageContext context, R value) const {
    if (result_) result_(std::move(context), Payload<R>::make(std::move(value)));
  }
  void error(MessageContext context, E value) const {
    if (error_) error_(std::move(context), Payload<E>::make(std::move(value)));
  }
  [[nodiscard]] bool hasResultOutput() const noexcept {
    return static_cast<bool>(result_);
  }
  [[nodiscard]] bool hasErrorOutput() const noexcept {
    return static_cast<bool>(error_);
  }

 private:
  ResultOutput result_;
  ErrorOutput error_;
};

template <typename T, typename R, typename E>
class SourceStreamContext final {
 public:
  using Output = Consumer<T>;
  using ErrorOutput = Consumer<E>;

  SourceStreamContext(Output output = {}, ErrorOutput error = {})
      : output_(std::move(output)), error_(std::move(error)) {}
  void collect(MessageContext context, T value) const {
    if (output_) output_(std::move(context), Payload<T>::make(std::move(value)));
  }
  void error(MessageContext context, E value) const {
    if (error_) error_(std::move(context), Payload<E>::make(std::move(value)));
  }

 private:
  Output output_;
  ErrorOutput error_;
};

}  // namespace servicelib

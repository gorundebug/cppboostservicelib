#pragma once

#include <functional>
#include <memory>
#include <string_view>
#include <utility>

#include <servicelib/runtime/context.hpp>
#include <servicelib/runtime/payload.hpp>

namespace servicelib {

inline constexpr std::string_view kStreamIdHeader = "x-stream-id";

template <typename R>
class SubStreamCollector {
 public:
  virtual ~SubStreamCollector() = default;
  virtual bool out(MessageContext context, const R& value) = 0;
};

template <typename R>
class SubStreamCollectorFunc final : public SubStreamCollector<R> {
 public:
  using Function = std::function<bool(MessageContext, const R&)>;
  explicit SubStreamCollectorFunc(Function function) : function_(std::move(function)) {}
  bool out(MessageContext context, const R& value) override {
    return function_(std::move(context), value);
  }
 private:
  Function function_;
};

template <typename T, typename R>
class ISubStream {
 public:
  virtual ~ISubStream() = default;
  virtual void consume(MessageContext context, Payload<T> value,
                       std::shared_ptr<SubStreamCollector<R>> collector) = 0;
};

template <typename State>
struct BeginResult final {
  MessageContext context;
  State state;
};

template <typename F>
void bestEffortTelemetry(F&& function) noexcept {
  try {
    std::forward<F>(function)();
  } catch (...) {
    // Telemetry must never change request processing semantics.
  }
}

}  // namespace servicelib

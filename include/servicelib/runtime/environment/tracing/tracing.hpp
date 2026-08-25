/*
 * tracing.hpp
 * C++ streams API — engine-agnostic distributed tracing interface
 *
 * Mirrors servicelib's Go implementation
 * (runtime/environment/tracing/tracing.go): Tracer/Span abstract
 * interfaces + Attribute value type. No concrete backend dependency here —
 * see servicelib/telemetry/userver/tracing.hpp for the userver-backed
 * implementation.
 *
 * Normal Span objects are coroutine-local: their parent/child linkage lives
 * in engine task-local storage. To continue a trace across a dispatch
 * boundary, capture SpanContext and start a child on the receiving coroutine.
 * A delayed operation is the deliberate exception: its logical span includes
 * the wait itself and therefore starts before one coroutine schedules the
 * timer and ends in another. Tracer::startDetachedChildOf creates a span that
 * is not attached to either coroutine stack for exactly that lifecycle.
 *
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the
 * [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */
#pragma once

#include <cstddef>
#include <cstdint>
#include <exception>
#include <initializer_list>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <variant>

namespace servicelib::tracing {

enum class StatusCode : uint8_t { kUnset, kOk, kError };

struct SpanContext;

inline std::optional<SpanContext> ParseTraceParent(
    std::string_view value) noexcept;

// W3C trace-context admission shared by HTTP and gRPC transports. Version 00
// has a fixed representation; malformed/all-zero identifiers must not enable
// tracing merely because their last byte looks sampled.
inline bool SampledTraceParent(std::string_view value) noexcept {
  if (value.size() != 55 || value[2] != '-' || value[35] != '-' ||
      value[52] != '-') {
    return false;
  }
  const auto hex = [](char ch) noexcept -> int {
    if (ch >= '0' && ch <= '9') return ch - '0';
    if (ch >= 'a' && ch <= 'f') return ch - 'a' + 10;
    if (ch >= 'A' && ch <= 'F') return ch - 'A' + 10;
    return -1;
  };
  for (std::size_t i = 0; i < value.size(); ++i) {
    if (i == 2 || i == 35 || i == 52) continue;
    if (hex(value[i]) < 0) return false;
  }
  if (value.substr(0, 2) == "ff") return false;
  const auto nonzero = [](std::string_view id) noexcept {
    return id.find_first_not_of('0') != std::string_view::npos;
  };
  if (!nonzero(value.substr(3, 32)) || !nonzero(value.substr(36, 16))) {
    return false;
  }
  return (hex(value[53]) * 16 + hex(value[54])) % 2 != 0;
}

// Go analog: tracing.Attribute + StringAttr/Int64Attr/Float64Attr/BoolAttr.
// See log::Field's comment on why this is a std::variant, not a hand-rolled
// tagged union like Go's — same reasoning applies here.
class Attribute {
 public:
  using Value = std::variant<std::string, int64_t, double, bool>;

  static Attribute String(std::string key, std::string val) {
    return Attribute(std::move(key),
                     Value(std::in_place_type<std::string>, std::move(val)));
  }

  static Attribute Int64(std::string key, int64_t val) {
    return Attribute(std::move(key), Value(std::in_place_type<int64_t>, val));
  }

  static Attribute Float64(std::string key, double val) {
    return Attribute(std::move(key), Value(std::in_place_type<double>, val));
  }

  static Attribute Bool(std::string key, bool val) {
    return Attribute(std::move(key), Value(std::in_place_type<bool>, val));
  }

  [[nodiscard]] const std::string& key() const noexcept { return key_; }
  [[nodiscard]] const Value& value() const noexcept { return value_; }

 private:
  Attribute(std::string key, Value value)
      : key_(std::move(key)), value_(std::move(value)) {}

  std::string key_;
  Value value_;
};

struct SpanContext {
  std::string traceId;
  std::string spanId;
  bool valid{false};
  std::string traceState;
  std::string baggage;

  [[nodiscard]] bool isValid() const noexcept { return valid; }
};

inline std::optional<SpanContext> ParseTraceParent(
    std::string_view value) noexcept {
  if (value.size() != 55 || value[2] != '-' || value[35] != '-' ||
      value[52] != '-') {
    return std::nullopt;
  }
  const auto isHex = [](char ch) noexcept {
    return (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f') ||
           (ch >= 'A' && ch <= 'F');
  };
  for (std::size_t i = 0; i < value.size(); ++i) {
    if (i == 2 || i == 35 || i == 52) continue;
    if (!isHex(value[i])) return std::nullopt;
  }
  const auto nonzero = [](std::string_view id) noexcept {
    return id.find_first_not_of('0') != std::string_view::npos;
  };
  if (value.substr(0, 2) == "ff" || !nonzero(value.substr(3, 32)) ||
      !nonzero(value.substr(36, 16))) {
    return std::nullopt;
  }
  return SpanContext{std::string{value.substr(3, 32)},
                     std::string{value.substr(36, 16)}, true, {}, {}};
}

// Go analog: tracing.Span.
class Span {
 public:
  virtual ~Span() = default;

  virtual void end() = 0;
  virtual void setAttributes(std::initializer_list<Attribute> attrs) = 0;
  virtual void recordError(std::string_view message) = 0;
  virtual void setStatus(StatusCode code, std::string_view description) = 0;
  virtual void addEvent(std::string_view name,
                        std::initializer_list<Attribute> attrs = {}) = 0;
  [[nodiscard]] virtual SpanContext spanContext() const = 0;
};

// No-op span returned when tracing is disabled — avoids null checks at call
// sites. Go analog: noopSpan.
class NoopSpan final : public Span {
 public:
  void end() override {}
  void setAttributes(std::initializer_list<Attribute>) override {}
  void recordError(std::string_view) override {}
  void setStatus(StatusCode, std::string_view) override {}
  void addEvent(std::string_view, std::initializer_list<Attribute>) override {}
  [[nodiscard]] SpanContext spanContext() const override { return {}; }
};

// Go analog: tracing.Tracer.
class Tracer {
 public:
  virtual ~Tracer() = default;

  // Starts a new span, parented under the ambient current span if the
  // concrete implementation tracks one. Must be called and the returned
  // Span ended within the same coroutine (see file comment). Never returns
  // null — implementations should return a NoopSpan when tracing/sampling
  // is disabled. shared_ptr (not unique_ptr) so callers can keep a copy for
  // error-path End() calls while still transferring a copy to whatever
  // owns the "normal" End() call.
  virtual std::shared_ptr<Span> start(
      std::string_view spanName,
      std::initializer_list<Attribute> attrs = {}) const = 0;

  // Returns the ambient current span's context, or an invalid
  // SpanContext if there is none / tracing is disabled. Used to bridge
  // a trace across a coroutine boundary together with startChildOf.
  [[nodiscard]] virtual SpanContext currentSpanContext() const = 0;

  // Starts a new span with an explicit parent context instead of the
  // ambient current span — the counterpart to currentSpanContext() for
  // continuing a trace on a different coroutine than the one that captured
  // the parent context. Same lifetime rules as start().
  virtual std::shared_ptr<Span> startChildOf(
      std::string_view spanName, const SpanContext& parent,
      std::initializer_list<Attribute> attrs = {}) const = 0;

  // Starts a span with an explicit parent without attaching it to the current
  // coroutine stack. The returned span may be ended on a different coroutine.
  // Intended for logical operations whose lifetime itself crosses a scheduling
  // boundary, such as stream.delay. It must not be used as an ambient parent;
  // propagate spanContext() explicitly instead.
  virtual std::shared_ptr<Span> startDetachedChildOf(
      std::string_view spanName, const SpanContext& parent,
      std::initializer_list<Attribute> attrs = {}) const = 0;
};

// Go analog: tracing.Tracing — factory for named tracers (e.g. one per
// service in a multi-service binary). IServiceEnvironment::getTracing()
// returns null when tracing is disabled entirely.
class Tracing {
 public:
  virtual ~Tracing() = default;

  virtual std::shared_ptr<Tracer> tracer(std::string_view name) const = 0;
};

inline void SpanEvent(Span* span, std::string_view name,
                      std::initializer_list<Attribute> attrs = {}) {
  if (span) span->addEvent(name, attrs);
}

inline void SpanError(Span* span, std::string_view error) {
  if (!span) return;
  span->recordError(error);
  span->setStatus(StatusCode::kError, error);
}

inline std::string ExceptionMessage(const std::exception_ptr& error) {
  if (!error) return {};
  try {
    std::rethrow_exception(error);
  } catch (const std::exception& exception) {
    return exception.what();
  } catch (...) {
    return "<unknown>";
  }
}

inline void SpanAttrs(Span* span, std::initializer_list<Attribute> attrs) {
  if (span) span->setAttributes(attrs);
}

inline void SpanEnd(Span* span) noexcept {
  if (span) span->end();
}

// Owns the context produced by a span start and ends the span exactly once.
// The wrapper is movable but intentionally not copyable.
template <typename TContext>
class StartedSpan final {
 public:
  StartedSpan(TContext context, std::shared_ptr<Span> span)
      : context_(std::move(context)), span_(std::move(span)) {}

  StartedSpan(const StartedSpan&) = delete;
  StartedSpan& operator=(const StartedSpan&) = delete;
  StartedSpan(StartedSpan&&) noexcept = default;
  StartedSpan& operator=(StartedSpan&&) noexcept = delete;

  ~StartedSpan() {
    if (span_) span_->end();
  }

  [[nodiscard]] TContext& context() noexcept { return context_; }
  [[nodiscard]] const TContext& context() const noexcept { return context_; }
  [[nodiscard]] Span* span() const noexcept { return span_.get(); }
  [[nodiscard]] const std::shared_ptr<Span>& sharedSpan() const noexcept {
    return span_;
  }

 private:
  TContext context_;
  std::shared_ptr<Span> span_;
};

// Owns only the active span. Internal hot paths use this guard together with
// an existing context so disabled tracing does not move the context into an
// otherwise empty StartedSpan wrapper.
class ActiveSpan final {
 public:
  ActiveSpan() = default;
  explicit ActiveSpan(std::shared_ptr<Span> span) : span_(std::move(span)) {}

  ActiveSpan(const ActiveSpan&) = delete;
  ActiveSpan& operator=(const ActiveSpan&) = delete;
  ActiveSpan(ActiveSpan&&) noexcept = default;
  ActiveSpan& operator=(ActiveSpan&& other) noexcept {
    if (this != &other) {
      if (span_) span_->end();
      span_ = std::move(other.span_);
    }
    return *this;
  }

  ~ActiveSpan() {
    if (span_) span_->end();
  }

  [[nodiscard]] Span* span() const noexcept { return span_.get(); }
  [[nodiscard]] const std::shared_ptr<Span>& sharedSpan() const noexcept {
    return span_;
  }

 private:
  std::shared_ptr<Span> span_;
};

// Go analog: tracing.EnableSampling / tracing.SamplingEnabled.
template <typename TContext>
[[nodiscard]] TContext EnableSampling(TContext context) {
  return context.withSampling(true);
}

template <typename TContext>
[[nodiscard]] bool SamplingEnabled(const TContext& context) noexcept {
  return context.samplingEnabled();
}

// Internal in-place variant of StartSpan. It preserves the public StartSpan
// value API while avoiding a context move and StartedSpan construction when
// tracing is disabled or the message is not sampled.
template <typename TContext>
[[nodiscard]] ActiveSpan StartSpanInPlace(
    TContext& context, Tracer* tracer, std::string_view operation,
    std::initializer_list<Attribute> attrs = {}) {
  if (!tracer || !SamplingEnabled(context)) return {};

  std::shared_ptr<Span> span;
  if (context.trace().isValid()) {
    span = tracer->startChildOf(operation, context.trace(), attrs);
  } else {
    span = tracer->start(operation, attrs);
  }
  if (span) {
    // This is an output parameter: downstream work must observe the child as
    // its parent. Making context const would silently break trace parentage.
    auto child = span->spanContext();
    const auto& parent = context.trace();
    if (child.traceState.empty()) child.traceState = parent.traceState;
    if (child.baggage.empty()) child.baggage = parent.baggage;
    context = std::move(context).withTrace(std::move(child));
  }
  return ActiveSpan(std::move(span));
}

// Go analog: tracing.StartSpan. TContext is intentionally generic so this
// engine-neutral header does not depend on the concrete servicelib context
// type. A context used here must expose samplingEnabled(), trace(),
// withTrace(SpanContext), and value semantics.
template <typename TContext>
[[nodiscard]] StartedSpan<TContext> StartSpan(
    TContext context, Tracer* tracer, std::string_view operation,
    std::initializer_list<Attribute> attrs = {}) {
  if (!tracer || !SamplingEnabled(context)) {
    return StartedSpan<TContext>(std::move(context), {});
  }

  std::shared_ptr<Span> span;
  if (context.trace().isValid()) {
    span = tracer->startChildOf(operation, context.trace(), attrs);
  } else {
    span = tracer->start(operation, attrs);
  }
  if (span) {
    auto child = span->spanContext();
    const auto& parent = context.trace();
    if (child.traceState.empty()) child.traceState = parent.traceState;
    if (child.baggage.empty()) child.baggage = parent.baggage;
    context = std::move(context).withTrace(std::move(child));
  }
  return StartedSpan<TContext>(std::move(context), std::move(span));
}

}  // namespace servicelib::tracing

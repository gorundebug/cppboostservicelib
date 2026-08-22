/*
 * tracing.hpp
 * OpenTelemetry-backed implementation of servicelib::tracing::Tracing/Span.
 *
 * This is the Boost runtime replacement for the canonical
 * runtime/telemetry/userver/tracing.hpp boundary.  The ServiceLib interfaces
 * and explicit cross-coroutine SpanContext contract remain unchanged.
 */
#pragma once

#include <array>
#include <cstdint>
#include <initializer_list>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <opentelemetry/common/attribute_value.h>
#include <opentelemetry/nostd/shared_ptr.h>
#include <opentelemetry/nostd/span.h>
#include <opentelemetry/nostd/string_view.h>
#include <opentelemetry/sdk/trace/tracer_provider.h>
#include <opentelemetry/trace/span.h>
#include <opentelemetry/trace/trace_flags.h>
#include <opentelemetry/trace/tracer.h>
#include <opentelemetry/trace/tracer_provider.h>

#include <servicelib/runtime/environment/tracing/tracing.hpp>

namespace servicelib::telemetry::opentelemetry_adapter {

namespace otel = ::opentelemetry;

namespace detail {

inline otel::nostd::string_view StringView(std::string_view value) noexcept {
  return {value.data(), value.size()};
}

inline otel::common::AttributeValue AttributeValue(
    const tracing::Attribute& attribute) {
  return std::visit(
      [](const auto& value) -> otel::common::AttributeValue {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, std::string>) {
          return StringView(value);
        } else {
          return value;
        }
      },
      attribute.value());
}

using Attributes = std::vector<
    std::pair<otel::nostd::string_view, otel::common::AttributeValue>>;

inline Attributes ConvertAttributes(
    std::initializer_list<tracing::Attribute> attributes) {
  Attributes converted;
  converted.reserve(attributes.size());
  for (const auto& attribute : attributes) {
    converted.emplace_back(StringView(attribute.key()),
                           AttributeValue(attribute));
  }
  return converted;
}

inline int HexDigit(char value) noexcept {
  if (value >= '0' && value <= '9') return value - '0';
  if (value >= 'a' && value <= 'f') return value - 'a' + 10;
  if (value >= 'A' && value <= 'F') return value - 'A' + 10;
  return -1;
}

template <std::size_t N>
inline bool DecodeHex(std::string_view value,
                      std::array<std::uint8_t, N>& bytes) noexcept {
  if (value.size() != N * 2) return false;
  bool nonzero = false;
  for (std::size_t index = 0; index < N; ++index) {
    const auto high = HexDigit(value[index * 2]);
    const auto low = HexDigit(value[index * 2 + 1]);
    if (high < 0 || low < 0) return false;
    bytes[index] = static_cast<std::uint8_t>((high << 4) | low);
    nonzero = nonzero || bytes[index] != 0;
  }
  return nonzero;
}

inline otel::trace::SpanContext ParentContext(
    const tracing::SpanContext& parent) noexcept {
  if (!parent.valid) return otel::trace::SpanContext::GetInvalid();

  std::array<std::uint8_t, otel::trace::TraceId::kSize> trace_id{};
  std::array<std::uint8_t, otel::trace::SpanId::kSize> span_id{};
  if (!DecodeHex(parent.traceId, trace_id) ||
      !DecodeHex(parent.spanId, span_id)) {
    return otel::trace::SpanContext::GetInvalid();
  }

  return otel::trace::SpanContext(
      otel::trace::TraceId(otel::nostd::span<
                           const std::uint8_t,
                           otel::trace::TraceId::kSize>{trace_id.data(),
                                                        trace_id.size()}),
      otel::trace::SpanId(otel::nostd::span<
                          const std::uint8_t,
                          otel::trace::SpanId::kSize>{span_id.data(),
                                                      span_id.size()}),
      otel::trace::TraceFlags(otel::trace::TraceFlags::kIsSampled), true,
      parent.traceState.empty()
          ? otel::trace::TraceState::GetDefault()
          : otel::trace::TraceState::FromHeader(StringView(parent.traceState)));
}

inline tracing::SpanContext SpanContext(
    const otel::trace::SpanContext& context) {
  if (!context.IsValid()) return {};

  std::array<char, otel::trace::TraceId::kSize * 2> trace_id{};
  std::array<char, otel::trace::SpanId::kSize * 2> span_id{};
  context.trace_id().ToLowerBase16(
      otel::nostd::span<char, otel::trace::TraceId::kSize * 2>{trace_id});
  context.span_id().ToLowerBase16(
      otel::nostd::span<char, otel::trace::SpanId::kSize * 2>{span_id});
  return tracing::SpanContext{
      std::string(trace_id.data(), trace_id.size()),
      std::string(span_id.data(), span_id.size()), true,
      context.trace_state() ? context.trace_state()->ToHeader() : std::string{},
      {}};
}

}  // namespace detail

class OpenTelemetrySpan final : public tracing::Span {
 public:
  explicit OpenTelemetrySpan(
      otel::nostd::shared_ptr<otel::trace::Span> span)
      : span_(std::move(span)) {}

  ~OpenTelemetrySpan() override { end(); }

  void end() override {
    std::lock_guard lock(mutex_);
    if (!span_) return;
    span_->End();
    span_ = nullptr;
  }

  void setAttributes(std::initializer_list<tracing::Attribute> attrs) override {
    std::lock_guard lock(mutex_);
    if (!span_) return;
    for (const auto& attribute : attrs) {
      span_->SetAttribute(detail::StringView(attribute.key()),
                          detail::AttributeValue(attribute));
    }
  }

  void recordError(std::string_view message) override {
    std::lock_guard lock(mutex_);
    if (!span_) return;
    span_->SetAttribute("error", true);
    if (!message.empty()) {
      const std::array<std::pair<otel::nostd::string_view,
                                 otel::common::AttributeValue>,
                       1>
          attributes{{{"exception.message", detail::StringView(message)}}};
      span_->AddEvent("exception", attributes);
    }
  }

  void setStatus(tracing::StatusCode code,
                 std::string_view description) override {
    std::lock_guard lock(mutex_);
    if (!span_) return;
    auto otel_code = otel::trace::StatusCode::kUnset;
    switch (code) {
      case tracing::StatusCode::kOk:
        otel_code = otel::trace::StatusCode::kOk;
        break;
      case tracing::StatusCode::kError:
        otel_code = otel::trace::StatusCode::kError;
        break;
      case tracing::StatusCode::kUnset:
        break;
    }
    span_->SetStatus(otel_code, detail::StringView(description));
  }

  void addEvent(std::string_view name,
                std::initializer_list<tracing::Attribute> attrs) override {
    std::lock_guard lock(mutex_);
    if (!span_) return;
    if (attrs.size() == 0) {
      span_->AddEvent(detail::StringView(name));
      return;
    }
    const auto attributes = detail::ConvertAttributes(attrs);
    span_->AddEvent(detail::StringView(name), attributes);
  }

  [[nodiscard]] tracing::SpanContext spanContext() const override {
    std::lock_guard lock(mutex_);
    return span_ ? detail::SpanContext(span_->GetContext())
                 : tracing::SpanContext{};
  }

 private:
  mutable std::mutex mutex_;
  otel::nostd::shared_ptr<otel::trace::Span> span_;
};

class OpenTelemetryTracer final : public tracing::Tracer {
 public:
  explicit OpenTelemetryTracer(
      otel::nostd::shared_ptr<otel::trace::Tracer> tracer)
      : tracer_(std::move(tracer)) {}

  std::shared_ptr<tracing::Span> start(
      std::string_view spanName,
      std::initializer_list<tracing::Attribute> attrs) const override {
    const auto attributes = detail::ConvertAttributes(attrs);
    return std::make_shared<OpenTelemetrySpan>(tracer_->StartSpan(
        detail::StringView(spanName), attributes));
  }

  [[nodiscard]] tracing::SpanContext currentSpanContext() const override {
    const auto span = otel::trace::Tracer::GetCurrentSpan();
    return span ? detail::SpanContext(span->GetContext())
                : tracing::SpanContext{};
  }

  std::shared_ptr<tracing::Span> startChildOf(
      std::string_view spanName, const tracing::SpanContext& parent,
      std::initializer_list<tracing::Attribute> attrs) const override {
    const auto parent_context = detail::ParentContext(parent);
    if (!parent_context.IsValid()) return start(spanName, attrs);

    otel::trace::StartSpanOptions options;
    options.parent = parent_context;
    const auto attributes = detail::ConvertAttributes(attrs);
    return std::make_shared<OpenTelemetrySpan>(tracer_->StartSpan(
        detail::StringView(spanName), attributes, options));
  }

  std::shared_ptr<tracing::Span> startDetachedChildOf(
      std::string_view spanName, const tracing::SpanContext& parent,
      std::initializer_list<tracing::Attribute> attrs) const override {
    // OTel parentage is explicit and a Span is not attached to the runtime
    // context unless Tracer::WithActiveSpan is called.  Therefore this is the
    // same safe cross-coroutine lifecycle as startChildOf, without an ambient
    // coroutine stack to detach from.
    return startChildOf(spanName, parent, attrs);
  }

 private:
  otel::nostd::shared_ptr<otel::trace::Tracer> tracer_;
};

class OpenTelemetryTracing final : public tracing::Tracing {
 public:
  explicit OpenTelemetryTracing(
      std::shared_ptr<otel::sdk::trace::TracerProvider> provider)
      : provider_(std::move(provider)) {
    if (!provider_) {
      throw std::invalid_argument("OpenTelemetry tracer provider is null");
    }
  }

  ~OpenTelemetryTracing() override { (void)shutdown(); }

  std::shared_ptr<tracing::Tracer> tracer(std::string_view name) const override {
    const std::string key(name);
    std::lock_guard lock(mutex_);
    if (const auto found = tracers_.find(key); found != tracers_.end()) {
      return found->second;
    }
    auto created = std::make_shared<OpenTelemetryTracer>(
        provider_->GetTracer(detail::StringView(key)));
    tracers_.emplace(key, created);
    return created;
  }

  [[nodiscard]] bool forceFlush() noexcept {
    return provider_->ForceFlush();
  }

  [[nodiscard]] bool shutdown() noexcept {
    std::lock_guard lock(shutdown_mutex_);
    if (shutdown_) return true;
    shutdown_ = true;
    return provider_->Shutdown();
  }

  [[nodiscard]] const std::shared_ptr<otel::sdk::trace::TracerProvider>&
  provider() const noexcept {
    return provider_;
  }

 private:
  std::shared_ptr<otel::sdk::trace::TracerProvider> provider_;
  mutable std::mutex mutex_;
  mutable std::unordered_map<std::string, std::shared_ptr<tracing::Tracer>> tracers_;
  std::mutex shutdown_mutex_;
  bool shutdown_{false};
};

}  // namespace servicelib::telemetry::opentelemetry_adapter

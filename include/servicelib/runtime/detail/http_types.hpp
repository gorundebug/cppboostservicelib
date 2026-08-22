#pragma once

#include <servicelib/runtime/context.hpp>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cctype>
#include <charconv>
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

namespace servicelib::http {

struct CaseInsensitiveLess final {
  using is_transparent = void;
  bool operator()(std::string_view left, std::string_view right) const noexcept {
    return std::lexicographical_compare(
        left.begin(), left.end(), right.begin(), right.end(),
        [](unsigned char lhs, unsigned char rhs) {
          return std::tolower(lhs) < std::tolower(rhs);
        });
  }
};

using Headers = std::map<std::string, std::string, CaseInsensitiveLess>;

struct Request final {
  std::string method;
  std::string target;
  std::string path;
  Headers headers;
  std::string body;
  bool keepAlive{true};
};

struct Response final {
  int status{200};
  Headers headers;
  std::string body;
  std::string contentType{"application/json; charset=utf-8"};
  bool keepAlive{true};
};

inline std::optional<std::string_view> Header(const Headers& headers,
                                              std::string_view name) {
  const auto found = headers.find(name);
  if (found == headers.end()) return std::nullopt;
  return found->second;
}

inline std::string NewStreamId() {
  static std::atomic<std::uint64_t> sequence{};
  const auto now = static_cast<std::uint64_t>(
      std::chrono::steady_clock::now().time_since_epoch().count());
  const auto value = sequence.fetch_add(1, std::memory_order_relaxed);
  std::array<char, 2 * sizeof(std::uint64_t) * 2 + 1> buffer{};
  auto* current = buffer.data();
  auto* end = buffer.data() + buffer.size();
  const auto nowResult = std::to_chars(current, end, now, 16);
  current = nowResult.ptr;
  *current++ = '-';
  const auto sequenceResult = std::to_chars(current, end, value, 16);
  return {buffer.data(), sequenceResult.ptr};
}

inline std::optional<tracing::SpanContext> ParseTraceParent(
    std::string_view value) {
  if (!tracing::SampledTraceParent(value)) return std::nullopt;
  tracing::SpanContext result;
  result.traceId = std::string(value.substr(3, 32));
  result.spanId = std::string(value.substr(36, 16));
  result.valid = true;
  return result;
}

inline MessageContext ContextFromHeaders(const Headers& headers,
                                         bool tracingEnabled = true) {
  MessageContext context;
  if (const auto stream = Header(headers, "x-stream-id");
      stream && !stream->empty()) {
    context = std::move(context).withStreamId(std::string(*stream));
  } else {
    context = std::move(context).withStreamId(NewStreamId());
  }

  if (const auto timeout = Header(headers, "x-timeout-ms")) {
    std::int64_t milliseconds{};
    const auto parsed = std::from_chars(
        timeout->data(), timeout->data() + timeout->size(), milliseconds);
    if (parsed.ec == std::errc{} &&
        parsed.ptr == timeout->data() + timeout->size() && milliseconds >= 0) {
      context = std::move(context).withDeadline(
          std::chrono::steady_clock::now() +
          std::chrono::milliseconds(milliseconds));
    }
  }

  if (tracingEnabled) {
    tracing::SpanContext propagation;
    if (const auto parent = Header(headers, "traceparent")) {
      if (auto trace = ParseTraceParent(*parent)) {
        propagation = std::move(*trace);
        context = std::move(context).withSampling(true);
      }
    }
    if (const auto traceState = Header(headers, "tracestate"))
      propagation.traceState = std::string(*traceState);
    if (const auto baggage = Header(headers, "baggage"))
      propagation.baggage = std::string(*baggage);
    if (propagation.isValid() || !propagation.traceState.empty() ||
        !propagation.baggage.empty())
      context = std::move(context).withTrace(std::move(propagation));
    if (const auto marker = Header(headers, "x-trace");
        marker && !marker->empty()) {
      context = std::move(context).withSampling(true);
    }
  }
  return context;
}

inline void InjectContext(const MessageContext& context, Headers& headers) {
  if (!context.streamId().empty()) {
    headers["x-stream-id"] = std::string(context.streamId());
  }
  if (context.deadline()) {
    const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::max(*context.deadline() - std::chrono::steady_clock::now(),
                 std::chrono::steady_clock::duration::zero()));
    headers["x-timeout-ms"] = std::to_string(remaining.count());
  }
  const auto& trace = context.trace();
  if (trace.isValid()) {
    headers["traceparent"] = "00-" + trace.traceId + "-" + trace.spanId +
                             (context.samplingEnabled() ? "-01" : "-00");
    if (!trace.traceState.empty()) headers["tracestate"] = trace.traceState;
  }
  if (!trace.baggage.empty()) headers["baggage"] = trace.baggage;
  if (context.samplingEnabled()) headers["x-trace"] = "1";
}

}  // namespace servicelib::http

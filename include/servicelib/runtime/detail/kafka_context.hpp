#pragma once

#include <map>
#include <string>
#include <string_view>

#include <servicelib/runtime/context.hpp>

namespace servicelib::detail {

using KafkaHeaders = std::map<std::string, std::string>;

inline MessageContext ContextFromKafkaHeaders(const KafkaHeaders& headers) {
  MessageContext context;
  if (const auto found = headers.find("x-stream-id");
      found != headers.end() && !found->second.empty()) {
    context = std::move(context).withStreamId(found->second);
  }
  tracing::SpanContext propagation;
  if (const auto found = headers.find("traceparent");
      found != headers.end()) {
    if (auto parsed = tracing::ParseTraceParent(found->second)) {
      propagation = std::move(*parsed);
    }
    if (tracing::SampledTraceParent(found->second)) {
      context = std::move(context).withSampling(true);
    }
  }
  if (const auto found = headers.find("tracestate"); found != headers.end()) {
    propagation.traceState = found->second;
  }
  if (const auto found = headers.find("baggage"); found != headers.end()) {
    propagation.baggage = found->second;
  }
  if (propagation.isValid() || !propagation.traceState.empty() ||
      !propagation.baggage.empty()) {
    context = std::move(context).withTrace(std::move(propagation));
  }
  if (const auto found = headers.find("x-trace");
      found != headers.end() && !found->second.empty()) {
    context = std::move(context).withSampling(true);
  }
  return context;
}

inline void InjectKafkaContext(const MessageContext& context,
                               KafkaHeaders& headers) {
  if (!context.streamId().empty()) {
    headers["x-stream-id"] = std::string{context.streamId()};
  }
  const auto& trace = context.trace();
  if (trace.isValid()) {
    headers["traceparent"] = "00-" + trace.traceId + "-" + trace.spanId +
                             (context.samplingEnabled() ? "-01" : "-00");
  }
  if (!trace.traceState.empty()) headers["tracestate"] = trace.traceState;
  if (!trace.baggage.empty()) headers["baggage"] = trace.baggage;
  if (context.samplingEnabled()) headers["x-trace"] = "1";
}

}  // namespace servicelib::detail

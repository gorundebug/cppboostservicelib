/*
 * stream_tracing.hpp
 * Shared tracing helpers for stream operators.
 *
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 * Licensed under the MIT License. See the
 * [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */
#pragma once

#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include <servicelib/runtime/base.hpp>
#include <servicelib/runtime/context.hpp>
#include <servicelib/runtime/environment/environment.hpp>
#include <servicelib/runtime/environment/tracing/tracing.hpp>

namespace servicelib {

// Immutable definition metadata resolved once by a concrete endpoint consumer.
struct StreamTraceIdentity {
  std::string name;
  std::string pipeline;
  std::string component;
};

}  // namespace servicelib

namespace servicelib::tracing {

// Go analog: runtime.ServiceStream.StartSpan. Graph identity is fixed while
// the topology is built, so tracing does not touch configuration on the hot
// path.
[[nodiscard]] inline ActiveSpan StartStreamSpan(
    MessageContext& context, const StreamBase& stream,
    std::string_view operation) {
  auto* const tracer = stream.getStreamTracer();
  if (!tracer || !SamplingEnabled(context)) {
    return {};
  }
  return StartSpanInPlace(
      context, tracer, operation,
      {Attribute::String("stream", stream.getName()),
       Attribute::String("pipeline", stream.getPipeline()),
       Attribute::String("component", stream.getComponent())});
}

}  // namespace servicelib::tracing

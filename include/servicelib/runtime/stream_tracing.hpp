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

namespace servicelib::tracing {

// Go analog: runtime.ServiceStream.StartSpan. Graph identity is fixed while
// the topology is built, so tracing does not touch configuration on the hot
// path.
[[nodiscard]] inline ActiveSpan StartStreamSpan(
    MessageContext& context, const StreamBase& stream,
    std::string_view operation) {
  Tracer* tracer = nullptr;
  std::shared_ptr<Tracer> tracerOwner;
  auto* const environment = stream.getEnv();
  if (!environment || !SamplingEnabled(context)) {
    return {};
  }
  if (auto* tracingEngine = environment->getTracing()) {
    tracerOwner = tracingEngine->tracer(environment->getServiceName());
    tracer = tracerOwner.get();
  }
  if (!tracer) {
    return {};
  }

  return StartSpanInPlace(
      context, tracer, operation,
      {Attribute::String("stream", stream.getName())});
}

}  // namespace servicelib::tracing

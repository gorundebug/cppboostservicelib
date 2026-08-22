#pragma once

#include <servicelib/runtime/detail/http_types.hpp>
#include <servicelib/runtime/context.hpp>

#include <grpcpp/client_context.h>
#include <grpcpp/server_context.h>

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cctype>
#include <string>

namespace servicelib::grpc_transport {

inline bool IsMetadataKey(std::string_view key) {
  return !key.empty() && std::ranges::all_of(key, [](unsigned char value) {
    return std::islower(value) || std::isdigit(value) || value == '-' ||
           value == '_' || value == '.';
  });
}

inline void InjectContext(const MessageContext& context,
                          grpc::ClientContext& client) {
  http::Headers headers;
  http::InjectContext(context, headers);
  for (const auto& [key, value] : headers) client.AddMetadata(key, value);

  if (context.deadline()) {
    const auto steadyNow = std::chrono::steady_clock::now();
    const auto systemNow = std::chrono::system_clock::now();
    if (*context.deadline() <= steadyNow) {
      client.set_deadline(systemNow - std::chrono::milliseconds{1});
    } else {
      client.set_deadline(
          systemNow +
          std::chrono::duration_cast<std::chrono::system_clock::duration>(
              *context.deadline() - steadyNow));
    }
  }
}

inline MessageContext ExtractContext(const grpc::ServerContext& server,
                                     bool tracingEnabled = true) {
  http::Headers headers;
  for (const auto& [key, value] : server.client_metadata()) {
    const std::string_view metadataKey{key.data(), key.size()};
    const bool transportMetadata =
        metadataKey == "x-stream-id" || metadataKey == "x-timeout-ms";
    const bool tracingMetadata =
        metadataKey == "x-trace" || metadataKey == "traceparent" ||
        metadataKey == "tracestate" || metadataKey == "baggage";
    if (!transportMetadata && !(tracingEnabled && tracingMetadata)) continue;
    headers.emplace(std::string(key.data(), key.size()),
                    std::string(value.data(), value.size()));
  }
  auto context = http::ContextFromHeaders(headers, tracingEnabled);
  const auto grpcDeadline = server.deadline();
  if (grpcDeadline != std::chrono::system_clock::time_point::max()) {
    const auto remaining = grpcDeadline - std::chrono::system_clock::now();
    context = std::move(context).withDeadline(
        std::chrono::steady_clock::now() +
        std::chrono::duration_cast<std::chrono::steady_clock::duration>(
            remaining));
  }
  return context;
}

}  // namespace servicelib::grpc_transport

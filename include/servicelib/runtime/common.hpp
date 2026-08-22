#pragma once

#include <string_view>
#include <utility>

#include <servicelib/runtime/context.hpp>

namespace servicelib {

inline constexpr std::string_view kStreamIdHeader = "x-stream-id";

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

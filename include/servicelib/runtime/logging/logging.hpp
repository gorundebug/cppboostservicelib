/*
 * logging.hpp
 * Facade selecting a concrete log::Logger backend by type.
 *
 * Go analog: runtime/logging/logging.go (CreateLogsEngine). Go has two
 * independent backends behind this facade — Logrus (plain, no tracing
 * correlation) and the OTel SDK engine (runtime/telemetry/opentelemetry) —
 * because they're genuinely different client libraries. The Boost runtime
 * uses the structured JSON logger behind the same ServiceLib facade.
 *
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */
#pragma once

#include <iostream>
#include <stdexcept>

#include <servicelib/runtime/environment/log/json_logger.hpp>
#include <servicelib/runtime/environment/log/log.hpp>

namespace servicelib::logging {

enum class LogsEngineType {
  kBoost = 1,
};

inline log::Logger& createLogsEngine(LogsEngineType type) {
  switch (type) {
    case LogsEngineType::kBoost: {
      static log::JsonLogger logger(std::clog);
      return logger;
    }
  }
  throw std::invalid_argument("unsupported logs engine");
}

}  // namespace servicelib::logging

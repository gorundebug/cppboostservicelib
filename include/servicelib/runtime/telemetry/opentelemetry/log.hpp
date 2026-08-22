/*
 * log.hpp
 * OpenTelemetry-backed implementation of servicelib::log::Logger.
 *
 * This replaces only the canonical userver telemetry boundary; the
 * ServiceLib structured logging interface and typed fields are unchanged.
 */
#pragma once

#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include <opentelemetry/common/attribute_value.h>
#include <opentelemetry/common/key_value_iterable_view.h>
#include <opentelemetry/logs/logger.h>
#include <opentelemetry/nostd/shared_ptr.h>
#include <opentelemetry/nostd/string_view.h>
#include <opentelemetry/sdk/logs/logger_provider.h>

#include <servicelib/runtime/environment/log/log.hpp>

namespace servicelib::telemetry::opentelemetry_adapter {

namespace otel = ::opentelemetry;

class OpenTelemetryLogger final : public log::Logger {
 public:
  OpenTelemetryLogger(
      std::shared_ptr<otel::sdk::logs::LoggerProvider> provider,
      std::string_view name)
      : provider_(std::move(provider)) {
    if (!provider_) {
      throw std::invalid_argument("OpenTelemetry logger provider is null");
    }
    logger_ = provider_->GetLogger(
        otel::nostd::string_view{name.data(), name.size()}, {}, {}, {},
        otel::common::NoopKeyValueIterable{});
    if (!logger_) {
      throw std::runtime_error("OpenTelemetry logger was not created");
    }
  }

  ~OpenTelemetryLogger() override { (void)shutdown(); }

  void debug(std::string_view msg,
             std::initializer_list<log::Field> fields) override {
    emit(otel::logs::Severity::kDebug, msg, fields);
  }

  void info(std::string_view msg,
            std::initializer_list<log::Field> fields) override {
    emit(otel::logs::Severity::kInfo, msg, fields);
  }

  void warn(std::string_view msg,
            std::initializer_list<log::Field> fields) override {
    emit(otel::logs::Severity::kWarn, msg, fields);
  }

  void error(std::string_view msg,
             std::initializer_list<log::Field> fields) override {
    emit(otel::logs::Severity::kError, msg, fields);
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

  [[nodiscard]] const std::shared_ptr<otel::sdk::logs::LoggerProvider>&
  provider() const noexcept {
    return provider_;
  }

 private:
  static otel::common::AttributeValue fieldValue(const log::Field& field) {
    return std::visit(
        [](const auto& value) -> otel::common::AttributeValue {
          using T = std::decay_t<decltype(value)>;
          if constexpr (std::is_same_v<T, std::string>) {
            return otel::nostd::string_view{value.data(), value.size()};
          } else {
            return value;
          }
        },
        field.value());
  }

  void emit(otel::logs::Severity severity, std::string_view message,
            std::initializer_list<log::Field> fields) noexcept {
    using Attribute =
        std::pair<otel::nostd::string_view, otel::common::AttributeValue>;
    std::vector<Attribute> attributes;
    attributes.reserve(fields.size());
    for (const auto& field : fields) {
      attributes.emplace_back(
          otel::nostd::string_view{field.key().data(), field.key().size()},
          fieldValue(field));
    }
    const otel::common::KeyValueIterableView<decltype(attributes)> view(
        attributes);
    logger_->Log(severity,
                 otel::nostd::string_view{message.data(), message.size()},
                 view);
  }

  std::shared_ptr<otel::sdk::logs::LoggerProvider> provider_;
  otel::nostd::shared_ptr<otel::logs::Logger> logger_;
  std::mutex shutdown_mutex_;
  bool shutdown_{false};
};

}  // namespace servicelib::telemetry::opentelemetry_adapter

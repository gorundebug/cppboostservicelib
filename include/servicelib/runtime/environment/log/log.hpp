/*
 * log.hpp
 * C++ streams API — engine-agnostic structured logging interface
 *
 * Mirrors servicelib's Go implementation
 * (runtime/environment/log/log.go): a Logger abstract interface + Field
 * value type for structured key-value logging. No concrete backend
 * dependency here — see servicelib/telemetry/userver/log.hpp for the
 * userver-backed implementation.
 *
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */
#pragma once

#include <cstdint>
#include <exception>
#include <initializer_list>
#include <string>
#include <string_view>
#include <type_traits>
#include <variant>

namespace servicelib::log {

enum class Level : uint8_t { kDebug, kInfo, kWarn, kError };

// Structured log field. Go analog: log.Field + Str/Int64/Float64/Bool/Err.
// Go hand-rolls a tagged union (numVal/strVal/anyVal) to dodge interface{}
// boxing — a workaround for Go lacking a sum type. C++ has one
// (std::variant), so there's no reason to reimplement that workaround here.
class Field {
 public:
  using Value = std::variant<std::string, int64_t, double, bool>;

  static Field Str(std::string key, std::string val) {
    return Field(std::move(key), Value(std::in_place_type<std::string>, std::move(val)));
  }

  static Field Int64(std::string key, int64_t val) {
    return Field(std::move(key), Value(std::in_place_type<int64_t>, val));
  }

  static Field Float64(std::string key, double val) {
    return Field(std::move(key), Value(std::in_place_type<double>, val));
  }

  static Field Bool(std::string key, bool val) {
    return Field(std::move(key), Value(std::in_place_type<bool>, val));
  }

  // Stores the exception's what() text — captured at construction, since
  // std::exception_ptr cannot be safely inspected outside a catch block.
  static Field Err(const std::exception& err) { return Field::Str("error", err.what()); }
  static Field Err(std::string_view what) { return Field::Str("error", std::string(what)); }

  [[nodiscard]] const std::string& key() const noexcept { return key_; }
  [[nodiscard]] const Value& value() const noexcept { return value_; }

  [[nodiscard]] std::string stringValue() const {
    return std::visit(
        [](const auto& v) -> std::string {
          using T = std::decay_t<decltype(v)>;
          if constexpr (std::is_same_v<T, std::string>) {
            return v;
          } else if constexpr (std::is_same_v<T, bool>) {
            return v ? "true" : "false";
          } else {
            return std::to_string(v);
          }
        },
        value_);
  }

 private:
  Field(std::string key, Value value) : key_(std::move(key)), value_(std::move(value)) {}

  std::string key_;
  Value       value_;
};

// Go analog: log.Logger. Implementations must be safe to call from any
// coroutine/thread.
class Logger {
 public:
  virtual ~Logger() = default;

  virtual void debug(std::string_view msg, std::initializer_list<Field> fields = {}) = 0;
  virtual void info(std::string_view msg, std::initializer_list<Field> fields = {}) = 0;
  virtual void warn(std::string_view msg, std::initializer_list<Field> fields = {}) = 0;
  virtual void error(std::string_view msg, std::initializer_list<Field> fields = {}) = 0;
};

// Discards everything. Returned by IServiceEnvironment implementations that
// have no logging backend wired up.
class NoopLogger final : public Logger {
 public:
  void debug(std::string_view, std::initializer_list<Field>) override {}
  void info(std::string_view, std::initializer_list<Field>) override {}
  void warn(std::string_view, std::initializer_list<Field>) override {}
  void error(std::string_view, std::initializer_list<Field>) override {}

  static Logger& instance() {
    static NoopLogger logger;
    return logger;
  }
};

}  // namespace servicelib::log

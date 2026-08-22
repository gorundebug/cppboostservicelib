#pragma once

#include <servicelib/runtime/environment/log/log.hpp>

#include <chrono>
#include <iomanip>
#include <mutex>
#include <ostream>
#include <sstream>
#include <string>

namespace servicelib::log {

class JsonLogger final : public Logger {
 public:
  explicit JsonLogger(std::ostream& output, Level minimum = Level::kInfo)
      : output_(output), minimum_(minimum) {}

  void debug(std::string_view message,
             std::initializer_list<Field> fields = {}) override {
    Write(Level::kDebug, message, fields);
  }
  void info(std::string_view message,
            std::initializer_list<Field> fields = {}) override {
    Write(Level::kInfo, message, fields);
  }
  void warn(std::string_view message,
            std::initializer_list<Field> fields = {}) override {
    Write(Level::kWarn, message, fields);
  }
  void error(std::string_view message,
             std::initializer_list<Field> fields = {}) override {
    Write(Level::kError, message, fields);
  }

 private:
  static const char* Name(Level level) {
    switch (level) {
      case Level::kDebug:
        return "debug";
      case Level::kInfo:
        return "info";
      case Level::kWarn:
        return "warn";
      case Level::kError:
        return "error";
    }
    return "unknown";
  }
  static std::string Escape(std::string_view value) {
    std::ostringstream output;
    for (const unsigned char character : value) {
      switch (character) {
        case '"': output << "\\\""; break;
        case '\\': output << "\\\\"; break;
        case '\n': output << "\\n"; break;
        case '\r': output << "\\r"; break;
        case '\t': output << "\\t"; break;
        default:
          if (character < 0x20)
            output << "\\u" << std::hex << std::setw(4) << std::setfill('0')
                   << static_cast<unsigned>(character);
          else
            output << static_cast<char>(character);
      }
    }
    return output.str();
  }
  static void WriteValue(std::ostream& output, const Field::Value& value) {
    std::visit(
        [&](const auto& typed) {
          using T = std::decay_t<decltype(typed)>;
          if constexpr (std::is_same_v<T, std::string>)
            output << '"' << Escape(typed) << '"';
          else if constexpr (std::is_same_v<T, bool>)
            output << (typed ? "true" : "false");
          else
            output << typed;
        },
        value);
  }
  void Write(Level level, std::string_view message,
             std::initializer_list<Field> fields) {
    if (level < minimum_) return;
    const auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                               std::chrono::system_clock::now()
                                   .time_since_epoch())
                               .count();
    std::lock_guard lock(mutex_);
    output_ << "{\"timestamp_unix_ms\":" << timestamp << ",\"level\":\""
            << Name(level) << "\",\"message\":\"" << Escape(message)
            << '"';
    for (const auto& field : fields) {
      output_ << ",\"" << Escape(field.key()) << "\":";
      WriteValue(output_, field.value());
    }
    output_ << "}\n";
    output_.flush();
  }

  std::ostream& output_;
  Level minimum_;
  std::mutex mutex_;
};

}  // namespace servicelib::log

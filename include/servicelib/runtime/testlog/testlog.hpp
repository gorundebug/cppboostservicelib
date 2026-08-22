/*
 * testlog.hpp
 * In-memory Logger for use in automated tests. Captures every log call so
 * assertions can be made on what was logged.
 *
 * Go analog: runtime/testlog/testlog.go (TestLog).
 *
 * Usage:
 *   servicelib::testlog::TestLog log;
 *   // wire log into IServiceEnvironment::getLogger()
 *   doWork();
 *   auto entries = log.entries();
 *   ASSERT_EQ(entries[0].level, servicelib::log::Level::kError);
 *
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */
#pragma once

#include <string>
#include <vector>

#include <mutex>

#include <servicelib/runtime/environment/log/log.hpp>

namespace servicelib::testlog {

struct Entry {
  log::Level level;
  std::string message;
  std::vector<log::Field> fields;
};

// engine::Mutex (not std::mutex): everything that would exercise this
// double — Caller, TaskPoolImpl, ... — only runs inside a userver
// coroutine, so the runtime's own no-blocking-primitives-in-coroutines
// rule applies here too.
class TestLog final : public log::Logger {
 public:
  void debug(std::string_view msg, std::initializer_list<log::Field> fields) override {
    record(log::Level::kDebug, msg, fields);
  }
  void info(std::string_view msg, std::initializer_list<log::Field> fields) override {
    record(log::Level::kInfo, msg, fields);
  }
  void warn(std::string_view msg, std::initializer_list<log::Field> fields) override {
    record(log::Level::kWarn, msg, fields);
  }
  void error(std::string_view msg, std::initializer_list<log::Field> fields) override {
    record(log::Level::kError, msg, fields);
  }

  // Snapshot of all recorded log entries.
  [[nodiscard]] std::vector<Entry> entries() const {
    std::lock_guard<std::mutex> lock(mu_);
    return entries_;
  }

  // Snapshot filtered to a specific level.
  [[nodiscard]] std::vector<Entry> entriesAtLevel(log::Level level) const {
    std::vector<Entry> out;
    for (auto& e : entries()) {
      if (e.level == level) {
        out.push_back(e);
      }
    }
    return out;
  }

  void reset() {
    std::lock_guard<std::mutex> lock(mu_);
    entries_.clear();
  }

 private:
  void record(log::Level level, std::string_view msg, std::initializer_list<log::Field> fields) {
    std::lock_guard<std::mutex> lock(mu_);
    entries_.push_back(Entry{level, std::string(msg), std::vector<log::Field>(fields)});
  }

  mutable std::mutex mu_;
  std::vector<Entry> entries_;
};

}  // namespace servicelib::testlog


/*
 * Copyright (c) 2026 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 * Licensed under the MIT License. See the
 * [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */
#pragma once

#include <array>
#include <chrono>
#include <cstdint>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>

#include <openssl/evp.h>

namespace servicelib {

enum class ScheduleBackend { kLocal, kTemporal };

struct ScheduleTrigger {
  std::string triggerId;
  std::string scheduleId;
  std::chrono::system_clock::time_point scheduledAt;
  std::chrono::system_clock::time_point firedAt;
  ScheduleBackend backend{ScheduleBackend::kLocal};

  bool operator==(const ScheduleTrigger&) const = default;
};

namespace detail {

inline std::string FormatScheduleTimestamp(
    std::chrono::system_clock::time_point value) {
  using namespace std::chrono;
  const auto wholeSeconds = floor<seconds>(value);
  const auto day = floor<days>(wholeSeconds);
  const year_month_day date{sys_days{day}};
  const hh_mm_ss time{wholeSeconds - day};
  const auto nanos = duration_cast<nanoseconds>(value - wholeSeconds).count();

  std::ostringstream output;
  output << std::setfill('0') << std::setw(4) << static_cast<int>(date.year())
         << '-' << std::setw(2) << static_cast<unsigned>(date.month()) << '-'
         << std::setw(2) << static_cast<unsigned>(date.day()) << 'T'
         << std::setw(2) << time.hours().count() << ':' << std::setw(2)
         << time.minutes().count() << ':' << std::setw(2)
         << time.seconds().count();
  if (nanos != 0) {
    std::ostringstream fraction;
    fraction << std::setfill('0') << std::setw(9) << nanos;
    auto digits = fraction.str();
    while (digits.back() == '0') digits.pop_back();
    output << '.' << digits;
  }
  output << 'Z';
  return output.str();
}

inline std::string ScheduleSha256(std::string_view value) {
  std::array<unsigned char, EVP_MAX_MD_SIZE> digest{};
  unsigned int digestSize = 0;
  if (EVP_Digest(value.data(), value.size(), digest.data(), &digestSize,
                 EVP_sha256(), nullptr) != 1 ||
      digestSize != 32) {
    throw std::runtime_error("cannot compute schedule trigger identity");
  }
  static constexpr char kHex[] = "0123456789abcdef";
  std::string result(digestSize * 2, '\0');
  for (std::size_t index = 0; index < digestSize; ++index) {
    result[index * 2] = kHex[digest[index] >> 4];
    result[index * 2 + 1] = kHex[digest[index] & 0x0f];
  }
  return result;
}

}  // namespace detail

inline ScheduleTrigger MakeScheduleTrigger(
    int endpointId, std::string scheduleId,
    std::chrono::system_clock::time_point scheduledAt,
    std::chrono::system_clock::time_point firedAt, ScheduleBackend backend) {
  if (endpointId < 1) {
    throw std::invalid_argument("endpointId must be positive");
  }
  if (scheduleId.empty()) {
    throw std::invalid_argument("scheduleId must not be empty");
  }
  const auto identity = "servicegen:schedule-trigger:v1\n" +
                        std::to_string(endpointId) + "\n" + scheduleId + "\n" +
                        detail::FormatScheduleTimestamp(scheduledAt);
  return ScheduleTrigger{detail::ScheduleSha256(identity), std::move(scheduleId),
                         scheduledAt, firedAt, backend};
}

inline int NormalizeTemporalPriority(int priority) noexcept {
  if (priority <= -2) return 1;
  if (priority == -1) return 2;
  if (priority == 0) return 3;
  if (priority == 1) return 4;
  return 5;
}

}  // namespace servicelib

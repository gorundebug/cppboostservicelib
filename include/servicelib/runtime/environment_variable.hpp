#pragma once

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <string>

namespace servicelib {

inline bool EnvironmentFlagEnabled(const char* name) {
  const char* raw = std::getenv(name);
  if (!raw) return false;
  std::string value{raw};
  value.erase(value.begin(), std::find_if(value.begin(), value.end(), [](unsigned char ch) {
                return !std::isspace(ch);
              }));
  value.erase(std::find_if(value.rbegin(), value.rend(), [](unsigned char ch) {
                return !std::isspace(ch);
              }).base(),
              value.end());
  std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
    return static_cast<char>(std::tolower(ch));
  });
  return value == "1" || value == "true" || value == "yes" || value == "on";
}

}  // namespace servicelib

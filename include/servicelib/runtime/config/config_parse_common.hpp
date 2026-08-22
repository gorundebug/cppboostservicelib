#pragma once

#include <initializer_list>
#include <string>
#include <string_view>
#include <unordered_map>

#include <servicelib/runtime/config/yaml_value.hpp>

namespace servicelib::config {

using PropertiesMap = std::unordered_map<std::string, YamlValue>;

namespace detail {

inline const YamlValue* FindProperty(const PropertiesMap& properties,
                                     const std::string& name) {
  const auto it = properties.find(name);
  return it != properties.end() ? &it->second : nullptr;
}

template <typename T>
void ParseFunctionFields(const YamlValue& value, T& result) {
  result.functionName = value["functionName"].template As<std::string>("");
  result.functionPackage =
      value["functionPackage"].template As<std::string>("");
  result.publicFunction = value["publicFunction"].template As<bool>(false);
  result.functionDescription =
      value["functionDescription"].template As<std::string>("");
  result.functionInitializerGroup =
      value["functionInitializerGroup"].template As<std::string>("");
  result.functionModule =
      value["functionModule"].template As<std::string>("");
}

inline void ParseRemainingProperties(
    const YamlValue& value,
    std::initializer_list<std::string_view> knownKeys,
    PropertiesMap& properties) {
  if (!value.IsObject()) {
    return;
  }
  for (auto it = value.begin(); it != value.end(); ++it) {
    const std::string key = it.GetName();
    bool known = false;
    for (const auto& candidate : knownKeys) {
      if (candidate == key) {
        known = true;
        break;
      }
    }
    if (!known) {
      properties.emplace(key, *it);
    }
  }
}

}  // namespace detail

}  // namespace servicelib::config

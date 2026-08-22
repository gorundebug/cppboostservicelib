#pragma once

#include <yaml-cpp/yaml.h>

#include <filesystem>
#include <fstream>
#include <iterator>
#include <stdexcept>
#include <string>
#include <string_view>

namespace servicelib::config {

inline YAML::Node DeepMerge(const YAML::Node& base,
                            const YAML::Node& overrides) {
  if (!base || !base.IsMap() || !overrides || !overrides.IsMap()) {
    return YAML::Clone(overrides);
  }

  YAML::Node result = YAML::Clone(base);
  for (const auto& entry : overrides) {
    if (!entry.first.IsScalar()) {
      throw std::invalid_argument("configuration mapping keys must be scalar");
    }
    const auto key = entry.first.as<std::string>();
    const auto current = result[key];
    result[key] = current ? DeepMerge(current, entry.second)
                          : YAML::Clone(entry.second);
  }
  return result;
}

inline std::string ReadConfigFile(const std::filesystem::path& path) {
  if (path.empty()) {
    return {};
  }
  std::ifstream input(path, std::ios::binary);
  if (!input) {
    throw std::runtime_error("cannot read configuration " + path.string());
  }
  return {std::istreambuf_iterator<char>(input),
          std::istreambuf_iterator<char>()};
}

inline YAML::Node ParseYaml(std::string_view contents,
                            const std::filesystem::path& source = {}) {
  try {
    auto value = YAML::Load(std::string(contents));
    if (!value || value.IsNull()) {
      return YAML::Node(YAML::NodeType::Map);
    }
    if (!value.IsMap()) {
      throw std::invalid_argument("configuration root must be a mapping: " +
                                  source.string());
    }
    return value;
  } catch (const std::invalid_argument&) {
    throw;
  } catch (const std::exception& error) {
    throw std::runtime_error("cannot parse configuration " + source.string() +
                             ": " + error.what());
  }
}

inline YAML::Node ReadYamlFile(const std::filesystem::path& path) {
  return ParseYaml(ReadConfigFile(path), path);
}

}  // namespace servicelib::config

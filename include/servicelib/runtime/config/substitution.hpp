#pragma once

#include <servicelib/runtime/config/yaml.hpp>

#include <cstdlib>
#include <filesystem>
#include <functional>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>

namespace servicelib::config {

struct SubstitutionOptions final {
  YAML::Node variables;
  std::filesystem::path baseDirectory;
  std::function<std::optional<std::string>(std::string_view)> environment;
};

namespace detail {
inline std::optional<std::string> Environment(
    const SubstitutionOptions& options, std::string_view name) {
  if (options.environment) return options.environment(name);
  if (const char* value = std::getenv(std::string(name).c_str()))
    return std::string(value);
  return std::nullopt;
}

inline YAML::Node ParseReplacement(const std::string& value) {
  try {
    const auto parsed = YAML::Load(value);
    return parsed && !parsed.IsNull() ? parsed : YAML::Node(value);
  } catch (...) {
    return YAML::Node(value);
  }
}

inline std::string ReadSubstitutionFile(const std::filesystem::path& path) {
  auto value = ReadConfigFile(path);
  while (!value.empty() && (value.back() == '\n' || value.back() == '\r'))
    value.pop_back();
  return value;
}

inline YAML::Node SubstituteNode(const YAML::Node&, const SubstitutionOptions&);

inline YAML::Node SubstituteScalar(const YAML::Node& node,
                                   const SubstitutionOptions& options) {
  const auto source = node.Scalar();
  if (source.size() > 1 && source.front() == '$' && source[1] != '{') {
    const auto name = source.substr(1);
    const auto variable =
        options.variables.IsDefined() && !options.variables.IsNull()
            ? options.variables[name]
            : YAML::Node{};
    if (!variable)
      throw std::runtime_error("configuration variable is not defined: " +
                               name);
    return SubstituteNode(variable, options);
  }
  if (source.starts_with("${file:") && source.ends_with('}')) {
    auto path = std::filesystem::path(
        source.substr(7, source.size() - 8));
    if (path.is_relative()) path = options.baseDirectory / path;
    return ParseReplacement(ReadSubstitutionFile(path));
  }

  std::string result;
  std::size_t cursor{};
  bool changed{};
  while (cursor < source.size()) {
    const auto begin = source.find("${", cursor);
    if (begin == std::string::npos) {
      result.append(source, cursor, std::string::npos);
      break;
    }
    result.append(source, cursor, begin - cursor);
    const auto end = source.find('}', begin + 2);
    if (end == std::string::npos)
      throw std::runtime_error("unterminated configuration substitution: " +
                               source);
    const auto expression = source.substr(begin + 2, end - begin - 2);
    const auto separator = expression.find(":-");
    const auto name = expression.substr(0, separator);
    auto value = Environment(options, name);
    if (!value && separator != std::string::npos)
      value = expression.substr(separator + 2);
    if (!value)
      throw std::runtime_error("environment variable is not defined: " + name);
    result += *value;
    cursor = end + 1;
    changed = true;
  }
  if (!changed) return YAML::Clone(node);
  return ParseReplacement(result);
}

inline YAML::Node SubstituteMap(const YAML::Node& node,
                                const SubstitutionOptions& options) {
  YAML::Node result(YAML::NodeType::Map);
  for (const auto& entry : node) {
    const auto key = entry.first.as<std::string>();
    if (key.ends_with("#env") || key.ends_with("#fallback") ||
        key.ends_with("#file"))
      continue;

    YAML::Node selected = entry.second;
    const auto environmentKey = node[key + "#env"];
    const auto fileKey = node[key + "#file"];
    const auto fallbackKey = node[key + "#fallback"];
    if (environmentKey) {
      const auto value = Environment(options, environmentKey.as<std::string>());
      if (value)
        selected = ParseReplacement(*value);
      else if (fallbackKey)
        selected = fallbackKey;
    } else if (fileKey) {
      auto path = std::filesystem::path(fileKey.as<std::string>());
      if (path.is_relative()) path = options.baseDirectory / path;
      selected = ParseReplacement(ReadSubstitutionFile(path));
    }
    result[key] = SubstituteNode(selected, options);
  }

  // A fallback may define a key that has no ordinary value.
  for (const auto& entry : node) {
    const auto decorated = entry.first.as<std::string>();
    if (!decorated.ends_with("#fallback")) continue;
    const auto key = decorated.substr(0, decorated.size() - 9);
    if (result[key]) continue;
    const auto environmentKey = node[key + "#env"];
    if (environmentKey) {
      const auto value = Environment(options, environmentKey.as<std::string>());
      result[key] = value ? ParseReplacement(*value)
                          : SubstituteNode(entry.second, options);
    } else {
      result[key] = SubstituteNode(entry.second, options);
    }
  }
  return result;
}

inline YAML::Node SubstituteNode(const YAML::Node& node,
                                 const SubstitutionOptions& options) {
  if (!node) return {};
  if (node.IsScalar()) return SubstituteScalar(node, options);
  if (node.IsSequence()) {
    YAML::Node result(YAML::NodeType::Sequence);
    for (const auto& item : node) result.push_back(SubstituteNode(item, options));
    return result;
  }
  if (node.IsMap()) return SubstituteMap(node, options);
  return YAML::Clone(node);
}
}  // namespace detail

inline YAML::Node SubstituteConfiguration(
    const YAML::Node& root, SubstitutionOptions options = {}) {
  if ((!options.variables.IsDefined() || options.variables.IsNull()) &&
      root.IsDefined() && root.IsMap()) {
    const auto variables = root["variables"];
    if (variables.IsDefined()) options.variables = variables;
  }
  return detail::SubstituteNode(root, options);
}

}  // namespace servicelib::config

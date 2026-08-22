#pragma once

#include <servicelib/runtime/config/substitution.hpp>
#include <servicelib/runtime/config/yaml.hpp>
#include <servicelib/runtime/config/yaml_value.hpp>

namespace servicelib::config {

inline YamlValue DeepMerge(const YamlValue& base,
                           const YamlValue& overrides) {
  return YamlValue(DeepMerge(base.node(), overrides.node()));
}

}  // namespace servicelib::config

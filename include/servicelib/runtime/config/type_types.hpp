/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */
#pragma once

#include <string>


#include <servicelib/api/serviceapi.hpp>
#include <servicelib/api/serviceapi_parse.hpp>
#include <servicelib/runtime/config/config_parse_common.hpp>

namespace servicelib::config {

struct TypeConfig {
    std::string name;
    servicelib::api::DataType type{};
    std::string typeDefinition;
    std::string typeImport;
    std::string valueType;
    std::string keyType;
    std::string package;
    std::string module;
    servicelib::api::TypeDefinitionFormat definitionFormat{};
    bool publicType{};
    bool transferByValue{};
    bool useAlias{};
    PropertiesMap properties;

    const YamlValue* GetProperty(const std::string& propName) const {
      return detail::FindProperty(properties, propName);
    }
};

inline TypeConfig Parse(const YamlValue& value,
                        TypeTag<TypeConfig>) {
  TypeConfig result;
  result.name = value["name"].As<std::string>("");
  result.type = value["type"].As<servicelib::api::DataType>(servicelib::api::DataType::kUndefined);
  result.typeDefinition = value["typeDefinition"].As<std::string>("");
  result.typeImport = value["typeImport"].As<std::string>("");
  result.valueType = value["valueType"].As<std::string>("");
  result.keyType = value["keyType"].As<std::string>("");
  result.package = value["package"].As<std::string>("");
  result.module = value["module"].As<std::string>("");
  result.definitionFormat = value["definitionFormat"].As<servicelib::api::TypeDefinitionFormat>(
      servicelib::api::TypeDefinitionFormat::kUndefined);
  result.publicType = value["publicType"].As<bool>(false);
  result.transferByValue = value["transferByValue"].As<bool>(false);
  result.useAlias = value["useAlias"].As<bool>(false);
  detail::ParseRemainingProperties(
      value,
      {"name", "type", "typeDefinition", "typeImport", "valueType", "keyType", "package",
       "module", "definitionFormat", "publicType", "transferByValue", "useAlias"},
      result.properties);
  return result;
}

}  // namespace servicelib::config

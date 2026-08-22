/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */
#pragma once

#include <functional>
#include <string>
#include <utility>
#include <variant>


#include <servicelib/api/serviceapi.hpp>
#include <servicelib/api/serviceapi_parse.hpp>
#include <servicelib/runtime/config/config_parse_common.hpp>

namespace servicelib::config {

struct HttpEndpointConfig {
    int id{};
    std::string name;
    int idDataConnector{};
    servicelib::api::HTTPMethodType httpMethodType{};
    std::string path;
    std::string functionName;
    std::string functionPackage;
    bool publicFunction{};
    std::string functionDescription;
    std::string functionInitializerGroup;
    std::string functionModule;
    PropertiesMap properties;

    const YamlValue* GetProperty(const std::string& propName) const {
      return detail::FindProperty(properties, propName);
    }
};

struct GrpcEndpointConfig {
    int id{};
    std::string name;
    int idDataConnector{};
    servicelib::api::GrpcMethodType grpcMethodType{};
    std::string methodName;
    std::string functionName;
    std::string functionPackage;
    bool publicFunction{};
    std::string functionDescription;
    std::string functionInitializerGroup;
    std::string functionModule;
    PropertiesMap properties;

    const YamlValue* GetProperty(const std::string& propName) const {
      return detail::FindProperty(properties, propName);
    }
};

struct KafkaEndpointConfig {
    int id{};
    std::string name;
    int idDataConnector{};
    bool enabled{};
    bool createTopic{};
    std::string topic;
    int partitions{};
    std::string consumerGroup;
    int replicationFactor{};
    std::string functionName;
    std::string functionPackage;
    bool publicFunction{};
    std::string functionDescription;
    std::string functionInitializerGroup;
    std::string functionModule;
    PropertiesMap properties;

    const YamlValue* GetProperty(const std::string& propName) const {
      return detail::FindProperty(properties, propName);
    }
};

struct CustomEndpointConfig {
    int id{};
    std::string name;
    int idDataConnector{};
    std::string functionName;
    std::string functionPackage;
    bool publicFunction{};
    std::string functionDescription;
    std::string functionInitializerGroup;
    std::string functionModule;
    PropertiesMap properties;

    const YamlValue* GetProperty(const std::string& propName) const {
      return detail::FindProperty(properties, propName);
    }
};

// Аналог Go interface EndpointConfig.
class EndpointConfigRef {
 public:
  using Variant = std::variant<
      std::reference_wrapper<const HttpEndpointConfig>,
      std::reference_wrapper<const GrpcEndpointConfig>,
      std::reference_wrapper<const KafkaEndpointConfig>,
      std::reference_wrapper<const CustomEndpointConfig>>;

 private:
  template <typename F>
  decltype(auto) Visit(F&& f) const {
    return std::visit(
        [&](const auto& ref) -> decltype(auto) { return std::forward<F>(f)(ref.get()); },
        value_);
  }

 public:
  EndpointConfigRef(const HttpEndpointConfig& v)   : value_(std::cref(v)) {}
  EndpointConfigRef(const GrpcEndpointConfig& v)   : value_(std::cref(v)) {}
  EndpointConfigRef(const KafkaEndpointConfig& v)  : value_(std::cref(v)) {}
  EndpointConfigRef(const CustomEndpointConfig& v) : value_(std::cref(v)) {}

  int GetID() const {
    return Visit([](const auto& c) { return c.id; });
  }

  const std::string& GetName() const {
    return Visit([](const auto& c) -> const std::string& { return c.name; });
  }

  int GetIdDataConnector() const {
    return Visit([](const auto& c) { return c.idDataConnector; });
  }

  template <typename T>
  const T* As() const {
    if (auto* p = std::get_if<std::reference_wrapper<const T>>(&value_)) {
      return &p->get();
    }
    return nullptr;
  }

 private:
  Variant value_;
};

inline HttpEndpointConfig Parse(const YamlValue& value,
                                TypeTag<HttpEndpointConfig>) {
  HttpEndpointConfig result;
  result.id = value["id"].As<int>(0);
  result.name = value["name"].As<std::string>("");
  result.idDataConnector = value["idDataConnector"].As<int>(0);
  result.httpMethodType = value["httpMethodType"].As<servicelib::api::HTTPMethodType>(
      servicelib::api::HTTPMethodType::kUndefined);
  result.path = value["path"].As<std::string>("");
  detail::ParseFunctionFields(value, result);
  detail::ParseRemainingProperties(
      value,
      {"id", "name", "idDataConnector", "httpMethodType", "path", "functionName",
       "functionPackage", "publicFunction", "functionDescription", "functionInitializerGroup",
       "functionModule"},
      result.properties);
  return result;
}

inline GrpcEndpointConfig Parse(const YamlValue& value,
                                TypeTag<GrpcEndpointConfig>) {
  GrpcEndpointConfig result;
  result.id = value["id"].As<int>(0);
  result.name = value["name"].As<std::string>("");
  result.idDataConnector = value["idDataConnector"].As<int>(0);
  result.grpcMethodType = value["grpcMethodType"].As<servicelib::api::GrpcMethodType>(
      servicelib::api::GrpcMethodType::kUndefined);
  result.methodName = value["methodName"].As<std::string>("");
  detail::ParseFunctionFields(value, result);
  detail::ParseRemainingProperties(
      value,
      {"id", "name", "idDataConnector", "grpcMethodType", "methodName", "functionName",
       "functionPackage", "publicFunction", "functionDescription", "functionInitializerGroup",
       "functionModule"},
      result.properties);
  return result;
}

inline KafkaEndpointConfig Parse(const YamlValue& value,
                                 TypeTag<KafkaEndpointConfig>) {
  KafkaEndpointConfig result;
  result.id = value["id"].As<int>(0);
  result.name = value["name"].As<std::string>("");
  result.idDataConnector = value["idDataConnector"].As<int>(0);
  result.enabled = value["enabled"].As<bool>(false);
  result.createTopic = value["createTopic"].As<bool>(false);
  result.topic = value["topic"].As<std::string>("");
  result.partitions = value["partitions"].As<int>(0);
  result.consumerGroup = value["consumerGroup"].As<std::string>("");
  result.replicationFactor = value["replicationFactor"].As<int>(0);
  detail::ParseFunctionFields(value, result);
  detail::ParseRemainingProperties(
      value,
      {"id", "name", "idDataConnector", "enabled", "createTopic", "topic", "partitions", "consumerGroup",
       "replicationFactor", "functionName", "functionPackage", "publicFunction",
       "functionDescription", "functionInitializerGroup", "functionModule"},
      result.properties);
  return result;
}

inline CustomEndpointConfig Parse(const YamlValue& value,
                                  TypeTag<CustomEndpointConfig>) {
  CustomEndpointConfig result;
  result.id = value["id"].As<int>(0);
  result.name = value["name"].As<std::string>("");
  result.idDataConnector = value["idDataConnector"].As<int>(0);
  detail::ParseFunctionFields(value, result);
  detail::ParseRemainingProperties(
      value,
      {"id", "name", "idDataConnector", "functionName", "functionPackage", "publicFunction",
       "functionDescription", "functionInitializerGroup", "functionModule"},
      result.properties);
  return result;
}

}  // namespace servicelib::config

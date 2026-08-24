/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */
#pragma once

#include <functional>
#include <stdexcept>
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

    servicelib::api::DataConnectorType GetType() const noexcept { return servicelib::api::DataConnectorType::kHTTP; }

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

    servicelib::api::DataConnectorType GetType() const noexcept { return servicelib::api::DataConnectorType::kGRPC; }

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

    servicelib::api::DataConnectorType GetType() const noexcept { return servicelib::api::DataConnectorType::kKafka; }

    const YamlValue* GetProperty(const std::string& propName) const {
      return detail::FindProperty(properties, propName);
    }
};

struct CronEndpointConfig {
    int id{};
    std::string name;
    int idDataConnector{};
    bool enabled{};
    std::string schedule;
    std::string timezone{"UTC"};
    servicelib::api::ScheduleOverlapPolicy overlapPolicy{servicelib::api::ScheduleOverlapPolicy::kSkip};
    servicelib::api::ScheduleMissedRunPolicy missedRunPolicy{servicelib::api::ScheduleMissedRunPolicy::kSkip};
    std::string functionName;
    std::string functionPackage;
    bool publicFunction{};
    std::string functionDescription;
    std::string functionInitializerGroup;
    std::string functionModule;
    PropertiesMap properties;

    servicelib::api::DataConnectorType GetType() const noexcept { return servicelib::api::DataConnectorType::kCron; }

    const YamlValue* GetProperty(const std::string& propName) const {
      return detail::FindProperty(properties, propName);
    }
};

struct TemporalEndpointConfig {
    int id{};
    std::string name;
    int idDataConnector{};
    bool enabled{};
    std::string taskQueue;
    std::string schedule;
    std::string scheduleId;
    std::string timezone{"UTC"};
    servicelib::api::ScheduleOverlapPolicy overlapPolicy{servicelib::api::ScheduleOverlapPolicy::kSkip};
    servicelib::api::ScheduleMissedRunPolicy missedRunPolicy{servicelib::api::ScheduleMissedRunPolicy::kSkip};
    int workflowExecutionTimeout{};
    int activityStartToCloseTimeout{};
    int activityHeartbeatTimeout{};
    int maximumAttempts{};
    std::string functionName;
    std::string functionPackage;
    bool publicFunction{};
    std::string functionDescription;
    std::string functionInitializerGroup;
    std::string functionModule;
    PropertiesMap properties;

    servicelib::api::DataConnectorType GetType() const noexcept { return servicelib::api::DataConnectorType::kTemporal; }

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

    servicelib::api::DataConnectorType GetType() const noexcept { return servicelib::api::DataConnectorType::kCustom; }

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
      std::reference_wrapper<const CronEndpointConfig>,
      std::reference_wrapper<const TemporalEndpointConfig>,
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
  EndpointConfigRef(const CronEndpointConfig& v)   : value_(std::cref(v)) {}
  EndpointConfigRef(const TemporalEndpointConfig& v) : value_(std::cref(v)) {}
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

  servicelib::api::DataConnectorType GetType() const {
    return Visit([](const auto& c) { return c.GetType(); });
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

inline CronEndpointConfig Parse(const YamlValue& value,
                                TypeTag<CronEndpointConfig>) {
  CronEndpointConfig result;
  result.id = value["id"].As<int>(0);
  result.name = value["name"].As<std::string>("");
  result.idDataConnector = value["idDataConnector"].As<int>(0);
  result.enabled = value["enabled"].As<bool>(false);
  result.schedule = value["schedule"].As<std::string>("");
  result.timezone = value["timezone"].As<std::string>("UTC");
  if (result.timezone != "UTC") {
    throw std::invalid_argument("scheduled endpoint timezone must be UTC");
  }
  result.overlapPolicy = value["overlapPolicy"].As<servicelib::api::ScheduleOverlapPolicy>(
      servicelib::api::ScheduleOverlapPolicy::kSkip);
  result.missedRunPolicy = value["missedRunPolicy"].As<servicelib::api::ScheduleMissedRunPolicy>(
      servicelib::api::ScheduleMissedRunPolicy::kSkip);
  detail::ParseFunctionFields(value, result);
  detail::ParseRemainingProperties(
      value,
      {"id", "name", "idDataConnector", "enabled", "schedule", "timezone",
       "overlapPolicy", "missedRunPolicy", "functionName", "functionPackage",
       "publicFunction", "functionDescription", "functionInitializerGroup", "functionModule"},
      result.properties);
  return result;
}

inline TemporalEndpointConfig Parse(
    const YamlValue& value,
    TypeTag<TemporalEndpointConfig>) {
  TemporalEndpointConfig result;
  result.id = value["id"].As<int>(0);
  result.name = value["name"].As<std::string>("");
  result.idDataConnector = value["idDataConnector"].As<int>(0);
  result.enabled = value["enabled"].As<bool>(false);
  result.taskQueue = value["taskQueue"].As<std::string>("");
  result.schedule = value["schedule"].As<std::string>("");
  result.scheduleId = value["scheduleId"].As<std::string>("");
  result.timezone = value["timezone"].As<std::string>("UTC");
  if (result.timezone != "UTC") {
    throw std::invalid_argument("scheduled endpoint timezone must be UTC");
  }
  result.overlapPolicy = value["overlapPolicy"].As<servicelib::api::ScheduleOverlapPolicy>(
      servicelib::api::ScheduleOverlapPolicy::kSkip);
  result.missedRunPolicy = value["missedRunPolicy"].As<servicelib::api::ScheduleMissedRunPolicy>(
      servicelib::api::ScheduleMissedRunPolicy::kSkip);
  result.workflowExecutionTimeout = value["workflowExecutionTimeout"].As<int>(0);
  result.activityStartToCloseTimeout = value["activityStartToCloseTimeout"].As<int>(0);
  result.activityHeartbeatTimeout = value["activityHeartbeatTimeout"].As<int>(0);
  result.maximumAttempts = value["maximumAttempts"].As<int>(0);
  detail::ParseFunctionFields(value, result);
  detail::ParseRemainingProperties(
      value,
      {"id", "name", "idDataConnector", "enabled", "taskQueue", "schedule",
       "scheduleId", "timezone", "overlapPolicy", "missedRunPolicy",
       "workflowExecutionTimeout", "activityStartToCloseTimeout",
       "activityHeartbeatTimeout", "maximumAttempts", "functionName",
       "functionPackage", "publicFunction", "functionDescription",
       "functionInitializerGroup", "functionModule"},
      result.properties);
  return result;
}

}  // namespace servicelib::config

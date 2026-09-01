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

struct HttpDataConnectorConfig {
    int id{};
    std::string name;
    servicelib::api::DataConnectorImplementation implementation{};
    std::string module;
    std::string host;
    int port{};
    bool useDedicatedListener{};
    PropertiesMap properties;

    servicelib::api::DataConnectorType GetType() const noexcept { return servicelib::api::DataConnectorType::kHTTP; }
    const YamlValue* GetProperty(const std::string& propName) const {
      return detail::FindProperty(properties, propName);
    }
};

struct GrpcDataConnectorConfig {
    int id{};
    std::string name;
    servicelib::api::DataConnectorImplementation implementation{};
    servicelib::api::ProgrammingLanguage programmingLanguage{};
    std::string module;
    std::string address;
    int connectionsCount{1};
    PropertiesMap properties;

    servicelib::api::DataConnectorType GetType() const noexcept { return servicelib::api::DataConnectorType::kGRPC; }
    const YamlValue* GetProperty(const std::string& propName) const {
      return detail::FindProperty(properties, propName);
    }
};

struct KafkaDataConnectorConfig {
    int id{};
    std::string name;
    servicelib::api::DataConnectorImplementation implementation{};
    servicelib::api::ProgrammingLanguage programmingLanguage{};
    std::string brokers;
    std::string version;
    float dialTimeout{};
    bool usePartitioner{};
    bool async{};
    servicelib::api::KafkaSecurityProtocol securityProtocol{};
    servicelib::api::KafkaSaslMechanism saslMechanism{};
    std::string username;
    std::string password;
    PropertiesMap properties;

    servicelib::api::DataConnectorType GetType() const noexcept { return servicelib::api::DataConnectorType::kKafka; }
    const YamlValue* GetProperty(const std::string& propName) const {
      return detail::FindProperty(properties, propName);
    }
};

struct CronDataConnectorConfig {
    int id{};
    std::string name;
    servicelib::api::DataConnectorImplementation implementation{};
    PropertiesMap properties;

    servicelib::api::DataConnectorType GetType() const noexcept { return servicelib::api::DataConnectorType::kCron; }
    const YamlValue* GetProperty(const std::string& propName) const {
      return detail::FindProperty(properties, propName);
    }
};

struct TemporalDataConnectorConfig {
    int id{};
    std::string name;
    servicelib::api::DataConnectorImplementation implementation{};
    std::string address;
    std::string namespaceName;
    std::string identity;
    std::string apiKey;
    bool tlsEnabled{};
    std::string tlsServerName;
    std::string tlsCaFile;
    std::string tlsCertFile;
    std::string tlsKeyFile;
    int maxConcurrentActivities{};
    int maxConcurrentWorkflows{};
    int workerStopTimeout{};
    PropertiesMap properties;

    servicelib::api::DataConnectorType GetType() const noexcept { return servicelib::api::DataConnectorType::kTemporal; }
    const YamlValue* GetProperty(const std::string& propName) const {
      return detail::FindProperty(properties, propName);
    }
};

struct CustomDataConnectorConfig {
    int id{};
    std::string name;
    servicelib::api::DataConnectorImplementation implementation{};
    PropertiesMap properties;

    servicelib::api::DataConnectorType GetType() const noexcept { return servicelib::api::DataConnectorType::kCustom; }
    const YamlValue* GetProperty(const std::string& propName) const {
      return detail::FindProperty(properties, propName);
    }
};

// Аналог Go interface DataConnectorConfig.
class DataConnectorConfigRef {
 public:
  using Variant = std::variant<
      std::reference_wrapper<const HttpDataConnectorConfig>,
      std::reference_wrapper<const GrpcDataConnectorConfig>,
      std::reference_wrapper<const KafkaDataConnectorConfig>,
      std::reference_wrapper<const CronDataConnectorConfig>,
      std::reference_wrapper<const TemporalDataConnectorConfig>,
      std::reference_wrapper<const CustomDataConnectorConfig>>;

 private:
  template <typename F>
  decltype(auto) Visit(F&& f) const {
    return std::visit(
        [&](const auto& ref) -> decltype(auto) { return std::forward<F>(f)(ref.get()); },
        value_);
  }

 public:
  DataConnectorConfigRef(const HttpDataConnectorConfig& v)   : value_(std::cref(v)) {}
  DataConnectorConfigRef(const GrpcDataConnectorConfig& v)   : value_(std::cref(v)) {}
  DataConnectorConfigRef(const KafkaDataConnectorConfig& v)  : value_(std::cref(v)) {}
  DataConnectorConfigRef(const CronDataConnectorConfig& v)   : value_(std::cref(v)) {}
  DataConnectorConfigRef(const TemporalDataConnectorConfig& v) : value_(std::cref(v)) {}
  DataConnectorConfigRef(const CustomDataConnectorConfig& v) : value_(std::cref(v)) {}

  int GetID() const {
    return Visit([](const auto& c) { return c.id; });
  }

  const std::string& GetName() const {
    return Visit([](const auto& c) -> const std::string& { return c.name; });
  }

  servicelib::api::DataConnectorType GetType() const {
    return Visit([](const auto& c) { return c.GetType(); });
  }

  servicelib::api::DataConnectorImplementation GetImplementation() const {
    return Visit([](const auto& c) { return c.implementation; });
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

inline HttpDataConnectorConfig Parse(
    const YamlValue& value,
    TypeTag<HttpDataConnectorConfig>) {
  HttpDataConnectorConfig result;
  result.id = value["id"].As<int>(0);
  result.name = value["name"].As<std::string>("");
  result.implementation = value["implementation"].As<servicelib::api::DataConnectorImplementation>(
      servicelib::api::DataConnectorImplementation::kUndefined);
  result.module = value["module"].As<std::string>("");
  result.host = value["host"].As<std::string>("");
  result.port = value["port"].As<int>(0);
  result.useDedicatedListener = value["useDedicatedListener"].As<bool>(false);
  detail::ParseRemainingProperties(
      value, {"id", "name", "implementation", "module", "host", "port", "useDedicatedListener"},
      result.properties);
  return result;
}

inline GrpcDataConnectorConfig Parse(
    const YamlValue& value,
    TypeTag<GrpcDataConnectorConfig>) {
  GrpcDataConnectorConfig result;
  result.id = value["id"].As<int>(0);
  result.name = value["name"].As<std::string>("");
  result.implementation = value["implementation"].As<servicelib::api::DataConnectorImplementation>(
      servicelib::api::DataConnectorImplementation::kUndefined);
  result.programmingLanguage = value["programmingLanguage"].As<servicelib::api::ProgrammingLanguage>(
      servicelib::api::ProgrammingLanguage::kUndefined);
  result.module = value["module"].As<std::string>("");
  result.address = value["address"].As<std::string>("");
  result.connectionsCount = value["connectionsCount"].As<int>(1);
	if (result.connectionsCount < 1) {
		throw std::invalid_argument(
			"gRPC connectionsCount must be at least 1");
	}
  detail::ParseRemainingProperties(
      value, {"id", "name", "implementation", "programmingLanguage", "module", "address", "connectionsCount"},
      result.properties);
  return result;
}

inline KafkaDataConnectorConfig Parse(
    const YamlValue& value,
    TypeTag<KafkaDataConnectorConfig>) {
  KafkaDataConnectorConfig result;
  result.id = value["id"].As<int>(0);
  result.name = value["name"].As<std::string>("");
  result.implementation = value["implementation"].As<servicelib::api::DataConnectorImplementation>(
      servicelib::api::DataConnectorImplementation::kUndefined);
  result.programmingLanguage = value["programmingLanguage"].As<servicelib::api::ProgrammingLanguage>(
      servicelib::api::ProgrammingLanguage::kUndefined);
  result.brokers = value["brokers"].As<std::string>("");
  result.version = value["version"].As<std::string>("");
  result.dialTimeout = value["dialTimeout"].As<float>(0.0F);
  result.usePartitioner = value["usePartitioner"].As<bool>(false);
  result.async = value["async"].As<bool>(false);
  result.securityProtocol = value["securityProtocol"].As<servicelib::api::KafkaSecurityProtocol>(
      servicelib::api::KafkaSecurityProtocol::kPLAINTEXT);
  result.saslMechanism = value["saslMechanism"].As<servicelib::api::KafkaSaslMechanism>(
      servicelib::api::KafkaSaslMechanism::kPLAIN);
  result.username = value["username"].As<std::string>("");
  result.password = value["password"].As<std::string>("");
  detail::ParseRemainingProperties(
      value,
      {"id", "name", "implementation", "programmingLanguage", "brokers", "version",
       "dialTimeout", "usePartitioner", "async", "securityProtocol", "saslMechanism",
       "username", "password"},
      result.properties);
  return result;
}

inline CustomDataConnectorConfig Parse(
    const YamlValue& value,
    TypeTag<CustomDataConnectorConfig>) {
  CustomDataConnectorConfig result;
  result.id = value["id"].As<int>(0);
  result.name = value["name"].As<std::string>("");
  result.implementation = value["implementation"].As<servicelib::api::DataConnectorImplementation>(
      servicelib::api::DataConnectorImplementation::kUndefined);
  detail::ParseRemainingProperties(value, {"id", "name", "implementation"}, result.properties);
  return result;
}

inline CronDataConnectorConfig Parse(
    const YamlValue& value,
    TypeTag<CronDataConnectorConfig>) {
  CronDataConnectorConfig result;
  result.id = value["id"].As<int>(0);
  result.name = value["name"].As<std::string>("");
  result.implementation = value["implementation"].As<servicelib::api::DataConnectorImplementation>(
      servicelib::api::DataConnectorImplementation::kUndefined);
  detail::ParseRemainingProperties(value, {"id", "name", "implementation"}, result.properties);
  return result;
}

inline TemporalDataConnectorConfig Parse(
    const YamlValue& value,
    TypeTag<TemporalDataConnectorConfig>) {
  TemporalDataConnectorConfig result;
  result.id = value["id"].As<int>(0);
  result.name = value["name"].As<std::string>("");
  result.implementation = value["implementation"].As<servicelib::api::DataConnectorImplementation>(
      servicelib::api::DataConnectorImplementation::kUndefined);
  result.address = value["address"].As<std::string>("");
  result.namespaceName = value["namespace"].As<std::string>("");
  result.identity = value["identity"].As<std::string>("");
  result.apiKey = value["apiKey"].As<std::string>("");
  result.tlsEnabled = value["tlsEnabled"].As<bool>(false);
  result.tlsServerName = value["tlsServerName"].As<std::string>("");
  result.tlsCaFile = value["tlsCaFile"].As<std::string>("");
  result.tlsCertFile = value["tlsCertFile"].As<std::string>("");
  result.tlsKeyFile = value["tlsKeyFile"].As<std::string>("");
  result.maxConcurrentActivities = value["maxConcurrentActivities"].As<int>(0);
  result.maxConcurrentWorkflows = value["maxConcurrentWorkflows"].As<int>(0);
  result.workerStopTimeout = value["workerStopTimeout"].As<int>(0);
  detail::ParseRemainingProperties(
      value,
      {"id", "name", "implementation", "address", "namespace", "identity",
       "apiKey", "tlsEnabled", "tlsServerName", "tlsCaFile", "tlsCertFile",
       "tlsKeyFile",
       "maxConcurrentActivities", "maxConcurrentWorkflows",
       "workerStopTimeout"},
      result.properties);
  return result;
}

}  // namespace servicelib::config

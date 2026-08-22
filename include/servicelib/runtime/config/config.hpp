/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the
 * [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */
#pragma once

#include <atomic>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>


#include <servicelib/api/serviceapi.hpp>
#include <servicelib/api/serviceapi_parse.hpp>
#include <servicelib/runtime/config/config_parse_common.hpp>
#include <servicelib/runtime/config/dataconnector_types.hpp>
#include <servicelib/runtime/config/endpoint_types.hpp>
#include <servicelib/runtime/config/link_types.hpp>
#include <servicelib/runtime/config/stream_types.hpp>
#include <servicelib/runtime/config/type_types.hpp>

namespace servicelib::config {

struct PoolConfig {
  std::string name;
  int executorsCount{};
  int queueCapacity{};
  PropertiesMap properties;

  const YamlValue* GetProperty(
      const std::string& propName) const {
    return detail::FindProperty(properties, propName);
  }
};

inline PoolConfig Parse(const YamlValue& value,
                        TypeTag<PoolConfig>) {
  PoolConfig result;
  result.name = value["name"].As<std::string>("");
  result.executorsCount = value["executorsCount"].As<int>(0);
  result.queueCapacity = value["queueCapacity"].As<int>(0);
  detail::ParseRemainingProperties(
      value, {"name", "executorsCount", "queueCapacity"}, result.properties);
  return result;
}

struct ModuleConfig {
  std::string name;
  std::string path;
  PropertiesMap properties;

  const YamlValue* GetProperty(
      const std::string& propName) const {
    return detail::FindProperty(properties, propName);
  }
};

inline ModuleConfig Parse(const YamlValue& value,
                          TypeTag<ModuleConfig>) {
  ModuleConfig result;
  result.name = value["name"].As<std::string>("");
  result.path = value["path"].As<std::string>("");
  detail::ParseRemainingProperties(value, {"name", "path"}, result.properties);
  return result;
}

struct ServiceConfig {
  int id{};
  std::string name;
  std::string color;
  std::optional<CallSemanticsGroup> defaultCallSemantics;
  int defaultGrpcTimeout{};
  servicelib::api::Environment environment{};
  std::string golangVersion;
  std::string grpcHost;
  int grpcPort{};
  std::string httpHost;
  int httpPort{};
  servicelib::api::LogLevel logLevel{};
  std::string metricsHandler;
  std::string startupHandler;
  std::string readinessHandler;
  std::string livenessHandler;
  servicelib::api::KubernetesWorkloadType kubernetesWorkloadType{};
  std::string modulePath;
  int shutdownTimeout{};
  std::string statusHandler;
  PropertiesMap properties;

  const YamlValue* GetProperty(
      const std::string& propName) const {
    return detail::FindProperty(properties, propName);
  }
};

inline ServiceConfig Parse(const YamlValue& value,
                           TypeTag<ServiceConfig>) {
  ServiceConfig result;
  result.id = value["id"].As<int>(0);
  result.name = value["name"].As<std::string>("");
  result.color = value["color"].As<std::string>("");
  if (const auto v = value["defaultCallSemantics"]; !v.IsMissing()) {
    if (v.IsString()) {
      const auto type = v.As<servicelib::api::CallSemantics>();
      if (type != servicelib::api::CallSemantics::kUndefined &&
          type != servicelib::api::CallSemantics::kInherited) {
        result.defaultCallSemantics = MakeCallSemanticsGroup(type);
      }
    } else {
      result.defaultCallSemantics = v.As<CallSemanticsGroup>();
    }
  }
  result.defaultGrpcTimeout = value["defaultGrpcTimeout"].As<int>(0);
  result.environment = value["environment"].As<servicelib::api::Environment>(
      servicelib::api::Environment::kUndefined);
  result.golangVersion = value["golangVersion"].As<std::string>("");
  result.grpcHost = value["grpcHost"].As<std::string>("");
  result.grpcPort = value["grpcPort"].As<int>(0);
  result.httpHost = value["httpHost"].As<std::string>("");
  result.httpPort = value["httpPort"].As<int>(0);
  result.logLevel = value["logLevel"].As<servicelib::api::LogLevel>(
      servicelib::api::LogLevel::kUndefined);
  result.metricsHandler = value["metricsHandler"].As<std::string>("");
  result.startupHandler = value["startupHandler"].As<std::string>("");
  result.readinessHandler = value["readinessHandler"].As<std::string>("");
  result.livenessHandler = value["livenessHandler"].As<std::string>("");
  result.kubernetesWorkloadType =
      value["kubernetesWorkloadType"].As<servicelib::api::KubernetesWorkloadType>(
          servicelib::api::KubernetesWorkloadType::kDeployment);
  result.modulePath = value["modulePath"].As<std::string>("");
  result.shutdownTimeout = value["shutdownTimeout"].As<int>(0);
  result.statusHandler = value["statusHandler"].As<std::string>("");
  detail::ParseRemainingProperties(
      value,
      {"id", "name", "color", "defaultCallSemantics", "defaultGrpcTimeout",
       "environment", "golangVersion", "grpcHost", "grpcPort", "httpHost",
       "httpPort", "logLevel", "metricsHandler", "startupHandler",
       "readinessHandler", "livenessHandler", "kubernetesWorkloadType", "modulePath",
       "shutdownTimeout", "statusHandler"},
      result.properties);
  return result;
}

// Аналог Go interface Config — реализуется пользователем.
class IConfig {
 public:
  virtual ~IConfig() = default;
  virtual std::vector<const ServiceConfig*> GetServices() const = 0;
  virtual std::vector<StreamConfigRef> GetStreams() const = 0;
  virtual std::vector<DataConnectorConfigRef> GetDataConnectors() const = 0;
  virtual std::vector<EndpointConfigRef> GetEndpoints() const = 0;
  virtual std::vector<const PoolConfig*> GetPools() const = 0;
  virtual std::vector<const LinkConfig*> GetLinks() const = 0;
  virtual std::vector<const ModuleConfig*> GetModules() const = 0;
  virtual std::vector<const TypeConfig*> GetTypes() const = 0;
  virtual const YamlValue* GetProperty(
      const std::string&) const {
    return nullptr;
  }
};

// Аналог Go LinkID.
struct LinkID {
  int from;
  int to;
  bool operator==(const LinkID& o) const noexcept = default;
};

struct LinkIDHash {
  size_t operator()(const LinkID& id) const noexcept {
    return std::hash<long long>{}(((long long)id.from << 32) |
                                  (unsigned int)id.to);
  }
};

// Аналог Go RuntimeConfig — индексы поверх IConfig.
// config должен жить не меньше RuntimeConfig (хранится по ссылке, не владеет).
class RuntimeConfig {
 public:
  explicit RuntimeConfig(const IConfig& config) : config_(config) {
    for (const auto& stream : config_.GetStreams()) {
      InsertUnique(streamsByName_, stream.GetName(), stream,
                   "duplicate stream name: " + stream.GetName());
      InsertUnique(streamsByID_, stream.GetID(), stream,
                   "duplicate stream id: " + std::to_string(stream.GetID()));
    }
    for (const auto* svc : config_.GetServices()) {
      InsertUnique(servicesByName_, svc->name, svc,
                   "duplicate service name: " + svc->name);
      InsertUnique(servicesByID_, svc->id, svc,
                   "duplicate service id: " + std::to_string(svc->id));
    }
    for (const auto& ep : config_.GetEndpoints()) {
      InsertUnique(endpointsByName_, ep.GetName(), ep,
                   "duplicate endpoint name: " + ep.GetName());
      InsertUnique(endpointsByID_, ep.GetID(), ep,
                   "duplicate endpoint id: " + std::to_string(ep.GetID()));
    }
    for (const auto& dc : config_.GetDataConnectors()) {
      InsertUnique(dataConnectorsByName_, dc.GetName(), dc,
                   "duplicate data connector name: " + dc.GetName());
      InsertUnique(
          dataConnectorsByID_, dc.GetID(), dc,
          "duplicate data connector id: " + std::to_string(dc.GetID()));
    }
    for (const auto* pool : config_.GetPools()) {
      InsertUnique(poolByName_, pool->name, pool,
                   "duplicate pool name: " + pool->name);
    }
    for (const auto* type : config_.GetTypes()) {
      InsertUnique(typesByName_, type->name, type,
                   "duplicate type name: " + type->name);
    }
    for (const auto* link : config_.GetLinks()) {
      link->Validate();
      InsertUnique(linksByID_, LinkID{link->from, link->to}, link,
                   "duplicate link from=" + std::to_string(link->from) +
                       " to=" + std::to_string(link->to));
    }
  }

  const IConfig& GetConfig() const { return config_; }

  std::optional<StreamConfigRef> GetStreamConfigByName(
      const std::string& name) const {
    auto it = streamsByName_.find(name);
    if (it == streamsByName_.end()) return std::nullopt;
    return it->second;
  }

  std::optional<StreamConfigRef> GetStreamConfigByID(int id) const {
    auto it = streamsByID_.find(id);
    if (it == streamsByID_.end()) return std::nullopt;
    return it->second;
  }

  const ServiceConfig* GetServiceConfigByName(const std::string& name) const {
    auto it = servicesByName_.find(name);
    return it != servicesByName_.end() ? it->second : nullptr;
  }

  const ServiceConfig* GetServiceConfigByID(int id) const {
    auto it = servicesByID_.find(id);
    return it != servicesByID_.end() ? it->second : nullptr;
  }

  const ServiceConfig* GetOnlyServiceConfig() const noexcept {
    return servicesByID_.size() == 1 ? servicesByID_.begin()->second : nullptr;
  }

  std::optional<EndpointConfigRef> GetEndpointConfigByID(int id) const {
    auto it = endpointsByID_.find(id);
    if (it == endpointsByID_.end()) return std::nullopt;
    return it->second;
  }

  std::optional<DataConnectorConfigRef> GetDataConnectorByID(int id) const {
    auto it = dataConnectorsByID_.find(id);
    if (it == dataConnectorsByID_.end()) return std::nullopt;
    return it->second;
  }

  const PoolConfig* GetPoolByName(const std::string& name) const {
    auto it = poolByName_.find(name);
    return it != poolByName_.end() ? it->second : nullptr;
  }

  const TypeConfig* GetTypeByName(const std::string& name) const {
    auto it = typesByName_.find(name);
    return it != typesByName_.end() ? it->second : nullptr;
  }

  const LinkConfig* GetLink(int from, int to) const {
    auto it = linksByID_.find(LinkID{from, to});
    return it != linksByID_.end() ? it->second : nullptr;
  }

 private:
  template <typename Map, typename Key, typename Value>
  static void InsertUnique(Map& map, Key&& key, Value&& value,
                           const std::string& error_msg) {
    if (!map.emplace(std::forward<Key>(key), std::forward<Value>(value))
             .second) {
      throw std::runtime_error(error_msg);
    }
  }

  const IConfig& config_;

  std::unordered_map<std::string, StreamConfigRef> streamsByName_;
  std::unordered_map<int, StreamConfigRef> streamsByID_;
  std::unordered_map<std::string, const ServiceConfig*> servicesByName_;
  std::unordered_map<int, const ServiceConfig*> servicesByID_;
  std::unordered_map<std::string, EndpointConfigRef> endpointsByName_;
  std::unordered_map<int, EndpointConfigRef> endpointsByID_;
  std::unordered_map<std::string, DataConnectorConfigRef> dataConnectorsByName_;
  std::unordered_map<int, DataConnectorConfigRef> dataConnectorsByID_;
  std::unordered_map<std::string, const PoolConfig*> poolByName_;
  std::unordered_map<std::string, const TypeConfig*> typesByName_;
  std::unordered_map<LinkID, const LinkConfig*, LinkIDHash> linksByID_;
};

// One userver process hosts one generated servicelib service. The config
// component publishes immutable snapshots here; runtime environments acquire
// shared ownership before reading them, so hot reload cannot invalidate an
// in-flight lookup.
class RuntimeConfigRegistry final {
 public:
  static void Publish(std::shared_ptr<const RuntimeConfig> config) noexcept {
    current_.store(std::move(config), std::memory_order_release);
  }

  static std::shared_ptr<const RuntimeConfig> Snapshot() noexcept {
    return current_.load(std::memory_order_acquire);
  }

 private:
  inline static std::atomic<std::shared_ptr<const RuntimeConfig>> current_;
};

// Keeps the concrete generated configuration and all RuntimeConfig indices in
// one ownership unit. RuntimeConfig stores references into ConcreteConfig, so
// publishing two unrelated shared_ptr objects would let a concurrent reload
// destroy the old ConcreteConfig while an in-flight request still owns its
// RuntimeConfig. The aliasing result below owns both for exactly the lifetime
// of the acquired runtime snapshot, matching ConfigLoader's canonical userver
// implementation.
namespace detail {

template <typename ConcreteConfig>
std::shared_ptr<const RuntimeConfig> MakeRuntimeConfigSnapshot(
    std::shared_ptr<const ConcreteConfig> config) {
  static_assert(std::is_base_of_v<IConfig, ConcreteConfig>,
                "ConcreteConfig must derive from servicelib::config::IConfig");
  if (!config) {
    throw std::invalid_argument("concrete runtime config is null");
  }

  struct OwnedRuntimeConfig final {
    explicit OwnedRuntimeConfig(std::shared_ptr<const ConcreteConfig> value)
        : config(std::move(value)), runtime(*config) {}

    std::shared_ptr<const ConcreteConfig> config;
    RuntimeConfig runtime;
  };

  auto owned =
      std::make_shared<const OwnedRuntimeConfig>(std::move(config));
  return std::shared_ptr<const RuntimeConfig>(owned, &owned->runtime);
}

}  // namespace detail

}  // namespace servicelib::config

/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the
 * [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */
#pragma once

#include <functional>
#include <string>
#include <utility>
#include <variant>
#include <vector>


#include <servicelib/api/serviceapi.hpp>
#include <servicelib/api/serviceapi_parse.hpp>
#include <servicelib/runtime/config/config_parse_common.hpp>

namespace servicelib::config {

struct InputStreamConfig {
  int id{};
  std::string name;
  std::string pipeline;
  int idService{};
  int idSource{};
  std::vector<int> idSources;
  double xPos{};
  double yPos{};
  std::string valueType;
  int idEndpoint{};
  PropertiesMap properties;

  servicelib::api::TransformationType GetType() const noexcept {
    return servicelib::api::TransformationType::kInput;
  }
  const YamlValue* GetProperty(const std::string& propName) const {
    return detail::FindProperty(properties, propName);
  }
};

struct MapStreamConfig {
  int id{};
  std::string name;
  std::string pipeline;
  int idService{};
  int idSource{};
  double xPos{};
  double yPos{};
  std::string valueType;
  std::string functionPackage;
  std::string functionName;
  bool publicFunction{};
  std::string functionDescription;
  std::string functionInitializerGroup;
  std::string functionModule;
  PropertiesMap properties;

  servicelib::api::TransformationType GetType() const noexcept {
    return servicelib::api::TransformationType::kMap;
  }
  const YamlValue* GetProperty(const std::string& propName) const {
    return detail::FindProperty(properties, propName);
  }
};

struct FilterStreamConfig {
  int id{};
  std::string name;
  std::string pipeline;
  int idService{};
  int idSource{};
  double xPos{};
  double yPos{};
  std::string functionPackage;
  std::string functionName;
  bool publicFunction{};
  std::string functionDescription;
  std::string functionInitializerGroup;
  std::string functionModule;
  PropertiesMap properties;

  servicelib::api::TransformationType GetType() const noexcept {
    return servicelib::api::TransformationType::kFilter;
  }
  const YamlValue* GetProperty(const std::string& propName) const {
    return detail::FindProperty(properties, propName);
  }
};

struct JoinStreamConfig {
  int id{};
  std::string name;
  std::string pipeline;
  int idService{};
  int idSource{};
  std::vector<int> idSources;
  double xPos{};
  double yPos{};
  std::string valueType;
  servicelib::api::JoinType joinType{};
  servicelib::api::JoinStorageType joinStorage{};
  int ttl{};
  bool renewTTL{};
  std::string functionPackage;
  std::string functionName;
  bool publicFunction{};
  std::string functionDescription;
  std::string functionInitializerGroup;
  std::string functionModule;
  PropertiesMap properties;

  servicelib::api::TransformationType GetType() const noexcept {
    return servicelib::api::TransformationType::kJoin;
  }
  const YamlValue* GetProperty(const std::string& propName) const {
    return detail::FindProperty(properties, propName);
  }
};

struct MultiJoinStreamConfig {
  int id{};
  std::string name;
  std::string pipeline;
  int idService{};
  int idSource{};
  std::vector<int> idSources;
  double xPos{};
  double yPos{};
  std::string valueType;
  servicelib::api::JoinStorageType joinStorage{};
  int ttl{};
  bool renewTTL{};
  std::string functionPackage;
  std::string functionName;
  bool publicFunction{};
  std::string functionDescription;
  std::string functionInitializerGroup;
  std::string functionModule;
  PropertiesMap properties;

  servicelib::api::TransformationType GetType() const noexcept {
    return servicelib::api::TransformationType::kMultiJoin;
  }
  const YamlValue* GetProperty(const std::string& propName) const {
    return detail::FindProperty(properties, propName);
  }
};

struct ProcessStreamConfig {
  int id{};
  std::string name;
  std::string pipeline;
  int idService{};
  int idSource{};
  double xPos{};
  double yPos{};
  servicelib::api::ProcessPattern pattern{};
  std::string functionPackage;
  std::string functionName;
  bool publicFunction{};
  std::string functionDescription;
  std::string functionInitializerGroup;
  std::string functionModule;
  PropertiesMap properties;

  servicelib::api::TransformationType GetType() const noexcept {
    return servicelib::api::TransformationType::kProcess;
  }
  const YamlValue* GetProperty(const std::string& propName) const {
    return detail::FindProperty(properties, propName);
  }
};

struct FlatMapStreamConfig {
  int id{};
  std::string name;
  std::string pipeline;
  int idService{};
  int idSource{};
  double xPos{};
  double yPos{};
  std::string valueType;
  std::string functionPackage;
  std::string functionName;
  bool publicFunction{};
  std::string functionDescription;
  std::string functionInitializerGroup;
  std::string functionModule;
  PropertiesMap properties;

  servicelib::api::TransformationType GetType() const noexcept {
    return servicelib::api::TransformationType::kFlatMap;
  }
  const YamlValue* GetProperty(const std::string& propName) const {
    return detail::FindProperty(properties, propName);
  }
};

struct FlatMapIterableStreamConfig {
  int id{};
  std::string name;
  std::string pipeline;
  int idService{};
  int idSource{};
  double xPos{};
  double yPos{};
  std::string valueType;
  PropertiesMap properties;

  servicelib::api::TransformationType GetType() const noexcept {
    return servicelib::api::TransformationType::kFlatMapIterable;
  }
  const YamlValue* GetProperty(const std::string& propName) const {
    return detail::FindProperty(properties, propName);
  }
};

struct KeyByStreamConfig {
  int id{};
  std::string name;
  std::string pipeline;
  int idService{};
  int idSource{};
  double xPos{};
  double yPos{};
  std::string keyType;
  std::string valueType;
  std::string functionPackage;
  std::string functionName;
  bool publicFunction{};
  std::string functionDescription;
  std::string functionInitializerGroup;
  std::string functionModule;
  PropertiesMap properties;

  servicelib::api::TransformationType GetType() const noexcept {
    return servicelib::api::TransformationType::kKeyBy;
  }
  const YamlValue* GetProperty(const std::string& propName) const {
    return detail::FindProperty(properties, propName);
  }
};

struct MergeStreamConfig {
  int id{};
  std::string name;
  std::string pipeline;
  int idService{};
  std::vector<int> idSources;
  double xPos{};
  double yPos{};
  PropertiesMap properties;

  servicelib::api::TransformationType GetType() const noexcept {
    return servicelib::api::TransformationType::kMerge;
  }
  const YamlValue* GetProperty(const std::string& propName) const {
    return detail::FindProperty(properties, propName);
  }
};

struct SplitStreamConfig {
  int id{};
  std::string name;
  std::string pipeline;
  int idService{};
  int idSource{};
  double xPos{};
  double yPos{};
  PropertiesMap properties;

  servicelib::api::TransformationType GetType() const noexcept {
    return servicelib::api::TransformationType::kSplit;
  }
  const YamlValue* GetProperty(const std::string& propName) const {
    return detail::FindProperty(properties, propName);
  }
};

struct CaseStreamConfig {
  int id{};
  std::string name;
  std::string pipeline;
  int idService{};
  int idSource{};
  double xPos{};
  double yPos{};
  std::string functionPackage;
  std::string functionName;
  bool publicFunction{};
  std::string functionDescription;
  std::string functionInitializerGroup;
  std::string functionModule;
  PropertiesMap properties;

  servicelib::api::TransformationType GetType() const noexcept {
    return servicelib::api::TransformationType::kCase;
  }
  const YamlValue* GetProperty(const std::string& propName) const {
    return detail::FindProperty(properties, propName);
  }
};

struct WhenStreamConfig {
  int id{};
  std::string name;
  std::string pipeline;
  int idService{};
  int idSource{};
  double xPos{};
  double yPos{};
  std::string valueType;
  PropertiesMap properties;

  servicelib::api::TransformationType GetType() const noexcept {
    return servicelib::api::TransformationType::kWhen;
  }
  const YamlValue* GetProperty(const std::string& propName) const {
    return detail::FindProperty(properties, propName);
  }
};

struct SinkStreamConfig {
  int id{};
  std::string name;
  std::string pipeline;
  int idService{};
  int idSource{};
  double xPos{};
  double yPos{};
  int idEndpoint{};
  std::string valueType;
  PropertiesMap properties;

  servicelib::api::TransformationType GetType() const noexcept {
    return servicelib::api::TransformationType::kSink;
  }
  const YamlValue* GetProperty(const std::string& propName) const {
    return detail::FindProperty(properties, propName);
  }
};

struct CycleLinkStreamConfig {
  int id{};
  std::string name;
  std::string pipeline;
  int idService{};
  int idSource{};
  double xPos{};
  double yPos{};
  PropertiesMap properties;

  servicelib::api::TransformationType GetType() const noexcept {
    return servicelib::api::TransformationType::kCycleLink;
  }
  const YamlValue* GetProperty(const std::string& propName) const {
    return detail::FindProperty(properties, propName);
  }
};

struct DelayStreamConfig {
  int id{};
  std::string name;
  std::string pipeline;
  int idService{};
  int idSource{};
  double xPos{};
  double yPos{};
  int duration{};
  std::string functionPackage;
  std::string functionName;
  bool publicFunction{};
  std::string functionDescription;
  std::string functionInitializerGroup;
  std::string functionModule;
  PropertiesMap properties;

  servicelib::api::TransformationType GetType() const noexcept {
    return servicelib::api::TransformationType::kDelay;
  }
  const YamlValue* GetProperty(const std::string& propName) const {
    return detail::FindProperty(properties, propName);
  }
};

// StreamConfigRef — аналог Go interface StreamConfig.
// Хранит reference внутрь объекта конфига (не владеет им).
class StreamConfigRef {
 public:
  using Variant =
      std::variant<std::reference_wrapper<const InputStreamConfig>,
                   std::reference_wrapper<const MapStreamConfig>,
                   std::reference_wrapper<const FilterStreamConfig>,
                   std::reference_wrapper<const JoinStreamConfig>,
                   std::reference_wrapper<const MultiJoinStreamConfig>,
                   std::reference_wrapper<const ProcessStreamConfig>,
                   std::reference_wrapper<const FlatMapStreamConfig>,
                   std::reference_wrapper<const FlatMapIterableStreamConfig>,
                   std::reference_wrapper<const KeyByStreamConfig>,
                   std::reference_wrapper<const MergeStreamConfig>,
                   std::reference_wrapper<const SplitStreamConfig>,
                   std::reference_wrapper<const CaseStreamConfig>,
                   std::reference_wrapper<const WhenStreamConfig>,
                   std::reference_wrapper<const SinkStreamConfig>,
                   std::reference_wrapper<const CycleLinkStreamConfig>,
                   std::reference_wrapper<const DelayStreamConfig>>;

 private:
  template <typename F>
  decltype(auto) Visit(F&& f) const {
    return std::visit(
        [&](const auto& ref) -> decltype(auto) {
          return std::forward<F>(f)(ref.get());
        },
        value_);
  }

 public:
  StreamConfigRef(const InputStreamConfig& v) : value_(std::cref(v)) {}
  StreamConfigRef(const MapStreamConfig& v) : value_(std::cref(v)) {}
  StreamConfigRef(const FilterStreamConfig& v) : value_(std::cref(v)) {}
  StreamConfigRef(const JoinStreamConfig& v) : value_(std::cref(v)) {}
  StreamConfigRef(const MultiJoinStreamConfig& v) : value_(std::cref(v)) {}
  StreamConfigRef(const ProcessStreamConfig& v) : value_(std::cref(v)) {}
  StreamConfigRef(const FlatMapStreamConfig& v) : value_(std::cref(v)) {}
  StreamConfigRef(const FlatMapIterableStreamConfig& v)
      : value_(std::cref(v)) {}
  StreamConfigRef(const KeyByStreamConfig& v) : value_(std::cref(v)) {}
  StreamConfigRef(const MergeStreamConfig& v) : value_(std::cref(v)) {}
  StreamConfigRef(const SplitStreamConfig& v) : value_(std::cref(v)) {}
  StreamConfigRef(const CaseStreamConfig& v) : value_(std::cref(v)) {}
  StreamConfigRef(const WhenStreamConfig& v) : value_(std::cref(v)) {}
  StreamConfigRef(const SinkStreamConfig& v) : value_(std::cref(v)) {}
  StreamConfigRef(const CycleLinkStreamConfig& v) : value_(std::cref(v)) {}
  StreamConfigRef(const DelayStreamConfig& v) : value_(std::cref(v)) {}

  int GetID() const {
    return Visit([](const auto& c) { return c.id; });
  }

  const std::string& GetName() const {
    return Visit([](const auto& c) -> const std::string& { return c.name; });
  }

  const std::string& GetPipeline() const {
    return Visit(
        [](const auto& c) -> const std::string& { return c.pipeline; });
  }

  servicelib::api::TransformationType GetType() const {
    return Visit([](const auto& c) { return c.GetType(); });
  }

  int GetIdService() const {
    return Visit([](const auto& c) { return c.idService; });
  }

  int GetIdSource() const {
    return Visit([](const auto& c) -> int {
      if constexpr (requires { c.idSource; }) {
        return c.idSource;
      } else {
        return 0;
      }
    });
  }

  const std::vector<int>& GetIdSources() const {
    static const std::vector<int> kEmpty;
    return Visit([](const auto& c) -> const std::vector<int>& {
      if constexpr (requires { c.idSources; }) {
        return c.idSources;
      } else {
        return kEmpty;
      }
    });
  }

  double GetXPos() const {
    return Visit([](const auto& c) { return c.xPos; });
  }

  double GetYPos() const {
    return Visit([](const auto& c) { return c.yPos; });
  }

  // Аналог Go type assertion: v.(*InputStreamConfig)
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

inline InputStreamConfig Parse(const YamlValue& value,
                               TypeTag<InputStreamConfig>) {
  InputStreamConfig result;
  result.id = value["id"].As<int>(0);
  result.name = value["name"].As<std::string>("");
  result.pipeline = value["pipeline"].As<std::string>("");
  result.idService = value["idService"].As<int>(0);
  result.idSource = value["idSource"].As<int>(0);
  result.idSources =
      value["idSources"].As<std::vector<int>>(std::vector<int>{});
  result.xPos = value["xPos"].As<double>(0.0);
  result.yPos = value["yPos"].As<double>(0.0);
  result.valueType = value["valueType"].As<std::string>("");
  result.idEndpoint = value["idEndpoint"].As<int>(0);
  detail::ParseRemainingProperties(
      value,
      {"id", "name", "pipeline", "idService", "idSource", "idSources", "xPos", "yPos",
       "valueType", "idEndpoint"},
      result.properties);
  return result;
}

inline MapStreamConfig Parse(const YamlValue& value,
                             TypeTag<MapStreamConfig>) {
  MapStreamConfig result;
  result.id = value["id"].As<int>(0);
  result.name = value["name"].As<std::string>("");
  result.pipeline = value["pipeline"].As<std::string>("");
  result.idService = value["idService"].As<int>(0);
  result.idSource = value["idSource"].As<int>(0);
  result.xPos = value["xPos"].As<double>(0.0);
  result.yPos = value["yPos"].As<double>(0.0);
  result.valueType = value["valueType"].As<std::string>("");
  detail::ParseFunctionFields(value, result);
  detail::ParseRemainingProperties(
      value,
      {"id", "name", "pipeline", "idService", "idSource", "xPos", "yPos", "valueType",
       "functionPackage", "functionName", "publicFunction", "functionDescription",
       "functionInitializerGroup", "functionModule"},
      result.properties);
  return result;
}

inline FilterStreamConfig Parse(
    const YamlValue& value,
    TypeTag<FilterStreamConfig>) {
  FilterStreamConfig result;
  result.id = value["id"].As<int>(0);
  result.name = value["name"].As<std::string>("");
  result.pipeline = value["pipeline"].As<std::string>("");
  result.idService = value["idService"].As<int>(0);
  result.idSource = value["idSource"].As<int>(0);
  result.xPos = value["xPos"].As<double>(0.0);
  result.yPos = value["yPos"].As<double>(0.0);
  detail::ParseFunctionFields(value, result);
  detail::ParseRemainingProperties(
      value,
      {"id", "name", "pipeline", "idService", "idSource", "xPos", "yPos", "functionPackage",
       "functionName", "publicFunction", "functionDescription", "functionInitializerGroup",
       "functionModule"},
      result.properties);
  return result;
}

inline JoinStreamConfig Parse(const YamlValue& value,
                              TypeTag<JoinStreamConfig>) {
  JoinStreamConfig result;
  result.id = value["id"].As<int>(0);
  result.name = value["name"].As<std::string>("");
  result.pipeline = value["pipeline"].As<std::string>("");
  result.idService = value["idService"].As<int>(0);
  result.idSource = value["idSource"].As<int>(0);
  result.idSources =
      value["idSources"].As<std::vector<int>>(std::vector<int>{});
  result.xPos = value["xPos"].As<double>(0.0);
  result.yPos = value["yPos"].As<double>(0.0);
  result.valueType = value["valueType"].As<std::string>("");
  result.joinType = value["joinType"].As<servicelib::api::JoinType>(
      servicelib::api::JoinType::kUndefined);
  result.joinStorage = value["joinStorage"].As<servicelib::api::JoinStorageType>(
      servicelib::api::JoinStorageType::kUndefined);
  result.ttl = value["ttl"].As<int>(0);
  result.renewTTL = value["renewTTL"].As<bool>(false);
  detail::ParseFunctionFields(value, result);
  detail::ParseRemainingProperties(
      value,
      {"id", "name", "pipeline", "idService", "idSource", "idSources", "xPos", "yPos",
       "valueType", "joinType", "joinStorage", "ttl", "renewTTL", "functionPackage",
       "functionName", "publicFunction", "functionDescription", "functionInitializerGroup",
       "functionModule"},
      result.properties);
  return result;
}

inline MultiJoinStreamConfig Parse(
    const YamlValue& value,
    TypeTag<MultiJoinStreamConfig>) {
  MultiJoinStreamConfig result;
  result.id = value["id"].As<int>(0);
  result.name = value["name"].As<std::string>("");
  result.pipeline = value["pipeline"].As<std::string>("");
  result.idService = value["idService"].As<int>(0);
  result.idSource = value["idSource"].As<int>(0);
  result.idSources =
      value["idSources"].As<std::vector<int>>(std::vector<int>{});
  result.xPos = value["xPos"].As<double>(0.0);
  result.yPos = value["yPos"].As<double>(0.0);
  result.valueType = value["valueType"].As<std::string>("");
  result.joinStorage = value["joinStorage"].As<servicelib::api::JoinStorageType>(
      servicelib::api::JoinStorageType::kUndefined);
  result.ttl = value["ttl"].As<int>(0);
  result.renewTTL = value["renewTTL"].As<bool>(false);
  detail::ParseFunctionFields(value, result);
  detail::ParseRemainingProperties(
      value,
      {"id", "name", "pipeline", "idService", "idSource", "idSources", "xPos", "yPos",
       "valueType", "joinStorage", "ttl", "renewTTL", "functionPackage", "functionName",
       "publicFunction", "functionDescription", "functionInitializerGroup", "functionModule"},
      result.properties);
  return result;
}

inline ProcessStreamConfig Parse(
    const YamlValue& value,
    TypeTag<ProcessStreamConfig>) {
  ProcessStreamConfig result;
  result.id = value["id"].As<int>(0);
  result.name = value["name"].As<std::string>("");
  result.pipeline = value["pipeline"].As<std::string>("");
  result.idService = value["idService"].As<int>(0);
  result.idSource = value["idSource"].As<int>(0);
  result.xPos = value["xPos"].As<double>(0.0);
  result.yPos = value["yPos"].As<double>(0.0);
  result.pattern = value["pattern"].As<servicelib::api::ProcessPattern>(
      servicelib::api::ProcessPattern::kUndefined);
  detail::ParseFunctionFields(value, result);
  detail::ParseRemainingProperties(
      value,
      {"id", "name", "pipeline", "idService", "idSource", "xPos", "yPos", "pattern",
       "functionPackage", "functionName", "publicFunction", "functionDescription",
       "functionInitializerGroup", "functionModule"},
      result.properties);
  return result;
}

inline FlatMapStreamConfig Parse(
    const YamlValue& value,
    TypeTag<FlatMapStreamConfig>) {
  FlatMapStreamConfig result;
  result.id = value["id"].As<int>(0);
  result.name = value["name"].As<std::string>("");
  result.pipeline = value["pipeline"].As<std::string>("");
  result.idService = value["idService"].As<int>(0);
  result.idSource = value["idSource"].As<int>(0);
  result.xPos = value["xPos"].As<double>(0.0);
  result.yPos = value["yPos"].As<double>(0.0);
  result.valueType = value["valueType"].As<std::string>("");
  detail::ParseFunctionFields(value, result);
  detail::ParseRemainingProperties(
      value,
      {"id", "name", "pipeline", "idService", "idSource", "xPos", "yPos", "valueType",
       "functionPackage", "functionName", "publicFunction", "functionDescription",
       "functionInitializerGroup", "functionModule"},
      result.properties);
  return result;
}

inline FlatMapIterableStreamConfig Parse(
    const YamlValue& value,
    TypeTag<FlatMapIterableStreamConfig>) {
  FlatMapIterableStreamConfig result;
  result.id = value["id"].As<int>(0);
  result.name = value["name"].As<std::string>("");
  result.pipeline = value["pipeline"].As<std::string>("");
  result.idService = value["idService"].As<int>(0);
  result.idSource = value["idSource"].As<int>(0);
  result.xPos = value["xPos"].As<double>(0.0);
  result.yPos = value["yPos"].As<double>(0.0);
  result.valueType = value["valueType"].As<std::string>("");
  detail::ParseRemainingProperties(
      value, {"id", "name", "pipeline", "idService", "idSource", "xPos", "yPos", "valueType"},
      result.properties);
  return result;
}

inline KeyByStreamConfig Parse(const YamlValue& value,
                               TypeTag<KeyByStreamConfig>) {
  KeyByStreamConfig result;
  result.id = value["id"].As<int>(0);
  result.name = value["name"].As<std::string>("");
  result.pipeline = value["pipeline"].As<std::string>("");
  result.idService = value["idService"].As<int>(0);
  result.idSource = value["idSource"].As<int>(0);
  result.xPos = value["xPos"].As<double>(0.0);
  result.yPos = value["yPos"].As<double>(0.0);
  result.keyType = value["keyType"].As<std::string>("");
  result.valueType = value["valueType"].As<std::string>("");
  detail::ParseFunctionFields(value, result);
  detail::ParseRemainingProperties(
      value,
      {"id", "name", "pipeline", "idService", "idSource", "xPos", "yPos", "keyType",
       "valueType", "functionPackage", "functionName", "publicFunction",
       "functionDescription", "functionInitializerGroup", "functionModule"},
      result.properties);
  return result;
}

inline MergeStreamConfig Parse(const YamlValue& value,
                               TypeTag<MergeStreamConfig>) {
  MergeStreamConfig result;
  result.id = value["id"].As<int>(0);
  result.name = value["name"].As<std::string>("");
  result.pipeline = value["pipeline"].As<std::string>("");
  result.idService = value["idService"].As<int>(0);
  result.idSources =
      value["idSources"].As<std::vector<int>>(std::vector<int>{});
  result.xPos = value["xPos"].As<double>(0.0);
  result.yPos = value["yPos"].As<double>(0.0);
  detail::ParseRemainingProperties(
      value, {"id", "name", "pipeline", "idService", "idSources", "xPos", "yPos"},
      result.properties);
  return result;
}

inline SplitStreamConfig Parse(const YamlValue& value,
                               TypeTag<SplitStreamConfig>) {
  SplitStreamConfig result;
  result.id = value["id"].As<int>(0);
  result.name = value["name"].As<std::string>("");
  result.pipeline = value["pipeline"].As<std::string>("");
  result.idService = value["idService"].As<int>(0);
  result.idSource = value["idSource"].As<int>(0);
  result.xPos = value["xPos"].As<double>(0.0);
  result.yPos = value["yPos"].As<double>(0.0);
  detail::ParseRemainingProperties(
      value, {"id", "name", "pipeline", "idService", "idSource", "xPos", "yPos"},
      result.properties);
  return result;
}

inline CaseStreamConfig Parse(const YamlValue& value,
                              TypeTag<CaseStreamConfig>) {
  CaseStreamConfig result;
  result.id = value["id"].As<int>(0);
  result.name = value["name"].As<std::string>("");
  result.pipeline = value["pipeline"].As<std::string>("");
  result.idService = value["idService"].As<int>(0);
  result.idSource = value["idSource"].As<int>(0);
  result.xPos = value["xPos"].As<double>(0.0);
  result.yPos = value["yPos"].As<double>(0.0);
  detail::ParseFunctionFields(value, result);
  detail::ParseRemainingProperties(
      value,
      {"id", "name", "pipeline", "idService", "idSource", "xPos", "yPos", "functionPackage",
       "functionName", "publicFunction", "functionDescription", "functionInitializerGroup",
       "functionModule"},
      result.properties);
  return result;
}

inline WhenStreamConfig Parse(const YamlValue& value,
                              TypeTag<WhenStreamConfig>) {
  WhenStreamConfig result;
  result.id = value["id"].As<int>(0);
  result.name = value["name"].As<std::string>("");
  result.pipeline = value["pipeline"].As<std::string>("");
  result.idService = value["idService"].As<int>(0);
  result.idSource = value["idSource"].As<int>(0);
  result.xPos = value["xPos"].As<double>(0.0);
  result.yPos = value["yPos"].As<double>(0.0);
  result.valueType = value["valueType"].As<std::string>("");
  detail::ParseRemainingProperties(
      value, {"id", "name", "pipeline", "idService", "idSource", "xPos", "yPos", "valueType"},
      result.properties);
  return result;
}

inline SinkStreamConfig Parse(const YamlValue& value,
                              TypeTag<SinkStreamConfig>) {
  SinkStreamConfig result;
  result.id = value["id"].As<int>(0);
  result.name = value["name"].As<std::string>("");
  result.pipeline = value["pipeline"].As<std::string>("");
  result.idService = value["idService"].As<int>(0);
  result.idSource = value["idSource"].As<int>(0);
  result.xPos = value["xPos"].As<double>(0.0);
  result.yPos = value["yPos"].As<double>(0.0);
  result.idEndpoint = value["idEndpoint"].As<int>(0);
  result.valueType = value["valueType"].As<std::string>("");
  detail::ParseRemainingProperties(
      value,
      {"id", "name", "pipeline", "idService", "idSource", "xPos", "yPos", "idEndpoint",
       "valueType"},
      result.properties);
  return result;
}

inline CycleLinkStreamConfig Parse(
    const YamlValue& value,
    TypeTag<CycleLinkStreamConfig>) {
  CycleLinkStreamConfig result;
  result.id = value["id"].As<int>(0);
  result.name = value["name"].As<std::string>("");
  result.pipeline = value["pipeline"].As<std::string>("");
  result.idService = value["idService"].As<int>(0);
  result.idSource = value["idSource"].As<int>(0);
  result.xPos = value["xPos"].As<double>(0.0);
  result.yPos = value["yPos"].As<double>(0.0);
  detail::ParseRemainingProperties(
      value, {"id", "name", "pipeline", "idService", "idSource", "xPos", "yPos"},
      result.properties);
  return result;
}

inline DelayStreamConfig Parse(const YamlValue& value,
                               TypeTag<DelayStreamConfig>) {
  DelayStreamConfig result;
  result.id = value["id"].As<int>(0);
  result.name = value["name"].As<std::string>("");
  result.pipeline = value["pipeline"].As<std::string>("");
  result.idService = value["idService"].As<int>(0);
  result.idSource = value["idSource"].As<int>(0);
  result.xPos = value["xPos"].As<double>(0.0);
  result.yPos = value["yPos"].As<double>(0.0);
  result.duration = value["duration"].As<int>(0);
  detail::ParseFunctionFields(value, result);
  detail::ParseRemainingProperties(
      value,
      {"id", "name", "pipeline", "idService", "idSource", "xPos", "yPos", "duration",
       "functionPackage", "functionName", "publicFunction", "functionDescription",
       "functionInitializerGroup", "functionModule"},
      result.properties);
  return result;
}

}  // namespace servicelib::config

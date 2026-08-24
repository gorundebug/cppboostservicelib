/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the
 * [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */
#pragma once

#include <optional>
#include <stdexcept>
#include <string>
#include <utility>


#include <servicelib/api/serviceapi.hpp>
#include <servicelib/runtime/config/config_parse_common.hpp>

namespace servicelib::config {

struct FunctionCallSemanticsConfig {
  bool async{};

  servicelib::api::CallSemantics GetType() const noexcept {
    return servicelib::api::CallSemantics::kFunctionCall;
  }
};

struct TaskPoolCallSemanticsConfig {
  std::string poolName;

  servicelib::api::CallSemantics GetType() const noexcept {
    return servicelib::api::CallSemantics::kTaskPool;
  }
};

struct PriorityTaskPoolCallSemanticsConfig {
  std::string poolName;
  int priority{};

  servicelib::api::CallSemantics GetType() const noexcept {
    return servicelib::api::CallSemantics::kPriorityTaskPool;
  }
};

struct ParallelCallSemanticsConfig {
  servicelib::api::CallSemantics GetType() const noexcept {
    return servicelib::api::CallSemantics::kParallelCall;
  }
};

struct DurableCallSemanticsConfig {
  int idDataConnector{};
  std::string taskQueue;
  int workflowExecutionTimeout{};
  int activityStartToCloseTimeout{};
  int activityHeartbeatTimeout{};
  int maximumAttempts{};

  servicelib::api::CallSemantics GetType() const noexcept {
    return servicelib::api::CallSemantics::kDurableCall;
  }
};

// Аналог Go CallSemanticsGroup — только одно поле должно быть задано.
struct CallSemanticsGroup {
  std::optional<FunctionCallSemanticsConfig> functionCall;
  std::optional<TaskPoolCallSemanticsConfig> taskPool;
  std::optional<PriorityTaskPoolCallSemanticsConfig> priorityTaskPool;
  std::optional<ParallelCallSemanticsConfig> parallelCall;
  std::optional<DurableCallSemanticsConfig> durableCall;

  void Validate() const {
    int count = (functionCall.has_value() ? 1 : 0) +
                (taskPool.has_value() ? 1 : 0) +
                (priorityTaskPool.has_value() ? 1 : 0) +
                (parallelCall.has_value() ? 1 : 0) +
                (durableCall.has_value() ? 1 : 0);
    if (count != 1) {
      throw std::runtime_error(
          "exactly one call semantics must be specified, got " +
          std::to_string(count));
    }
  }
};

struct LinkConfig {
  int from{};
  int to{};
  std::optional<CallSemanticsGroup> callSemantics;
  PropertiesMap properties;

  const YamlValue* GetProperty(
      const std::string& propName) const {
    return detail::FindProperty(properties, propName);
  }

  void Validate() const {
    if (!callSemantics.has_value()) return;
    try {
      callSemantics->Validate();
    } catch (const std::exception& e) {
      throw std::runtime_error("link from=" + std::to_string(from) +
                               " to=" + std::to_string(to) +
                               " has invalid configuration: " + e.what());
    }
  }
};

inline CallSemanticsGroup MakeCallSemanticsGroup(
    servicelib::api::CallSemantics type, std::string poolName = {},
    int priority = 0, bool async = false, int idDataConnector = 0) {
  CallSemanticsGroup result;
  switch (type) {
    case servicelib::api::CallSemantics::kFunctionCall:
      result.functionCall = FunctionCallSemanticsConfig{async};
      break;
    case servicelib::api::CallSemantics::kTaskPool:
      result.taskPool = TaskPoolCallSemanticsConfig{std::move(poolName)};
      break;
    case servicelib::api::CallSemantics::kPriorityTaskPool:
      result.priorityTaskPool =
          PriorityTaskPoolCallSemanticsConfig{std::move(poolName), priority};
      break;
    case servicelib::api::CallSemantics::kParallelCall:
      result.parallelCall.emplace();
      break;
    case servicelib::api::CallSemantics::kDurableCall:
      result.durableCall.emplace();
      result.durableCall->idDataConnector = idDataConnector;
      break;
    case servicelib::api::CallSemantics::kUndefined:
    case servicelib::api::CallSemantics::kInherited:
      throw std::runtime_error(
          "undefined or inherited semantics has no concrete call-semantics "
          "group");
  }
  return result;
}

// Each of CallSemanticsGroup's fields is its own YAML key, at most one of
// which should be present (RuntimeConfig's construction calls
// LinkConfig::Validate(), which enforces exactly one — Parse itself stays
// permissive so the "0 or >1 present" error message can name the
// offending link, not just "a callSemantics object").
inline FunctionCallSemanticsConfig Parse(
    const YamlValue& value,
    TypeTag<FunctionCallSemanticsConfig>) {
  return FunctionCallSemanticsConfig{value["async"].As<bool>(false)};
}

inline TaskPoolCallSemanticsConfig Parse(
    const YamlValue& value,
    TypeTag<TaskPoolCallSemanticsConfig>) {
  TaskPoolCallSemanticsConfig result;
  result.poolName = value["poolName"].As<std::string>("");
  return result;
}

inline PriorityTaskPoolCallSemanticsConfig Parse(
    const YamlValue& value,
    TypeTag<PriorityTaskPoolCallSemanticsConfig>) {
  PriorityTaskPoolCallSemanticsConfig result;
  result.poolName = value["poolName"].As<std::string>("");
  result.priority = value["priority"].As<int>(0);
  return result;
}

inline ParallelCallSemanticsConfig Parse(
    const YamlValue&,
    TypeTag<ParallelCallSemanticsConfig>) {
  return ParallelCallSemanticsConfig{};
}

inline DurableCallSemanticsConfig Parse(
    const YamlValue& value,
    TypeTag<DurableCallSemanticsConfig>) {
  DurableCallSemanticsConfig result;
  result.idDataConnector = value["idDataConnector"].As<int>(0);
  result.taskQueue = value["taskQueue"].As<std::string>("");
  result.workflowExecutionTimeout = value["workflowExecutionTimeout"].As<int>(0);
  result.activityStartToCloseTimeout =
      value["activityStartToCloseTimeout"].As<int>(0);
  result.activityHeartbeatTimeout =
      value["activityHeartbeatTimeout"].As<int>(0);
  result.maximumAttempts = value["maximumAttempts"].As<int>(0);
  return result;
}

inline CallSemanticsGroup Parse(
    const YamlValue& value,
    TypeTag<CallSemanticsGroup>) {
  if (value.IsString()) {
    return MakeCallSemanticsGroup(value.As<servicelib::api::CallSemantics>());
  }
  CallSemanticsGroup result;
  auto function = value["functionCall"];
  if (function.IsMissing()) {
    function = value["function"];
  }
  if (!function.IsMissing()) {
    result.functionCall = function.As<FunctionCallSemanticsConfig>();
  }
  if (const auto v = value["taskPool"]; !v.IsMissing()) {
    result.taskPool = v.As<TaskPoolCallSemanticsConfig>();
  }
  if (const auto v = value["priorityTaskPool"]; !v.IsMissing()) {
    result.priorityTaskPool = v.As<PriorityTaskPoolCallSemanticsConfig>();
  }
  if (const auto v = value["parallelCall"]; !v.IsMissing()) {
    result.parallelCall = v.As<ParallelCallSemanticsConfig>();
  }
  if (const auto v = value["durableCall"]; !v.IsMissing()) {
    result.durableCall = v.As<DurableCallSemanticsConfig>();
  }
  return result;
}

inline LinkConfig Parse(const YamlValue& value,
                        TypeTag<LinkConfig>) {
  LinkConfig result;
  result.from = value["from"].As<int>(0);
  result.to = value["to"].As<int>(0);
  if (const auto v = value["callSemantics"]; !v.IsMissing()) {
    if (v.IsString()) {
      const auto type = v.As<servicelib::api::CallSemantics>();
      if (type != servicelib::api::CallSemantics::kUndefined &&
          type != servicelib::api::CallSemantics::kInherited) {
        result.callSemantics =
            MakeCallSemanticsGroup(type, value["poolName"].As<std::string>(""),
                                   value["priority"].As<int>(0),
                                   value["async"].As<bool>(false),
                                   value["idDataConnector"].As<int>(0));
      }
    } else {
      result.callSemantics = v.As<CallSemanticsGroup>();
    }
  }
  detail::ParseRemainingProperties(
      value,
      {"from", "to", "callSemantics", "poolName", "priority", "async", "idDataConnector"},
      result.properties);
  return result;
}

}  // namespace servicelib::config

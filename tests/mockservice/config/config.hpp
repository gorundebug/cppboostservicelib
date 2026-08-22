/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the
 * [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */
#pragma once

#include <memory>
#include <vector>

#include <servicelib/runtime/config/config.hpp>

namespace mockservice::config {

// Service IDs
constexpr int kIncomeServiceId = 1;

// Stream IDs
constexpr int kInputRequestId = 1;
constexpr int kSinkId = 2;
constexpr int kInputRequestId3 = 3;
constexpr int kSinkId4 = 4;
constexpr int kInputRequestSyncId = 5;
constexpr int kSinkSyncId = 6;

// Data connector IDs
constexpr int kHttpServerConnId = 1;
constexpr int kCustomSinkConnId = 2;

// Endpoint IDs
constexpr int kDataEndpointId = 1;
constexpr int kDataEndpoint3Id = 2;
constexpr int kDataEndpointSyncId = 3;
constexpr int kSinkEndpointId = 4;
constexpr int kSink4EndpointId = 5;
constexpr int kSinkSyncEndpointId = 6;

// Pool names
inline constexpr const char* kDefaultTaskPoolName = "TaskPool";
inline constexpr const char* kDefaultPriorityPoolName = "PriorityTaskPool";

// Аналог Go Config struct — конкретная реализация IConfig.
class Config : public servicelib::config::IConfig {
 public:
  struct {
    servicelib::config::ServiceConfig incomeService;
  } services;

  struct {
    servicelib::config::InputStreamConfig inputRequest;
    servicelib::config::SinkStreamConfig sink;
    servicelib::config::InputStreamConfig inputRequest3;
    servicelib::config::SinkStreamConfig sink4;
    servicelib::config::InputStreamConfig inputRequestSync;
    servicelib::config::SinkStreamConfig sinkSync;
  } streams;

  struct {
    servicelib::config::HttpDataConnectorConfig httpServer;
    servicelib::config::CustomDataConnectorConfig customSinkConnector;
  } dataConnectors;

  struct {
    servicelib::config::HttpEndpointConfig data;
    servicelib::config::HttpEndpointConfig data3;
    servicelib::config::HttpEndpointConfig dataSync;
    servicelib::config::CustomEndpointConfig sink;
    servicelib::config::CustomEndpointConfig sink4;
    servicelib::config::CustomEndpointConfig sinkSync;
  } endpoints;

  struct {
    servicelib::config::PoolConfig taskPool;
    servicelib::config::PoolConfig priorityTaskPool;
  } pools;

  struct {
    servicelib::config::LinkConfig inputRequestToSink;
    servicelib::config::LinkConfig inputRequest3ToSink4;
    servicelib::config::LinkConfig inputRequestSyncToSinkSync;
  } links;

  struct {
    servicelib::config::ModuleConfig model;
  } modules;

  std::vector<const servicelib::config::ServiceConfig*> GetServices()
      const override {
    return {&services.incomeService};
  }

  std::vector<servicelib::config::StreamConfigRef> GetStreams() const override {
    return {
        streams.inputRequest,     streams.sink,
        streams.inputRequest3,    streams.sink4,
        streams.inputRequestSync, streams.sinkSync,
    };
  }

  std::vector<servicelib::config::DataConnectorConfigRef> GetDataConnectors()
      const override {
    return {
        dataConnectors.httpServer,
        dataConnectors.customSinkConnector,
    };
  }

  std::vector<servicelib::config::EndpointConfigRef> GetEndpoints() const override {
    return {
        endpoints.data, endpoints.data3, endpoints.dataSync,
        endpoints.sink, endpoints.sink4, endpoints.sinkSync,
    };
  }

  std::vector<const servicelib::config::PoolConfig*> GetPools() const override {
    return {&pools.taskPool, &pools.priorityTaskPool};
  }

  std::vector<const servicelib::config::LinkConfig*> GetLinks() const override {
    return {
        &links.inputRequestToSink,
        &links.inputRequest3ToSink4,
        &links.inputRequestSyncToSinkSync,
    };
  }

  std::vector<const servicelib::config::ModuleConfig*> GetModules() const override {
    return {&modules.model};
  }

  std::vector<const servicelib::config::TypeConfig*> GetTypes() const override {
    return {};
  }
};

// Аналог Go MakeConfig() — инициализирует конфиг значениями по умолчанию.
inline Config MakeConfig() {
  using namespace servicelib::config;
  using namespace servicelib::api;

  Config cfg;

  cfg.services.incomeService = ServiceConfig{
      .id = kIncomeServiceId,
      .name = "IncomeService",
      .color = "#D2E5FF",
      .grpcHost = "localhost",
      .grpcPort = 9201,
      .httpHost = "localhost",
      .httpPort = 9091,
      .metricsHandler = "metrics",
      .shutdownTimeout = 30000,
      .statusHandler = "status",
  };

  cfg.streams.inputRequest = InputStreamConfig{
      .id = kInputRequestId,
      .name = "InputRequest",
      .idService = kIncomeServiceId,
      .xPos = -1,
      .yPos = 36,
      .valueType = "RequestData",
      .idEndpoint = kDataEndpointId,
  };

  cfg.streams.sink = SinkStreamConfig{
      .id = kSinkId,
      .name = "sink",
      .idService = kIncomeServiceId,
      .idSource = kInputRequestId,
      .xPos = -1285,
      .yPos = -62,
      .idEndpoint = kSinkEndpointId,
      .valueType = "error",
  };

  cfg.streams.inputRequest3 = InputStreamConfig{
      .id = kInputRequestId3,
      .name = "InputRequest3",
      .idService = kIncomeServiceId,
      .xPos = -1,
      .yPos = 36,
      .valueType = "RequestData",
      .idEndpoint = kDataEndpoint3Id,
  };

  cfg.streams.sink4 = SinkStreamConfig{
      .id = kSinkId4,
      .name = "Sink4",
      .idService = kIncomeServiceId,
      .idSource = kInputRequestId3,
      .xPos = -1285,
      .yPos = -62,
      .idEndpoint = kSink4EndpointId,
  };

  cfg.streams.inputRequestSync = InputStreamConfig{
      .id = kInputRequestSyncId,
      .name = "InputRequestSync",
      .idService = kIncomeServiceId,
      .xPos = -1,
      .yPos = 36,
      .valueType = "RequestData",
      .idEndpoint = kDataEndpointSyncId,
  };

  cfg.streams.sinkSync = SinkStreamConfig{
      .id = kSinkSyncId,
      .name = "SinkSync",
      .idService = kIncomeServiceId,
      .idSource = kInputRequestSyncId,
      .xPos = -1285,
      .yPos = -62,
      .idEndpoint = kSinkSyncEndpointId,
  };

  cfg.dataConnectors.httpServer = HttpDataConnectorConfig{
      .id = kHttpServerConnId,
      .name = "HttpServer",
      .implementation = DataConnectorImplementation::kUserverHTTP,
      .host = "localhost",
      .port = 8080,
      .useDedicatedListener = false,
  };

  cfg.dataConnectors.customSinkConnector = CustomDataConnectorConfig{
      .id = kCustomSinkConnId,
      .name = "CustomSink",
      .implementation = DataConnectorImplementation::kFunction,
  };

  cfg.endpoints.data = HttpEndpointConfig{
      .id = kDataEndpointId,
      .name = "Data",
      .idDataConnector = kHttpServerConnId,
      .httpMethodType = HTTPMethodType::kPOST,
      .path = "/data",
  };

  cfg.endpoints.data3 = HttpEndpointConfig{
      .id = kDataEndpoint3Id,
      .name = "Data3",
      .idDataConnector = kHttpServerConnId,
      .httpMethodType = HTTPMethodType::kPOST,
      .path = "/data3",
  };

  cfg.endpoints.dataSync = HttpEndpointConfig{
      .id = kDataEndpointSyncId,
      .name = "DataSync",
      .idDataConnector = kHttpServerConnId,
      .httpMethodType = HTTPMethodType::kPOST,
      .path = "/dataSync",
  };

  cfg.endpoints.sink = CustomEndpointConfig{
      .id = kSinkEndpointId,
      .name = "Sink",
      .idDataConnector = kCustomSinkConnId,
      .functionName = "SinkFunction",
  };

  cfg.endpoints.sink4 = CustomEndpointConfig{
      .id = kSink4EndpointId,
      .name = "Sink4",
      .idDataConnector = kCustomSinkConnId,
      .functionName = "SinkFunction",
  };

  cfg.endpoints.sinkSync = CustomEndpointConfig{
      .id = kSinkSyncEndpointId,
      .name = "SinkSync",
      .idDataConnector = kCustomSinkConnId,
      .functionName = "SinkFunction",
  };

  cfg.pools.taskPool = PoolConfig{
      .name = kDefaultTaskPoolName,
      .executorsCount = 8,
  };

  cfg.pools.priorityTaskPool = PoolConfig{
      .name = kDefaultPriorityPoolName,
      .executorsCount = 8,
  };

  cfg.links.inputRequestToSink = LinkConfig{
      .from = kInputRequestId,
      .to = kSinkId,
      .callSemantics =
          CallSemanticsGroup{
              .priorityTaskPool =
                  PriorityTaskPoolCallSemanticsConfig{
                      .poolName = kDefaultPriorityPoolName,
                      .priority = 0,
                  },
          },
  };

  cfg.links.inputRequest3ToSink4 = LinkConfig{
      .from = kInputRequestId3,
      .to = kSinkId4,
      .callSemantics =
          CallSemanticsGroup{
              .taskPool =
                  TaskPoolCallSemanticsConfig{
                      .poolName = kDefaultTaskPoolName,
                  },
          },
  };

  cfg.links.inputRequestSyncToSinkSync = LinkConfig{
      .from = kInputRequestSyncId,
      .to = kSinkSyncId,
      .callSemantics =
          CallSemanticsGroup{
              .functionCall = FunctionCallSemanticsConfig{},
          },
  };

  cfg.modules.model = ModuleConfig{
      .name = "Model",
      .path = "service1.com/model",
  };

  return cfg;
}


}  // namespace mockservice::config

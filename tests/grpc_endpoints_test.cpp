#include <atomic>
#include <exception>
#include <memory>
#include <string>
#include <utility>
#include <vector>


#include <servicelib/datasink/grpc/asio.hpp>
#include <servicelib/datasource/grpc/asio.hpp>
#include <servicelib/runtime/detail/sync.hpp>
#include <gtest/gtest.h>
#include <servicelib/runtime/environment/environment.hpp>
#include <servicelib/runtime/testlog/testlog.hpp>
#include <servicelib/runtime/testmetrics/testmetrics.hpp>
#include "test_async.hpp"
#include "test_sink_endpoint_stream.hpp"

namespace {

test_async::AsioRuntime asioRuntime;

class TestConfig final : public servicelib::config::IConfig {
 public:
  TestConfig() {
    connector.id = 10;
    connector.name = "grpc";
    connector.address = "localhost:9201";
    const std::vector<servicelib::api::GrpcMethodType> methods{
        servicelib::api::GrpcMethodType::kNoStreaming,
        servicelib::api::GrpcMethodType::kServerStreaming,
        servicelib::api::GrpcMethodType::kClientStreaming,
        servicelib::api::GrpcMethodType::kBidirectionalStreaming};
    for (std::size_t i = 0; i < methods.size(); ++i) {
      endpoints[i].id = static_cast<int>(i + 1);
      endpoints[i].name = "grpc-" + std::to_string(i + 1);
      endpoints[i].idDataConnector = connector.id;
      endpoints[i].grpcMethodType = methods[i];
      endpoints[i].methodName = endpoints[i].name;
    }
  }

  std::vector<const servicelib::config::ServiceConfig*> GetServices()
      const override {
    return {};
  }
  std::vector<servicelib::config::StreamConfigRef> GetStreams() const override {
    return {};
  }
  std::vector<servicelib::config::DataConnectorConfigRef> GetDataConnectors()
      const override {
    return {connector};
  }
  std::vector<servicelib::config::EndpointConfigRef> GetEndpoints()
      const override {
    return {endpoints[0], endpoints[1], endpoints[2], endpoints[3]};
  }
  std::vector<const servicelib::config::PoolConfig*> GetPools() const override {
    return {};
  }
  std::vector<const servicelib::config::LinkConfig*> GetLinks() const override {
    return {};
  }
  std::vector<const servicelib::config::ModuleConfig*> GetModules()
      const override {
    return {};
  }
  std::vector<const servicelib::config::TypeConfig*> GetTypes() const override {
    return {};
  }

  servicelib::config::GrpcDataConnectorConfig connector;
  servicelib::config::GrpcEndpointConfig endpoints[4];
};

class TestEnvironment final : public servicelib::IRuntimeEnvironment {
 public:
  TestEnvironment() : runtimeConfig_(config_) { service_.name = "grpc-test"; }
  servicelib::pool::ITaskPool* getTaskPool(const std::string&) override {
    return nullptr;
  }
  servicelib::pool::IPriorityTaskPool* getPriorityTaskPool(
      const std::string&) override {
    return nullptr;
  }
  std::shared_ptr<const servicelib::config::RuntimeConfig>
  getRuntimeConfigSnapshot() const override {
    return std::make_shared<const servicelib::config::RuntimeConfig>(
        runtimeConfig_);
  }
  std::shared_ptr<const servicelib::config::ServiceConfig>
  getServiceConfigSnapshot() const override {
    return std::make_shared<const servicelib::config::ServiceConfig>(service_);
  }
  servicelib::log::Logger& getLogger() override { return log_; }
  servicelib::metrics::Metrics& getMetrics() override { return metrics_; }
  servicelib::tracing::Tracing* getTracing() override { return nullptr; }

 private:
  TestConfig config_;
  servicelib::config::RuntimeConfig runtimeConfig_;
  servicelib::config::ServiceConfig service_;
  servicelib::testlog::TestLog log_;
  servicelib::testmetrics::TestMetrics metrics_;
};

struct SourceHandler final {
  using State = int;
  servicelib::BeginResult<State> beginRequest(
      servicelib::MessageContext context, auto&) {
    return {std::move(context), 0};
  }
  void consumeMessage(servicelib::MessageContext context, auto& sc, State&,
                      const std::string& request, auto result, auto&) {
    result.setResultCallback(
        "result", [result](servicelib::MessageContext, auto&, State&,
                           const std::string& value, auto& sender) mutable {
          sender.send("reply:" + value);
          result.done();
          return true;
        });
    sc.collect(std::move(context), request);
  }
  std::string getMessageId(servicelib::MessageContext, auto&, State&,
                           const std::string&) {
    return "result";
  }
  void eof(servicelib::MessageContext, auto&, State&) {}
  void endRequest(servicelib::MessageContext, auto&, std::exception_ptr,
                  State&) {}
};

struct FakeWriter {
  void Write(std::string&& value) { values.push_back(std::move(value)); }
  std::vector<std::string> values;
};

struct FakeReader {
  bool Read(std::string& value) {
    if (next == values.size()) return false;
    value = values[next++];
    return true;
  }
  std::vector<std::string> values;
  std::size_t next{};
};

struct FakeReaderWriter : FakeReader, FakeWriter {};

TEST(GrpcTracing, RequiresExplicitSampledTraceParent) {
  using servicelib::tracing::SampledTraceParent;
  EXPECT_FALSE(SampledTraceParent(""));
  EXPECT_FALSE(SampledTraceParent("00"));
  EXPECT_FALSE(SampledTraceParent(
      "00-00000000000000000000000000000001-0000000000000001-00"));
  EXPECT_TRUE(SampledTraceParent(
      "00-00000000000000000000000000000001-0000000000000001-01"));
  EXPECT_TRUE(SampledTraceParent(
      "00-00000000000000000000000000000001-0000000000000001-03"));
  EXPECT_FALSE(SampledTraceParent(
      "00-00000000000000000000000000000001-0000000000000001-g1"));
  EXPECT_FALSE(SampledTraceParent(
      "00-00000000000000000000000000000000-0000000000000001-01"));
}

TEST(GrpcDataSource, SupportsAllFourMethodTypesAndCorrelation) {
  TestEnvironment environment;

  using Unary = servicelib::datasource::grpc::NoStreamingEndpoint<
      std::string, std::string, std::string, std::string, SourceHandler>;
  Unary* unaryPtr{};
  Unary unary{environment, 1, SourceHandler{},
              [&](servicelib::MessageContext context,
                  servicelib::Payload<std::string> value) {
                unaryPtr->consumeResult(
                    std::move(context),
                    servicelib::Payload<std::string>::make(value.get()));
              },
              true};
  unaryPtr = &unary;
  unary.start(servicelib::Context{});
  EXPECT_EQ(
      unary.handle(servicelib::MessageContext{}.withStreamId("unary"), "one"),
      "reply:one");
  unary.stop(servicelib::Context{});

  using Server = servicelib::datasource::grpc::ServerStreamingEndpoint<
      std::string, std::string, std::string, std::string, SourceHandler>;
  Server* serverPtr{};
  Server server{environment, 2, SourceHandler{},
                [&](servicelib::MessageContext context,
                    servicelib::Payload<std::string> value) {
                  serverPtr->consumeResult(
                      std::move(context),
                      servicelib::Payload<std::string>::make(value.get()));
                },
                true};
  serverPtr = &server;
  server.start(servicelib::Context{});
  FakeWriter writer;
  server.handle(servicelib::MessageContext{}.withStreamId("server"), "two",
                writer);
  ASSERT_EQ(writer.values.size(), 1);
  EXPECT_EQ(writer.values[0], "reply:two");
  server.stop(servicelib::Context{});

  using Client = servicelib::datasource::grpc::ClientStreamingEndpoint<
      std::string, std::string, std::string, std::string, SourceHandler>;
  Client* clientPtr{};
  Client client{environment, 3, SourceHandler{},
                [&](servicelib::MessageContext context,
                    servicelib::Payload<std::string> value) {
                  clientPtr->consumeResult(
                      std::move(context),
                      servicelib::Payload<std::string>::make(value.get()));
                },
                true};
  clientPtr = &client;
  client.start(servicelib::Context{});
  FakeReader reader{{"three"}};
  EXPECT_EQ(client.handle(servicelib::MessageContext{}.withStreamId("client"),
                          reader),
            "reply:three");
  client.stop(servicelib::Context{});

  using Bidi = servicelib::datasource::grpc::BidirectionalStreamingEndpoint<
      std::string, std::string, std::string, std::string, SourceHandler>;
  Bidi* bidiPtr{};
  Bidi bidi{environment, 4, SourceHandler{},
            [&](servicelib::MessageContext context,
                servicelib::Payload<std::string> value) {
              bidiPtr->consumeResult(
                  std::move(context),
                  servicelib::Payload<std::string>::make(value.get()));
            },
            true};
  bidiPtr = &bidi;
  bidi.start(servicelib::Context{});
  FakeReaderWriter rw;
  rw.FakeReader::values = {"four"};
  bidi.handle(servicelib::MessageContext{}.withStreamId("bidi"), rw);
  ASSERT_EQ(rw.FakeWriter::values.size(), 1);
  EXPECT_EQ(rw.FakeWriter::values[0], "reply:four");
  bidi.stop(servicelib::Context{});
}

struct SinkHandler final {
  using State = int;
  std::vector<std::string>* responses{};
  servicelib::detail::SingleUseEvent* ended{};

  servicelib::BeginResult<State> beginRequest(
      servicelib::MessageContext context, auto&) {
    return {std::move(context), 0};
  }
  void consumeMessage(servicelib::MessageContext, auto&, State&,
                      const std::string& value, auto& sender, auto result) {
    sender.send(value);
    if (value == "last") result.done();
  }
  void handleResponse(servicelib::MessageContext, auto&, State&,
                      const std::string& response) {
    responses->push_back(response);
  }
  void endRequest(servicelib::MessageContext, auto&, std::exception_ptr,
                  State&) noexcept {
    if (ended) ended->Send();
  }
};

struct FakeClientStreamState {
  std::vector<std::string> requests;
};

struct FakeClientStream {
  std::shared_ptr<FakeClientStreamState> state;
  void WriteAndCheck(const std::string& request) {
    state->requests.push_back(request);
  }
  std::string Finish() { return "client:" + state->requests.back(); }
};

struct FakeBidiState {
  std::vector<std::string> requests;
  servicelib::detail::SingleUseEvent done;
};

struct FakeBidiStream {
  std::shared_ptr<FakeBidiState> state;
  bool read{};
  void WriteAndCheck(const std::string& request) {
    state->requests.push_back(request);
  }
  bool WritesDone() {
    state->done.Send();
    return true;
  }
  bool Read(std::string& response) {
    state->done.Wait();
    if (read) return false;
    read = true;
    response = "bidi:" + state->requests.back();
    return true;
  }
};

TEST(GrpcDataSink, SupportsAllFourMethodTypesAndStreamIdSessions) {
  TestEnvironment environment;
  std::vector<std::string> responses;
  TestSinkEndpointStream<std::string, std::string> unaryStream{environment, 1};
  TestSinkEndpointStream<std::string, std::string> serverStream{environment, 2};
  TestSinkEndpointStream<std::string, std::string> clientStream{environment, 3};
  TestSinkEndpointStream<std::string, std::string> bidiStream{environment, 4};

  auto unaryClient = [](const std::string& request,
                        servicelib::datasink::grpc::CallOptions) {
    return "unary:" + request;
  };
  servicelib::datasink::grpc::NoStreamingEndpoint<
      std::string, std::string, std::string, std::string, SinkHandler,
      decltype(unaryClient)>
      unary{unaryStream, SinkHandler{&responses}, unaryClient};
  unary.consume(servicelib::MessageContext{},
                servicelib::Payload<std::string>::make("one"));

  struct ServerRpc {
    bool read{};
    std::string value;
    bool Read(std::string& response) {
      if (read) return false;
      read = true;
      response = "server:" + value;
      return true;
    }
  };
  auto serverClient = [](const std::string& request,
                         servicelib::datasink::grpc::CallOptions) {
    return ServerRpc{false, request};
  };
  servicelib::datasink::grpc::ServerStreamingEndpoint<
      std::string, std::string, std::string, std::string, SinkHandler,
      decltype(serverClient)>
      server{serverStream, SinkHandler{&responses}, serverClient};
  server.consume(servicelib::MessageContext{},
                 servicelib::Payload<std::string>::make("two"));

  auto clientState = std::make_shared<FakeClientStreamState>();
  servicelib::detail::SingleUseEvent clientEnded;
  auto clientFn = [clientState](servicelib::datasink::grpc::CallOptions) {
    return FakeClientStream{clientState};
  };
  servicelib::datasink::grpc::ClientStreamingEndpoint<
      std::string, std::string, std::string, std::string, SinkHandler,
      decltype(clientFn)>
      client{clientStream, SinkHandler{&responses, &clientEnded}, clientFn};
  client.start(servicelib::Context{});
  auto clientContext =
      servicelib::MessageContext{}.withStreamId("client-stream");
  client.consume(clientContext,
                 servicelib::Payload<std::string>::make("first"));
  client.consume(clientContext, servicelib::Payload<std::string>::make("last"));
  clientEnded.Wait();
  client.stop(servicelib::Context{});
  EXPECT_EQ(clientState->requests, (std::vector<std::string>{"first", "last"}));

  auto bidiState = std::make_shared<FakeBidiState>();
  servicelib::detail::SingleUseEvent bidiEnded;
  auto bidiFn = [bidiState](servicelib::datasink::grpc::CallOptions) {
    return FakeBidiStream{bidiState};
  };
  servicelib::datasink::grpc::BidirectionalStreamingEndpoint<
      std::string, std::string, std::string, std::string, SinkHandler,
      decltype(bidiFn)>
      bidi{bidiStream, SinkHandler{&responses, &bidiEnded}, bidiFn};
  bidi.start(servicelib::Context{});
  auto bidiContext = servicelib::MessageContext{}.withStreamId("bidi-stream");
  bidi.consume(bidiContext, servicelib::Payload<std::string>::make("first"));
  bidi.consume(bidiContext, servicelib::Payload<std::string>::make("last"));
  bidiEnded.Wait();
  bidi.stop(servicelib::Context{});

  EXPECT_EQ(responses, (std::vector<std::string>{"unary:one", "server:two",
                                                 "client:last", "bidi:last"}));
}

}  // namespace

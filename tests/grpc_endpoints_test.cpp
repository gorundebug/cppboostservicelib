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
#include <servicelib/runtime/testtracing/testtracing.hpp>
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
  servicelib::tracing::Tracing* getTracing() override { return tracingEngine; }
  servicelib::tracing::Tracing* tracingEngine{};

 private:
  TestConfig config_;
  servicelib::config::RuntimeConfig runtimeConfig_;
  servicelib::config::ServiceConfig service_;
  servicelib::testlog::TestLog log_;
  servicelib::testmetrics::TestMetrics metrics_;
};

struct SourceHandler {
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

struct RetainedSourceHandler final : SourceHandler {
  void consumeMessage(servicelib::MessageContext context, auto& sc, State&,
                      const std::string& request, auto result, auto&) {
    result.setResultCallback(
        "result", [result, calls = 0](servicelib::MessageContext, auto&, State&,
                                     const std::string&, auto& sender) mutable {
          sender.send(std::to_string(++calls));
          if (calls == 2) result.done();
          return calls == 2;
        });
    sc.collect(context, request);
    sc.collect(std::move(context), request);
  }
};

TEST(GrpcDataSource, RetainsCallbackAcrossResults) {
  TestEnvironment environment;
  using Server = servicelib::datasource::grpc::ServerStreamingEndpoint<
      std::string, std::string, std::string, std::string, RetainedSourceHandler>;
  Server* endpointPtr{};
  Server endpoint{environment, 2, RetainedSourceHandler{},
                  [&](servicelib::MessageContext context,
                      servicelib::Payload<std::string> value) {
                    endpointPtr->consumeResult(std::move(context), std::move(value));
                  }, true};
  endpointPtr = &endpoint;
  endpoint.start(servicelib::Context{});
  FakeWriter writer;
  endpoint.handle(servicelib::MessageContext{}.withStreamId("retained"),
                  "request", writer);
  endpoint.stop(servicelib::Context{});
  EXPECT_EQ(writer.values, (std::vector<std::string>{"1", "2"}));
}

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

struct AsyncStreamControl final {
  servicelib::detail::SingleUseEvent beginEntered, beginRelease, started;
  servicelib::detail::SingleUseEvent holdEntered, holdRelease, secondEntered, ended;
  std::atomic<int> begins{0}, consumes{0}, writes{0}, responses{0}, ends{0}, cancels{0};
  std::atomic<bool> finished{false};
  bool holdBegin{false};
  bool earlyFinish{false};
  bool throwStart{false};
  bool holdResponse{false};
  std::function<void(std::string)> response;
  std::function<void(std::exception_ptr)> completion;
  void finish() {
    if (!finished.exchange(true)) completion({});
  }
};

struct AsyncStreamRpc final {
  std::shared_ptr<AsyncStreamControl> control;
  void write(std::string) { ++control->writes; }
  void done() { control->finish(); }
  void cancel() { ++control->cancels; control->finish(); }
};
struct AsyncStreamClient final {
  using AsyncSession = std::shared_ptr<AsyncStreamRpc>;
  std::shared_ptr<AsyncStreamControl> control;
  AsyncSession start(servicelib::datasink::grpc::CallOptions,
                     std::function<void(std::string)> response,
                     std::function<void(std::exception_ptr)> completion) {
    control->response = std::move(response);
    control->completion = std::move(completion);
    if (control->throwStart) throw std::runtime_error("start failed");
    auto rpc = std::make_shared<AsyncStreamRpc>(control);
    if (control->earlyFinish) {
      control->response("early");
      control->finish();
    }
    control->started.Send();
    return rpc;
  }
};
struct AsyncStreamHandler final {
  using State = int;
  std::shared_ptr<AsyncStreamControl> control;
  servicelib::BeginResult<State> beginRequest(servicelib::MessageContext context, auto&) {
    ++control->begins;
    control->beginEntered.Send();
    if (control->holdBegin) control->beginRelease.Wait();
    return {std::move(context), 0};
  }
  void consumeMessage(servicelib::MessageContext, auto&, State&,
                      const std::string& value, auto& sender, auto result) {
    ++control->consumes;
    if (value == "hold") {
      control->holdEntered.Send();
      control->holdRelease.Wait();
    }
    sender.send(value);
    if (value == "second") control->secondEntered.Send();
    if (value == "done") result.done();
  }
  void handleResponse(servicelib::MessageContext, auto&, State&, const std::string&) {
    ++control->responses;
    if (control->holdResponse) {
      control->holdEntered.Send();
      control->holdRelease.Wait();
    }
  }
  void endRequest(servicelib::MessageContext, auto&, std::exception_ptr, State&) noexcept {
    ++control->ends;
    control->ended.Send();
  }
};

template <bool Bidi>
using AsyncStreamEndpoint = std::conditional_t<Bidi,
    servicelib::datasink::grpc::BidirectionalStreamingEndpoint<
        std::string, std::string, std::string, std::string,
        AsyncStreamHandler, AsyncStreamClient>,
    servicelib::datasink::grpc::ClientStreamingEndpoint<
        std::string, std::string, std::string, std::string,
        AsyncStreamHandler, AsyncStreamClient>>;

bool Await(servicelib::detail::SingleUseEvent& event) {
  return event.WaitUntil(std::chrono::steady_clock::now() + std::chrono::seconds{5});
}

template <bool Bidi>
void CheckAsyncSessionLifecycle() {
  TestEnvironment environment;
  auto control = std::make_shared<AsyncStreamControl>();
  control->holdBegin = true;
  TestSinkEndpointStream<std::string, std::string> stream{environment, Bidi ? 4 : 3};
  AsyncStreamEndpoint<Bidi> endpoint{stream, AsyncStreamHandler{control},
                                    AsyncStreamClient{control}};
  endpoint.start({});
  auto context = servicelib::MessageContext{}.withStreamId("shared");
  std::thread creator([&] { endpoint.consume(context, servicelib::Payload<std::string>::make("first")); });
  EXPECT_TRUE(Await(control->beginEntered));
  // These calls must return before the creator finishes beginRequest.
  auto waiting = std::async(std::launch::async, [&] {
    endpoint.consume(context, servicelib::Payload<std::string>::make("hold"));
    endpoint.consume(context, servicelib::Payload<std::string>::make("second"));
    std::stop_source cancellation;
    auto cancelled = context.withStopToken(cancellation.get_token());
    endpoint.consume(cancelled, servicelib::Payload<std::string>::make("cancelled"));
    cancellation.request_stop();
  });
  EXPECT_EQ(waiting.wait_for(std::chrono::seconds{2}), std::future_status::ready);
  control->beginRelease.Send();
  creator.join();
  waiting.get();
  EXPECT_TRUE(Await(control->holdEntered));
  EXPECT_TRUE(Await(control->secondEntered));
  // Completion must not wait synchronously for the outstanding handler.
  auto completion = std::async(std::launch::async, [&] { control->finish(); });
  EXPECT_EQ(completion.wait_for(std::chrono::seconds{2}), std::future_status::ready);
  EXPECT_EQ(control->ends.load(), 0);
  control->completion({});
  control->holdRelease.Send();
  completion.get();
  EXPECT_TRUE(Await(control->ended));
  endpoint.stop({});
  EXPECT_EQ(control->begins.load(), 1);
  EXPECT_EQ(control->consumes.load(), 3);
  EXPECT_EQ(control->writes.load(), 3);
  EXPECT_EQ(control->ends.load(), 1);
  control->response("late");
  control->completion({});
  EXPECT_EQ(control->responses.load(), 0);
  EXPECT_EQ(control->ends.load(), 1);
}

template <bool Bidi>
void CheckStopDuringCreation() {
  TestEnvironment environment;
  auto control = std::make_shared<AsyncStreamControl>();
  control->holdBegin = true;
  TestSinkEndpointStream<std::string, std::string> stream{environment, Bidi ? 4 : 3};
  AsyncStreamEndpoint<Bidi> endpoint{stream, AsyncStreamHandler{control}, AsyncStreamClient{control}};
  endpoint.start({});
  std::thread creator([&] {
    endpoint.consume(servicelib::MessageContext{}.withStreamId("creating"),
                     servicelib::Payload<std::string>::make("first"));
  });
  EXPECT_TRUE(Await(control->beginEntered));
  auto stopped = std::async(std::launch::async, [&] { endpoint.stop({}); });
  EXPECT_EQ(stopped.wait_for(std::chrono::milliseconds{50}), std::future_status::timeout);
  control->beginRelease.Send();
  creator.join();
  EXPECT_EQ(stopped.wait_for(std::chrono::seconds{5}), std::future_status::ready);
  stopped.get();
  EXPECT_EQ(control->cancels.load(), 1);
  EXPECT_EQ(control->ends.load(), 1);
}

template <bool Bidi>
void CheckImmediateTransportCompletion(bool early, bool fail = false) {
  servicelib::testtracing::TestTracing tracing;
  TestEnvironment environment;
  environment.tracingEngine = &tracing;
  auto control = std::make_shared<AsyncStreamControl>();
  control->earlyFinish = early;
  control->throwStart = fail;
  TestSinkEndpointStream<std::string, std::string> stream{environment, Bidi ? 4 : 3};
  AsyncStreamEndpoint<Bidi> endpoint{stream, AsyncStreamHandler{control}, AsyncStreamClient{control}};
  endpoint.start({});
  endpoint.consume(servicelib::MessageContext{}.withStreamId("immediate").withSampling(true),
                   servicelib::Payload<std::string>::make("done"));
  EXPECT_TRUE(Await(control->ended));
  endpoint.stop({});
  EXPECT_EQ(control->responses.load(), early ? 1 : 0);
  EXPECT_EQ(control->ends.load(), 1);
  const auto spans = tracing.spans();
  ASSERT_EQ(spans.size(), 1U);
  EXPECT_EQ(spans.front().name, "grpc.output");
  std::vector<std::string> events;
  for (const auto& event : spans.front().events) events.push_back(event.name);
  if (early) {
    EXPECT_EQ(events, (std::vector<std::string>{"begin_request", "grpc_call", "handle_response"}));
  } else if (!fail) {
    EXPECT_EQ(events, (std::vector<std::string>{"begin_request", "grpc_call", "send",
        "done_called", "done_received", "consume_message"}));
  }
}

TEST(GrpcStreamingLifecycle, ClientWaitersAndCompletionDoNotBlock) { CheckAsyncSessionLifecycle<false>(); }
TEST(GrpcStreamingLifecycle, BidiWaitersAndCompletionDoNotBlock) { CheckAsyncSessionLifecycle<true>(); }
TEST(GrpcStreamingLifecycle, ClientStopIncludesInitializingSession) { CheckStopDuringCreation<false>(); }
TEST(GrpcStreamingLifecycle, BidiStopIncludesInitializingSession) { CheckStopDuringCreation<true>(); }
TEST(GrpcStreamingLifecycle, ClientCompletionInsideStart) { CheckImmediateTransportCompletion<false>(true); }
TEST(GrpcStreamingLifecycle, BidiCompletionInsideStart) { CheckImmediateTransportCompletion<true>(true); }
TEST(GrpcStreamingLifecycle, ClientCompletionInsideDone) { CheckImmediateTransportCompletion<false>(false); }
TEST(GrpcStreamingLifecycle, BidiCompletionInsideDone) { CheckImmediateTransportCompletion<true>(false); }


template <bool Bidi>
void CheckResponseDraining() {
  TestEnvironment environment;
  auto control = std::make_shared<AsyncStreamControl>();
  control->holdResponse = true;
  TestSinkEndpointStream<std::string, std::string> stream{environment, Bidi ? 4 : 3};
  AsyncStreamEndpoint<Bidi> endpoint{stream, AsyncStreamHandler{control}, AsyncStreamClient{control}};
  endpoint.start({});
  endpoint.consume(servicelib::MessageContext{}.withStreamId("response"),
                   servicelib::Payload<std::string>::make("first"));
  std::thread response([&] { control->response("hold"); });
  EXPECT_TRUE(Await(control->holdEntered));
  auto completion = std::async(std::launch::async, [&] { control->finish(); });
  EXPECT_EQ(completion.wait_for(std::chrono::seconds{2}), std::future_status::ready);
  EXPECT_EQ(control->ends.load(), 0);
  control->holdRelease.Send();
  response.join();
  completion.get();
  endpoint.stop({});
  EXPECT_EQ(control->responses.load(), 1);
  EXPECT_EQ(control->ends.load(), 1);
}
TEST(GrpcStreamingLifecycle, ClientResponseDrainsBeforeEnd) { CheckResponseDraining<false>(); }
TEST(GrpcStreamingLifecycle, BidiResponseDrainsBeforeEnd) { CheckResponseDraining<true>(); }
TEST(GrpcStreamingLifecycle, ClientStartFailureDrains) { CheckImmediateTransportCompletion<false>(false, true); }
TEST(GrpcStreamingLifecycle, BidiStartFailureDrains) { CheckImmediateTransportCompletion<true>(false, true); }


struct LegacyStreamRpc final {
  std::shared_ptr<AsyncStreamControl> control;
  bool read{false};
  void WriteAndCheck(const std::string&) { ++control->writes; }
  std::string Finish() { return "response"; }
  bool WritesDone() { control->started.Send(); return true; }
  bool Read(std::string& value) {
    if (read) return false;
    control->started.Wait();
    read = true;
    value = "response";
    return true;
  }
};

template <bool Bidi>
void CheckLegacyWaiter() {
  TestEnvironment environment;
  auto control = std::make_shared<AsyncStreamControl>();
  control->holdBegin = true;
  auto client = [control](servicelib::datasink::grpc::CallOptions) {
    return LegacyStreamRpc{control};
  };
  using Client = decltype(client);
  using Endpoint = std::conditional_t<Bidi,
      servicelib::datasink::grpc::BidirectionalStreamingEndpoint<
          std::string, std::string, std::string, std::string, AsyncStreamHandler, Client>,
      servicelib::datasink::grpc::ClientStreamingEndpoint<
          std::string, std::string, std::string, std::string, AsyncStreamHandler, Client>>;
  TestSinkEndpointStream<std::string, std::string> stream{environment, Bidi ? 4 : 3};
  Endpoint endpoint{stream, AsyncStreamHandler{control}, client};
  endpoint.start({});
  auto context = servicelib::MessageContext{}.withStreamId("legacy");
  std::thread creator([&] {
    endpoint.consume(context, servicelib::Payload<std::string>::make("first"));
  });
  EXPECT_TRUE(Await(control->beginEntered));
  auto waiter = std::async(std::launch::async, [&] {
    endpoint.consume(context, servicelib::Payload<std::string>::make("done"));
  });
  EXPECT_EQ(waiter.wait_for(std::chrono::seconds{2}), std::future_status::ready);
  control->beginRelease.Send();
  creator.join();
  waiter.get();
  EXPECT_TRUE(Await(control->ended));
  endpoint.stop({});
  EXPECT_EQ(control->begins.load(), 1);
  EXPECT_EQ(control->ends.load(), 1);
}
TEST(GrpcStreamingLifecycle, LegacyClientWaiterReturnsBeforeReady) { CheckLegacyWaiter<false>(); }
TEST(GrpcStreamingLifecycle, LegacyBidiWaiterReturnsBeforeReady) { CheckLegacyWaiter<true>(); }

}  // namespace

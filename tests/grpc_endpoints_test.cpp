#include <atomic>
#include <exception>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>


#include <servicelib/datasink/grpc/asio.hpp>
#include <servicelib/datasource/grpc/asio.hpp>
#include <servicelib/runtime/detail/sync.hpp>
#include <servicelib/runtime/detail/grpc_streaming.hpp>
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

class LifecycleGroupingGrpcEndpoint final : public servicelib::datasink::grpc::IEndpoint {
 public:
  LifecycleGroupingGrpcEndpoint(int id, std::function<void()> stop)
      : id_(id), stop_(std::move(stop)) {}
  int id() const noexcept override { return id_; }
  void start(servicelib::Context) override {}
  void stop(servicelib::Context) override { stop_(); }
 private:
  int id_;
  std::function<void()> stop_;
};

TEST(GrpcDataSink, StopIsConcurrentBetweenIdsButReverseSequentialWithinOneId) {
  using namespace std::chrono_literals;
  TestEnvironment environment;
  TestSinkEndpointStream<int, int> stream{environment, 1};
  auto sink = servicelib::datasink::grpc::DataSink::make(stream);
  std::promise<void> lastEntered, otherEntered, releaseLast;
  auto lastReady = lastEntered.get_future();
  auto otherReady = otherEntered.get_future();
  auto release = releaseLast.get_future().share();
  std::atomic<bool> firstStarted{false};
  std::atomic<bool> lastFinished{false};
  std::mutex orderMutex;
  std::vector<int> sameEndpointOrder;
  sink->addEndpoint(std::make_shared<LifecycleGroupingGrpcEndpoint>(1, [&] {
    firstStarted.store(true);
    EXPECT_TRUE(lastFinished.load());
    std::lock_guard lock(orderMutex);
    sameEndpointOrder.push_back(1);
  }));
  sink->addEndpoint(std::make_shared<LifecycleGroupingGrpcEndpoint>(1, [&] {
    {
      std::lock_guard lock(orderMutex);
      sameEndpointOrder.push_back(2);
    }
    lastEntered.set_value();
    release.wait();
    lastFinished.store(true);
  }));
  sink->addEndpoint(std::make_shared<LifecycleGroupingGrpcEndpoint>(2, [&] {
    otherEntered.set_value();
  }));
  sink->start({});
  auto stopped = std::async(std::launch::async, [&] { sink->stop({}); });
  EXPECT_EQ(lastReady.wait_for(1s), std::future_status::ready);
  EXPECT_EQ(otherReady.wait_for(1s), std::future_status::ready);
  EXPECT_FALSE(firstStarted.load());
  EXPECT_EQ(stopped.wait_for(0ms), std::future_status::timeout);
  releaseLast.set_value();
  EXPECT_NO_THROW(stopped.get());
  EXPECT_TRUE(firstStarted.load());
  EXPECT_EQ(sameEndpointOrder, (std::vector<int>{2, 1}));
}

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

struct CooperativeSourceHandler final {
  using State = int;
  servicelib::detail::SingleUseEvent* entered;
  servicelib::detail::SingleUseEvent* release;
  int* ends;

  servicelib::BeginResult<State> beginRequest(servicelib::MessageContext context, auto&) {
    EXPECT_TRUE(servicelib::detail::CooperativeExecution::Active());
    return {std::move(context), 0};
  }
  void consumeMessage(servicelib::MessageContext context, auto& stream, State&,
                      const std::string& request, auto, auto& sender) {
    EXPECT_TRUE(servicelib::detail::CooperativeExecution::Active());
    entered->Send();
    release->Wait();
    stream.collect(std::move(context), request);
    sender.send("reply:" + request);
  }
  std::string getMessageId(servicelib::MessageContext, auto&, State&, const std::string&) {
    return "result";
  }
  void eof(servicelib::MessageContext, auto&, State&) {
    EXPECT_TRUE(servicelib::detail::CooperativeExecution::Active());
  }
  void endRequest(servicelib::MessageContext, auto&, std::exception_ptr error, State&) {
    EXPECT_TRUE(servicelib::detail::CooperativeExecution::Active());
    EXPECT_FALSE(error);
    ++*ends;
  }
};

struct CooperativeSourceRpc final {
  using Request = std::string;
  using Response = std::string;
  struct Call { void TryCancel() {} } call;
  bool received{};
  std::vector<std::string> responses;
  Call& context() { return call; }
  template <typename Token>
  boost::asio::awaitable<bool> read(std::string& request, Token) {
    co_await boost::asio::post(boost::asio::use_awaitable);
    if (received) co_return false;
    received = true;
    request = "request";
    co_return true;
  }
  template <typename Token>
  boost::asio::awaitable<bool> write(const std::string& response, Token) {
    co_await boost::asio::post(boost::asio::use_awaitable);
    responses.push_back(response);
    co_return true;
  }
};

template <int Method>
void CheckCooperativeStreamingSource() {
  boost::asio::io_context io;
  struct RestoreExecutor {
    boost::asio::any_io_executor previous = servicelib::detail::ParallelExecutorRegistry::Get();
    ~RestoreExecutor() { servicelib::detail::ParallelExecutorRegistry::Set(previous); }
  } restore;
  servicelib::detail::ParallelExecutorRegistry::Set(io.get_executor());
  TestEnvironment environment;
  using Server = servicelib::datasource::grpc::ServerStreamingEndpoint<
      std::string, std::string, std::string, std::string, CooperativeSourceHandler>;
  using Client = servicelib::datasource::grpc::ClientStreamingEndpoint<
      std::string, std::string, std::string, std::string, CooperativeSourceHandler>;
  using Bidi = servicelib::datasource::grpc::BidirectionalStreamingEndpoint<
      std::string, std::string, std::string, std::string, CooperativeSourceHandler>;
  using Endpoint = std::conditional_t<Method == 2, Server,
      std::conditional_t<Method == 3, Client, Bidi>>;
  servicelib::detail::SingleUseEvent entered, release;
  int ends = 0;
  std::vector<std::string> observed;
  Endpoint endpoint{environment, Method, CooperativeSourceHandler{&entered, &release, &ends},
      [&](servicelib::MessageContext context, servicelib::Payload<std::string> value) {
        EXPECT_EQ(context.streamId(), "parent");
        observed.push_back(value.get());
      }, false};
  CooperativeSourceRpc rpc;
  const std::string request = "request";
  auto operation = [&]() -> boost::asio::awaitable<void> {
    auto context = servicelib::MessageContext{}.withStreamId("parent");
    if constexpr (Method == 2) {
      co_await servicelib::grpc_transport::HandleServerStreamingSource(endpoint, rpc, request, context);
    } else if constexpr (Method == 3) {
      const auto response = co_await servicelib::grpc_transport::HandleClientStreamingSource(endpoint, rpc, context);
      EXPECT_EQ(response, "reply:request");
    } else {
      co_await servicelib::grpc_transport::HandleBidirectionalStreamingSource(endpoint, rpc, context);
    }
  };
  auto consumed = boost::asio::co_spawn(io, operation(), boost::asio::use_future);
  auto responder = boost::asio::co_spawn(io,
      servicelib::detail::CooperativeExecution::Run([&] {
        entered.Wait();
        EXPECT_EQ(ends, 0);
        EXPECT_TRUE(observed.empty());
        release.Send();
      }), boost::asio::use_future);
  io.run();
  consumed.get(); responder.get();
  EXPECT_EQ(ends, 1);
  EXPECT_EQ(observed, (std::vector<std::string>{"request"}));
  if constexpr (Method != 3) {
    EXPECT_EQ(rpc.responses, (std::vector<std::string>{"reply:request"}));
  }
}

TEST(GrpcDataSource, ServerStreamingBusinessCallCanSuspendOnOneWorker) {
  CheckCooperativeStreamingSource<2>();
}
TEST(GrpcDataSource, ClientStreamingBusinessCallCanSuspendOnOneWorker) {
  CheckCooperativeStreamingSource<3>();
}
TEST(GrpcDataSource, BidirectionalBusinessCallCanSuspendOnOneWorker) {
  CheckCooperativeStreamingSource<4>();
}

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

struct ControlledUnaryCall final {
  servicelib::detail::SingleUseEvent started;
  std::function<void(std::exception_ptr, std::optional<std::string>)> complete;
};

struct ControlledUnaryClient final {
  std::shared_ptr<ControlledUnaryCall> call;

  void async(std::string, servicelib::datasink::grpc::CallOptions,
             std::function<void(std::exception_ptr,
                                std::optional<std::string>)> completion) {
    call->complete = std::move(completion);
    call->started.Send();
  }
};

TEST(GrpcDataSink, DirectUnaryConsumeWaitsForResponseAndEndRequest) {
  TestEnvironment environment;
  TestSinkEndpointStream<std::string, std::string> stream{environment, 1};
  std::vector<std::string> responses;
  servicelib::detail::SingleUseEvent ended;
  auto call = std::make_shared<ControlledUnaryCall>();
  servicelib::datasink::grpc::NoStreamingEndpoint<
      std::string, std::string, std::string, std::string, SinkHandler,
      ControlledUnaryClient>
      endpoint{stream, SinkHandler{&responses, &ended}, ControlledUnaryClient{call}};
  auto consumed = std::async(std::launch::async, [&] {
    endpoint.consume(servicelib::MessageContext{},
                     servicelib::Payload<std::string>::make("request"));
  });
  const bool started = call->started.WaitUntil(
      std::chrono::steady_clock::now() + std::chrono::seconds{5});
  EXPECT_TRUE(started);
  if (started) {
    // The transport has accepted the RPC, but no response is available yet.
    // A FunctionCall must not expose this as completed business processing.
    EXPECT_EQ(consumed.wait_for(std::chrono::milliseconds{20}),
              std::future_status::timeout);
    call->complete({}, std::string{"response"});
  }
  consumed.get();
  EXPECT_TRUE(ended.WaitUntil(
      std::chrono::steady_clock::now() + std::chrono::seconds{5}));
  EXPECT_EQ(responses, (std::vector<std::string>{"response"}));
}

TEST(GrpcDataSink, UnaryConsumeCooperativelyWaitsOnOneWorker) {
  boost::asio::io_context io;
  TestEnvironment environment;
  TestSinkEndpointStream<std::string, std::string> stream{environment, 1};
  std::vector<std::string> responses;
  servicelib::detail::SingleUseEvent ended;
  auto call = std::make_shared<ControlledUnaryCall>();
  servicelib::datasink::grpc::NoStreamingEndpoint<
      std::string, std::string, std::string, std::string, SinkHandler,
      ControlledUnaryClient>
      endpoint{stream, SinkHandler{&responses, &ended}, ControlledUnaryClient{call}};
  bool returned = false;
  auto consumed = boost::asio::co_spawn(io,
      servicelib::detail::CooperativeExecution::Run([&] {
        endpoint.consume(servicelib::MessageContext{},
                         servicelib::Payload<std::string>::make("request"));
        EXPECT_TRUE(ended.IsReady());
        EXPECT_EQ(responses, (std::vector<std::string>{"response"}));
        EXPECT_TRUE(servicelib::detail::CooperativeExecution::Active());
        returned = true;
      }), boost::asio::use_future);
  auto transport = boost::asio::co_spawn(io,
      servicelib::detail::CooperativeExecution::Run([&] {
        call->started.Wait();
        EXPECT_FALSE(returned);
        EXPECT_TRUE(responses.empty());
        call->complete({}, std::string{"response"});
        // Completion only publishes the response; it does not run business
        // callbacks inline on the transport's stack.
        EXPECT_TRUE(responses.empty());
      }), boost::asio::use_future);
  io.run();
  consumed.get();
  transport.get();
  EXPECT_TRUE(returned);
}

struct ControlledServerCall final {
  servicelib::detail::SingleUseEvent started, responseEntered, releaseResponse;
  std::function<void(std::string)> response;
  std::function<void(std::exception_ptr)> complete;
};

struct ControlledServerClient final {
  std::shared_ptr<ControlledServerCall> call;
  void async(std::string, servicelib::datasink::grpc::CallOptions,
             std::function<void(std::string)> response,
             std::function<void(std::exception_ptr)> completion) {
    call->response = std::move(response);
    call->complete = std::move(completion);
    call->started.Send();
  }
};

struct CooperativeServerSinkHandler final {
  using State = int;
  std::shared_ptr<ControlledServerCall> call;
  std::vector<std::string>* responses;
  int* endCalls;
  std::exception_ptr* error;
  bool failResponse;
  servicelib::BeginResult<State> beginRequest(servicelib::MessageContext context, auto&) {
    return {std::move(context), 0};
  }
  void consumeMessage(servicelib::MessageContext, auto&, State&,
                      const std::string& value, auto& sender, auto) {
    sender.send(value);
  }
  void handleResponse(servicelib::MessageContext, auto&, State&, const std::string& value) {
    EXPECT_TRUE(servicelib::detail::CooperativeExecution::Active());
    call->responseEntered.Send();
    call->releaseResponse.Wait();
    if (failResponse) throw std::logic_error("response failed");
    responses->push_back(value);
  }
  void endRequest(servicelib::MessageContext, auto&, std::exception_ptr value, State&) {
    EXPECT_TRUE(servicelib::detail::CooperativeExecution::Active());
    *error = value;
    ++*endCalls;
  }
};

TEST(GrpcDataSink, ServerStreamingWaitsForHandlersAndEndOnOneWorker) {
  for (const bool failResponse : {false, true}) {
    SCOPED_TRACE(failResponse);
    boost::asio::io_context io;
    TestEnvironment environment;
    TestSinkEndpointStream<std::string, std::string> stream{environment, 2};
    auto call = std::make_shared<ControlledServerCall>();
    std::vector<std::string> responses;
    int endCalls = 0;
    std::exception_ptr error;
    bool returned = false;
    servicelib::datasink::grpc::ServerStreamingEndpoint<
        std::string, std::string, std::string, std::string,
        CooperativeServerSinkHandler, ControlledServerClient>
        endpoint{stream, {call, &responses, &endCalls, &error, failResponse}, {call}};
    auto consumed = boost::asio::co_spawn(io,
        servicelib::detail::CooperativeExecution::Run([&] {
          endpoint.consume({}, servicelib::Payload<std::string>::make("request"));
          EXPECT_EQ(endCalls, 1);
          EXPECT_EQ(static_cast<bool>(error), failResponse);
          returned = true;
        }), boost::asio::use_future);
    auto transport = boost::asio::co_spawn(io,
        servicelib::detail::CooperativeExecution::Run([&] {
          call->started.Wait();
          EXPECT_FALSE(returned);
          try {
            call->response("first");
            call->response("second");
            call->complete({});
          } catch (...) {
            call->complete(std::current_exception());
          }
          // Terminal callbacks only publish completion; EndRequest runs on
          // the resumed caller, and late/duplicate transport callbacks are inert.
          EXPECT_EQ(endCalls, 0);
          call->response("late");
          call->complete({});
        }), boost::asio::use_future);
    auto release = boost::asio::co_spawn(io,
        servicelib::detail::CooperativeExecution::Run([&] {
          call->responseEntered.Wait();
          EXPECT_FALSE(returned);
          EXPECT_EQ(endCalls, 0);
          call->releaseResponse.Send();
        }), boost::asio::use_future);
    io.run();
    consumed.get(); transport.get(); release.get();
    EXPECT_TRUE(returned);
    if (failResponse) {
      EXPECT_TRUE(responses.empty());
      ASSERT_NE(error, nullptr);
      EXPECT_THROW(std::rethrow_exception(error), std::logic_error);
    } else {
      EXPECT_EQ(responses, (std::vector<std::string>{"first", "second"}));
    }
  }
}

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
  void finish(std::exception_ptr error = {}) {
    if (!finished.exchange(true)) completion(error);
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

struct AsyncCallbackObservation final {
  boost::asio::any_io_executor executor;
  servicelib::detail::SingleUseEvent responseRelease, endRelease, ended;
  std::atomic<bool> responseCooperative{}, endCooperative{};
  std::atomic<bool> responseReleased{}, endReleased{};
  std::atomic<int> responses{}, ends{};
};

struct CooperativeAsyncSinkHandler final {
  using State = int;
  std::shared_ptr<AsyncCallbackObservation> observation;
  bool failResponse{};

  servicelib::BeginResult<State> beginRequest(servicelib::MessageContext context, auto&) {
    return {std::move(context), 0};
  }
  void consumeMessage(servicelib::MessageContext, auto&, State&,
                      const std::string& value, auto& sender, auto) {
    sender.send(value);
  }
  void handleResponse(servicelib::MessageContext context, auto&, State&,
                      const std::string&) {
    EXPECT_EQ(context.streamId(), "cooperative-callbacks");
    ++observation->responses;
    observation->responseCooperative = servicelib::detail::CooperativeExecution::Active();
    boost::asio::post(observation->executor, [state = observation] {
      state->responseRelease.Send();
    });
    observation->responseReleased = observation->responseRelease.WaitUntil(
        std::chrono::steady_clock::now() + std::chrono::milliseconds{500});
    if (failResponse) throw std::logic_error("response failed");
  }
  void endRequest(servicelib::MessageContext context, auto&, std::exception_ptr error,
                  State&) noexcept {
    EXPECT_EQ(context.streamId(), "cooperative-callbacks");
    EXPECT_EQ(static_cast<bool>(error), failResponse);
    if (error) {
      try {
        std::rethrow_exception(error);
      } catch (const std::logic_error& failure) {
        EXPECT_STREQ(failure.what(), "response failed");
      } catch (...) {
        ADD_FAILURE() << "unexpected callback failure type";
      }
    }
    EXPECT_EQ(observation->responses.load(), 1);
    ++observation->ends;
    observation->endCooperative = servicelib::detail::CooperativeExecution::Active();
    boost::asio::post(observation->executor, [state = observation] {
      state->endRelease.Send();
    });
    observation->endReleased = observation->endRelease.WaitUntil(
        std::chrono::steady_clock::now() + std::chrono::milliseconds{500});
    observation->ended.Send();
  }
};

template <bool Bidi>
void CheckCooperativeAsyncSinkCallbacks(bool failResponse = false) {
  for (const bool early : {false, true}) {
    SCOPED_TRACE(early ? "response during session creation" : "response after session creation");
    boost::asio::io_context io;
    auto work = boost::asio::make_work_guard(io);
    struct RestoreExecutor final {
      boost::asio::any_io_executor previous = servicelib::detail::ParallelExecutorRegistry::Get();
      ~RestoreExecutor() { servicelib::detail::ParallelExecutorRegistry::Set(previous); }
    } restore;
    servicelib::detail::ParallelExecutorRegistry::Set(io.get_executor());
    TestEnvironment environment;
    TestSinkEndpointStream<std::string, std::string> stream{environment, Bidi ? 4 : 3};
    auto control = std::make_shared<AsyncStreamControl>();
    control->earlyFinish = early;
    auto observation = std::make_shared<AsyncCallbackObservation>();
    observation->executor = io.get_executor();
    using Endpoint = std::conditional_t<Bidi,
        servicelib::datasink::grpc::BidirectionalStreamingEndpoint<
            std::string, std::string, std::string, std::string,
            CooperativeAsyncSinkHandler, AsyncStreamClient>,
        servicelib::datasink::grpc::ClientStreamingEndpoint<
            std::string, std::string, std::string, std::string,
            CooperativeAsyncSinkHandler, AsyncStreamClient>>;
    Endpoint endpoint{stream, CooperativeAsyncSinkHandler{observation, failResponse}, AsyncStreamClient{control}};
    endpoint.start({});
    auto consumed = boost::asio::co_spawn(io,
        servicelib::detail::CooperativeExecution::Run([&] {
          endpoint.consume(servicelib::MessageContext{}.withStreamId("cooperative-callbacks"),
                           servicelib::Payload<std::string>::make("request"));
          if (!early) {
            try {
              control->response("response");
              control->finish();
            } catch (...) {
              control->finish(std::current_exception());
            }
          }
        }), boost::asio::use_future);
    std::jthread worker([&] { io.run(); });
    const bool ended = observation->ended.WaitUntil(
        std::chrono::steady_clock::now() + std::chrono::seconds{5});
    observation->responseRelease.Send();
    observation->endRelease.Send();
    endpoint.stop({});
    work.reset();
    worker.join();
    consumed.get();
    EXPECT_TRUE(ended);
    EXPECT_TRUE(observation->responseCooperative.load());
    EXPECT_TRUE(observation->endCooperative.load());
    EXPECT_TRUE(observation->responseReleased.load());
    EXPECT_TRUE(observation->endReleased.load());
    EXPECT_EQ(observation->responses.load(), 1);
    EXPECT_EQ(observation->ends.load(), 1);
  }
}

TEST(GrpcDataSink, ClientStreamingCallbacksReleaseSingleWorker) {
  CheckCooperativeAsyncSinkCallbacks<false>();
}

TEST(GrpcDataSink, BidiStreamingCallbacksReleaseSingleWorker) {
  CheckCooperativeAsyncSinkCallbacks<true>();
}

TEST(GrpcDataSink, ClientStreamingCallbacksPreserveResponseError) {
  CheckCooperativeAsyncSinkCallbacks<false>(true);
}

TEST(GrpcDataSink, BidiStreamingCallbacksPreserveResponseError) {
  CheckCooperativeAsyncSinkCallbacks<true>(true);
}

template <bool Bidi>
using AsyncStreamEndpoint = std::conditional_t<Bidi,
    servicelib::datasink::grpc::BidirectionalStreamingEndpoint<
        std::string, std::string, std::string, std::string,
        AsyncStreamHandler, AsyncStreamClient>,
    servicelib::datasink::grpc::ClientStreamingEndpoint<
        std::string, std::string, std::string, std::string,
        AsyncStreamHandler, AsyncStreamClient>>;

template <bool Bidi>
void CheckStreamingResponseConcurrency() {
  boost::asio::io_context io;
  auto work = boost::asio::make_work_guard(io);
  struct RestoreExecutor final {
    boost::asio::any_io_executor previous = servicelib::detail::ParallelExecutorRegistry::Get();
    ~RestoreExecutor() { servicelib::detail::ParallelExecutorRegistry::Set(previous); }
  } restore;
  servicelib::detail::ParallelExecutorRegistry::Set(io.get_executor());
  TestEnvironment environment;
  auto control = std::make_shared<AsyncStreamControl>();
  TestSinkEndpointStream<std::string, std::string> stream{environment, Bidi ? 4 : 3};
  AsyncStreamEndpoint<Bidi> endpoint{stream, AsyncStreamHandler{control}, AsyncStreamClient{control}};
  endpoint.start({});
  int responsesBeforeRelease = -1;
  auto held = boost::asio::co_spawn(io, servicelib::detail::CooperativeExecution::Run([&] {
    endpoint.consume(servicelib::MessageContext{}.withStreamId("response-concurrency"),
                     servicelib::Payload<std::string>::make("hold"));
  }), boost::asio::use_future);
  auto reader = boost::asio::co_spawn(io, servicelib::detail::CooperativeExecution::Run([&] {
    control->holdEntered.Wait();
    control->response("response");
    control->finish();
  }), boost::asio::use_future);
  // Drain all runnable handlers, including a separately scheduled response.
  // Only work awaiting the held message may remain; no sleep or queue-hop
  // count is used to infer whether the response was allowed to overtake it.
  io.poll();
  responsesBeforeRelease = control->responses.load();
  control->holdRelease.Send();
  std::jthread worker([&] { io.run(); });
  const bool ended = control->ended.WaitUntil(
      std::chrono::steady_clock::now() + std::chrono::seconds{5});
  control->holdRelease.Send();
  endpoint.stop({});
  work.reset();
  worker.join();
  held.get();
  reader.get();
  EXPECT_TRUE(ended);
  EXPECT_EQ(responsesBeforeRelease, Bidi ? 1 : 0);
  EXPECT_EQ(control->begins.load(), 1);
  EXPECT_EQ(control->consumes.load(), 1);
  EXPECT_EQ(control->responses.load(), 1);
  EXPECT_EQ(control->ends.load(), 1);
}

TEST(GrpcDataSink, ClientStreamingResponseWaitsForAdmittedMessages) {
  CheckStreamingResponseConcurrency<false>();
}

TEST(GrpcDataSink, BidiStreamingResponseCanOverlapAdmittedMessages) {
  CheckStreamingResponseConcurrency<true>();
}

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
  // Every Consume waits for creation and its own handler, but distinct
  // messages are still allowed to execute concurrently within the session.
  auto held = std::async(std::launch::async, [&] {
    endpoint.consume(context, servicelib::Payload<std::string>::make("hold"));
  });
  auto second = std::async(std::launch::async, [&] {
    endpoint.consume(context, servicelib::Payload<std::string>::make("second"));
  });
  std::stop_source cancellation;
  auto cancelled = context.withStopToken(cancellation.get_token());
  auto cancelledWaiter = std::async(std::launch::async, [&] {
    endpoint.consume(cancelled, servicelib::Payload<std::string>::make("cancelled"));
  });
  cancellation.request_stop();
  EXPECT_EQ(held.wait_for(std::chrono::milliseconds{20}), std::future_status::timeout);
  EXPECT_EQ(second.wait_for(std::chrono::milliseconds{20}), std::future_status::timeout);
  EXPECT_EQ(cancelledWaiter.wait_for(std::chrono::milliseconds{20}), std::future_status::timeout);
  control->beginRelease.Send();
  creator.join();
  EXPECT_TRUE(Await(control->holdEntered));
  EXPECT_TRUE(Await(control->secondEntered));
  second.get();
  cancelledWaiter.get();
  EXPECT_EQ(held.wait_for(std::chrono::milliseconds{20}), std::future_status::timeout);
  // Completion must not wait synchronously for the outstanding handler.
  auto completion = std::async(std::launch::async, [&] { control->finish(); });
  EXPECT_EQ(completion.wait_for(std::chrono::seconds{2}), std::future_status::ready);
  EXPECT_EQ(control->ends.load(), 0);
  control->completion({});
  control->holdRelease.Send();
  held.get();
  completion.get();
  EXPECT_TRUE(Await(control->ended));
  endpoint.stop({});
  EXPECT_EQ(control->begins.load(), 1);
  EXPECT_EQ(control->consumes.load(), 4);
  EXPECT_EQ(control->writes.load(), 4);
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

TEST(GrpcStreamingLifecycle, ClientWaitersPreserveConsumeBoundary) { CheckAsyncSessionLifecycle<false>(); }
TEST(GrpcStreamingLifecycle, BidiWaitersPreserveConsumeBoundary) { CheckAsyncSessionLifecycle<true>(); }
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
  EXPECT_EQ(waiter.wait_for(std::chrono::milliseconds{20}), std::future_status::timeout);
  control->beginRelease.Send();
  creator.join();
  waiter.get();
  EXPECT_TRUE(Await(control->ended));
  endpoint.stop({});
  EXPECT_EQ(control->begins.load(), 1);
  EXPECT_EQ(control->ends.load(), 1);
}
TEST(GrpcStreamingLifecycle, LegacyClientWaiterWaitsForReady) { CheckLegacyWaiter<false>(); }
TEST(GrpcStreamingLifecycle, LegacyBidiWaiterWaitsForReady) { CheckLegacyWaiter<true>(); }

template <bool Bidi>
void CheckCooperativeSessionReadiness() {
  boost::asio::io_context io;
  struct RestoreExecutor final {
    boost::asio::any_io_executor previous = servicelib::detail::ParallelExecutorRegistry::Get();
    ~RestoreExecutor() { servicelib::detail::ParallelExecutorRegistry::Set(previous); }
  } restoreExecutor;
  servicelib::detail::ParallelExecutorRegistry::Set(io.get_executor());
  TestEnvironment environment;
  auto control = std::make_shared<AsyncStreamControl>();
  control->holdBegin = true;
  TestSinkEndpointStream<std::string, std::string> stream{environment, Bidi ? 4 : 3};
  AsyncStreamEndpoint<Bidi> endpoint{stream, AsyncStreamHandler{control}, AsyncStreamClient{control}};
  endpoint.start({});
  const auto context = servicelib::MessageContext{}.withStreamId("one-worker");
  servicelib::detail::SingleUseEvent waiterEntered;
  bool waiterReturned = false;
  auto creator = boost::asio::co_spawn(io, servicelib::detail::CooperativeExecution::Run([&] {
    endpoint.consume(context, servicelib::Payload<std::string>::make("first"));
  }), boost::asio::use_future);
  auto waiter = boost::asio::co_spawn(io, servicelib::detail::CooperativeExecution::Run([&] {
    control->beginEntered.Wait();
    waiterEntered.Send();
    endpoint.consume(context, servicelib::Payload<std::string>::make("second"));
    EXPECT_TRUE(control->secondEntered.IsReady());
    waiterReturned = true;
  }), boost::asio::use_future);
  auto release = boost::asio::co_spawn(io, servicelib::detail::CooperativeExecution::Run([&] {
    waiterEntered.Wait();
    EXPECT_FALSE(waiterReturned);
    control->beginRelease.Send();
  }), boost::asio::use_future);
  std::jthread worker([&] { io.run(); });
  const auto status = waiter.wait_for(std::chrono::seconds{5});
  // Rescue a regressed blocking wait so cleanup can still complete.
  control->beginRelease.Send();
  EXPECT_EQ(status, std::future_status::ready);
  creator.get(); waiter.get(); release.get();
  EXPECT_EQ(control->begins.load(), 1);
  EXPECT_EQ(control->consumes.load(), 2);
  EXPECT_EQ(control->writes.load(), 2);
  EXPECT_EQ(control->ends.load(), 0);
  control->finish();
  EXPECT_TRUE(Await(control->ended));
  endpoint.stop({});
  io.stop();
  worker.join();
}

TEST(GrpcStreamingLifecycle, ClientCreationWaitReleasesOneWorker) { CheckCooperativeSessionReadiness<false>(); }
TEST(GrpcStreamingLifecycle, BidiCreationWaitReleasesOneWorker) { CheckCooperativeSessionReadiness<true>(); }

}  // namespace

namespace {

struct EarlyBidiOrderControl final {
  servicelib::detail::SingleUseEvent firstEntered, releaseFirst, ended;
  std::vector<std::string> responses;
  bool secondEntered{};
  bool earlySecond{true};
  bool failFirst{};
  std::function<void(std::string)> response;
  std::function<void(std::exception_ptr)> completion;
  std::atomic<bool> finished{};

  void finish() {
    if (!finished.exchange(true)) completion({});
  }
};

struct EarlyBidiOrderRpc final {
  std::shared_ptr<EarlyBidiOrderControl> control;
  void write(std::string) {}
  void done() { control->finish(); }
  void cancel() { control->finish(); }
};

struct EarlyBidiOrderClient final {
  using AsyncSession = std::shared_ptr<EarlyBidiOrderRpc>;
  std::shared_ptr<EarlyBidiOrderControl> control;

  AsyncSession start(servicelib::datasink::grpc::CallOptions,
                     std::function<void(std::string)> response,
                     std::function<void(std::exception_ptr)> completion) {
    control->completion = std::move(completion);
    control->response = std::move(response);
    control->response("first");
    if (control->earlySecond) control->response("second");
    return std::make_shared<EarlyBidiOrderRpc>(EarlyBidiOrderRpc{control});
  }
};

struct EarlyBidiOrderHandler final {
  using State = int;
  std::shared_ptr<EarlyBidiOrderControl> control;

  servicelib::BeginResult<State> beginRequest(servicelib::MessageContext context, auto&) {
    return {std::move(context), 0};
  }
  void consumeMessage(servicelib::MessageContext, auto&, State&,
                      const std::string& value, auto& sender, auto) {
    sender.send(value);
  }
  void handleResponse(servicelib::MessageContext, auto&, State&,
                      const std::string& response) {
    if (response == "first") {
      control->firstEntered.Send();
      control->releaseFirst.Wait();
      if (control->failFirst) throw std::logic_error("first response failed");
    } else {
      control->secondEntered = true;
    }
    control->responses.push_back(response);
  }
  void endRequest(servicelib::MessageContext, auto&, std::exception_ptr error, State&) noexcept {
    EXPECT_EQ(static_cast<bool>(error), control->failFirst);
    control->ended.Send();
  }
};

void CheckEarlyBidiResponseOrder(bool earlySecond, bool failFirst) {
  boost::asio::io_context io;
  auto guard = boost::asio::make_work_guard(io);
  struct RestoreExecutor {
    boost::asio::any_io_executor previous = servicelib::detail::ParallelExecutorRegistry::Get();
    ~RestoreExecutor() { servicelib::detail::ParallelExecutorRegistry::Set(previous); }
  } restore;
  servicelib::detail::ParallelExecutorRegistry::Set(io.get_executor());
  TestEnvironment environment;
  TestSinkEndpointStream<std::string, std::string> stream{environment, 4};
  auto control = std::make_shared<EarlyBidiOrderControl>();
  control->earlySecond = earlySecond;
  control->failFirst = failFirst;
  servicelib::datasink::grpc::BidirectionalStreamingEndpoint<
      std::string, std::string, std::string, std::string, EarlyBidiOrderHandler,
      EarlyBidiOrderClient>
      endpoint{stream, EarlyBidiOrderHandler{control}, EarlyBidiOrderClient{control}};
  endpoint.start(servicelib::Context{});
  auto consumed = boost::asio::co_spawn(io,
      servicelib::detail::CooperativeExecution::Run([&] {
        endpoint.consume(servicelib::MessageContext{}.withStreamId("early-ordered"),
                         servicelib::Payload<std::string>::make("request"));
      }), boost::asio::use_future);

  // Drain runnable work on one worker. The first response is suspended, so a
  // later response must wait for it rather than accessing handler state early.
  io.poll();
  EXPECT_TRUE(control->firstEntered.IsReady());
  std::future<void> nextResponse;
  if (!earlySecond) {
    nextResponse = boost::asio::co_spawn(io,
        servicelib::detail::CooperativeExecution::Run([&] {
          control->response("second");
        }), boost::asio::use_future);
    io.poll();
  }
  EXPECT_FALSE(control->secondEntered);
  EXPECT_TRUE(control->responses.empty());
  control->releaseFirst.Send();
  control->finish();
  std::jthread worker([&] { io.run(); });
  EXPECT_TRUE(control->ended.WaitUntil(
      std::chrono::steady_clock::now() + std::chrono::seconds{5}));
  endpoint.stop(servicelib::Context{});
  guard.reset();
  worker.join();
  consumed.get();
  if (nextResponse.valid()) nextResponse.get();
  if (failFirst) {
    EXPECT_FALSE(control->secondEntered);
    EXPECT_TRUE(control->responses.empty());
  } else {
    EXPECT_EQ(control->responses, (std::vector<std::string>{"first", "second"}));
  }
}

TEST(GrpcDataSink, EarlyBidiResponsesRemainOrderedAcrossCooperativeWait) {
  CheckEarlyBidiResponseOrder(true, false);
}

TEST(GrpcDataSink, EarlyBidiResponseCannotBeOvertakenAfterSessionReady) {
  CheckEarlyBidiResponseOrder(false, false);
}

TEST(GrpcDataSink, EarlyBidiResponseFailureSkipsSubsequentResponses) {
  CheckEarlyBidiResponseOrder(true, true);
  CheckEarlyBidiResponseOrder(false, true);
}

}  // namespace

namespace {

struct CorrelationReuseCall final {
  std::function<void(std::string)> response;
  std::function<void(std::exception_ptr)> completion;
  std::vector<std::string> writes;
  std::atomic<bool> finished{};
  void finish() {
    if (!finished.exchange(true)) completion({});
  }
};

struct CorrelationReuseState final {
  std::vector<std::shared_ptr<CorrelationReuseCall>> calls;
  servicelib::detail::SingleUseEvent oldResponseEntered, releaseOldResponse;
  servicelib::detail::SingleUseEvent oldEndEntered, releaseOldEnd;
  bool failResponse{};
  std::function<void()> reenterEnd;
  std::function<void()> reenterResponse;
  unsigned begins{};
  std::vector<unsigned> ends;
};

struct CorrelationReuseRpc final {
  std::shared_ptr<CorrelationReuseCall> call;
  void write(std::string value) { call->writes.push_back(std::move(value)); }
  void done() { call->finish(); }
  void cancel() { call->finish(); }
};

struct CorrelationReuseClient final {
  using AsyncSession = std::shared_ptr<CorrelationReuseRpc>;
  std::shared_ptr<CorrelationReuseState> state;
  AsyncSession start(servicelib::datasink::grpc::CallOptions,
                     std::function<void(std::string)> response,
                     std::function<void(std::exception_ptr)> completion) {
    auto call = std::make_shared<CorrelationReuseCall>();
    call->response = std::move(response);
    call->completion = std::move(completion);
    state->calls.push_back(call);
    return std::make_shared<CorrelationReuseRpc>(CorrelationReuseRpc{std::move(call)});
  }
};

struct CorrelationReuseHandler final {
  using State = unsigned;
  std::shared_ptr<CorrelationReuseState> shared;
  servicelib::BeginResult<State> beginRequest(servicelib::MessageContext context, auto&) {
    EXPECT_EQ(context.streamId(), "reused-correlation");
    return {std::move(context), ++shared->begins};
  }
  void consumeMessage(servicelib::MessageContext, auto&, State&,
                      const std::string& value, auto& sender, auto) {
    sender.send(value);
  }
  void handleResponse(servicelib::MessageContext context, auto&, State& state,
                      const std::string&) {
    EXPECT_EQ(context.streamId(), "reused-correlation");
    if (state == 1) {
      if (shared->reenterResponse) shared->reenterResponse();
      shared->oldResponseEntered.Send();
      shared->releaseOldResponse.Wait();
      if (shared->failResponse) throw std::logic_error("response failed");
    }
  }
  void endRequest(servicelib::MessageContext context, auto&, std::exception_ptr error, State& state) noexcept {
    EXPECT_EQ(context.streamId(), "reused-correlation");
    EXPECT_EQ(static_cast<bool>(error), state == 1 && shared->failResponse);
    if (error) {
      try {
        std::rethrow_exception(error);
      } catch (const std::logic_error& failure) {
        EXPECT_STREQ(failure.what(), "response failed");
      } catch (...) {
        ADD_FAILURE() << "unexpected response failure";
      }
    }
    if (state == 1) {
      if (shared->reenterEnd) shared->reenterEnd();
      shared->oldEndEntered.Send();
      shared->releaseOldEnd.Wait();
    }
    shared->ends.push_back(state);
  }
};

void CheckClientStreamingActiveReservation(bool failResponse) {
  boost::asio::io_context io;
  auto guard = boost::asio::make_work_guard(io);
  struct RestoreExecutor {
    boost::asio::any_io_executor previous = servicelib::detail::ParallelExecutorRegistry::Get();
    ~RestoreExecutor() { servicelib::detail::ParallelExecutorRegistry::Set(previous); }
  } restore;
  servicelib::detail::ParallelExecutorRegistry::Set(io.get_executor());
  TestEnvironment environment;
  TestSinkEndpointStream<std::string, std::string> stream{environment, 3};
  auto state = std::make_shared<CorrelationReuseState>();
  state->failResponse = failResponse;
  servicelib::datasink::grpc::ClientStreamingEndpoint<
      std::string, std::string, std::string, std::string, CorrelationReuseHandler,
      CorrelationReuseClient>
      endpoint{stream, CorrelationReuseHandler{state}, CorrelationReuseClient{state}};
  endpoint.start(servicelib::Context{});
  state->reenterEnd = [&] {
    endpoint.consume(servicelib::MessageContext{}.withStreamId("reused-correlation"),
                     servicelib::Payload<std::string>::make("reentrant"));
  };
  state->reenterResponse = state->reenterEnd;
  auto& failures = static_cast<servicelib::testmetrics::TestMetrics&>(environment.getMetrics()).counter(
      "datasink_endpoint.events_total", {{"connector", "grpc"}, {"endpoint", "grpc-3"},
      {"protocol", "grpc"}, {"event", "begin_request_failed"}});
  auto consume = [&](std::string value) {
    return boost::asio::co_spawn(io,
        servicelib::detail::CooperativeExecution::Run([&, value = std::move(value)] {
          endpoint.consume(servicelib::MessageContext{}.withStreamId("reused-correlation"),
                           servicelib::Payload<std::string>::make(value));
        }), boost::asio::use_future);
  };
  auto first = consume("old");
  io.poll();
  EXPECT_EQ(first.wait_for(std::chrono::seconds{0}), std::future_status::ready);
  first.get();
  ASSERT_EQ(state->calls.size(), 1U);
  state->calls.front()->response("old-result");
  state->calls.front()->finish();
  io.poll();
  EXPECT_TRUE(state->oldResponseEntered.IsReady());
  EXPECT_TRUE(state->ends.empty());

  auto duringResponse = consume("during-response");
  io.poll();
  EXPECT_EQ(duringResponse.wait_for(std::chrono::seconds{0}), std::future_status::ready);
  EXPECT_EQ(state->begins, 1U);
  state->releaseOldResponse.Send();
  io.poll();
  EXPECT_TRUE(state->oldEndEntered.IsReady());
  EXPECT_TRUE(state->ends.empty());
  auto duringEnd = consume("during-end");
  io.poll();
  EXPECT_EQ(duringEnd.wait_for(std::chrono::seconds{0}), std::future_status::ready);
  EXPECT_EQ(state->begins, 1U);
  state->releaseOldEnd.Send();
  io.poll();
  EXPECT_NO_THROW(duringResponse.get());
  EXPECT_NO_THROW(duringEnd.get());
  EXPECT_EQ(failures.count(), 4);
  EXPECT_EQ(state->ends, (std::vector<unsigned>{1}));

  auto second = consume("new-first");
  io.poll();
  EXPECT_EQ(second.wait_for(std::chrono::seconds{0}), std::future_status::ready);
  second.get();
  EXPECT_EQ(state->begins, 2U);
  auto third = consume("new-second");
  io.poll();
  EXPECT_EQ(third.wait_for(std::chrono::seconds{0}), std::future_status::ready);
  third.get();
  EXPECT_EQ(state->begins, 2U);
  EXPECT_EQ(state->calls.size(), 2U);
  if (state->calls.size() >= 2) {
    EXPECT_EQ(state->calls[1]->writes,
              (std::vector<std::string>{"new-first", "new-second"}));
  }
  // Complete every call even on regression, so Stop does not hide an assertion
  // failure behind unfinished test-owned RPCs.
  for (std::size_t index = 1; index < state->calls.size(); ++index) {
    state->calls[index]->response("new-result");
    state->calls[index]->finish();
  }
  io.poll();
  endpoint.stop(servicelib::Context{});
  guard.reset();
  io.run();
  EXPECT_EQ(state->ends, (std::vector<unsigned>{1, 2}));
  EXPECT_EQ(failures.count(), 4);
}

TEST(GrpcDataSink, ClientStreamingReservesIdThroughResponseAndEndRequest) {
  CheckClientStreamingActiveReservation(false);
}

TEST(GrpcDataSink, ClientStreamingResponseFailureReleasesIdAfterEndRequest) {
  CheckClientStreamingActiveReservation(true);
}

void CheckBidiStreamingActiveReservation(bool failResponse) {
  boost::asio::io_context io;
  auto work = boost::asio::make_work_guard(io);
  struct RestoreExecutor {
    boost::asio::any_io_executor previous = servicelib::detail::ParallelExecutorRegistry::Get();
    ~RestoreExecutor() { servicelib::detail::ParallelExecutorRegistry::Set(previous); }
  } restore;
  servicelib::detail::ParallelExecutorRegistry::Set(io.get_executor());
  TestEnvironment environment;
  TestSinkEndpointStream<std::string, std::string> stream{environment, 4};
  auto state = std::make_shared<CorrelationReuseState>();
  state->failResponse = failResponse;
  servicelib::datasink::grpc::BidirectionalStreamingEndpoint<
      std::string, std::string, std::string, std::string, CorrelationReuseHandler,
      CorrelationReuseClient>
      endpoint{stream, CorrelationReuseHandler{state}, CorrelationReuseClient{state}};
  endpoint.start({});
  auto consumeDirect = [&](std::string value) {
    endpoint.consume(servicelib::MessageContext{}.withStreamId("reused-correlation"),
                     servicelib::Payload<std::string>::make(std::move(value)));
  };
  auto consume = [&](std::string value) {
    return boost::asio::co_spawn(io,
        servicelib::detail::CooperativeExecution::Run([&, value = std::move(value)] {
          consumeDirect(value);
        }), boost::asio::use_future);
  };
  state->reenterEnd = [&] { consumeDirect("reentrant-end"); };
  auto& failures = static_cast<servicelib::testmetrics::TestMetrics&>(environment.getMetrics()).counter(
      "datasink_endpoint.events_total", {{"connector", "grpc"}, {"endpoint", "grpc-4"},
      {"protocol", "grpc"}, {"event", "begin_request_failed"}});
  auto first = consume("first");
  io.poll();
  first.get();
  ASSERT_EQ(state->calls.size(), 1U);
  auto response = boost::asio::co_spawn(io,
      servicelib::detail::CooperativeExecution::Run([state, failResponse] {
        try {
          state->calls.front()->response("result");
          EXPECT_FALSE(failResponse);
        } catch (const std::logic_error& error) {
          EXPECT_TRUE(failResponse);
          EXPECT_STREQ(error.what(), "response failed");
        }
        state->calls.front()->finish();
      }), boost::asio::use_future);
  io.poll();
  EXPECT_TRUE(state->oldResponseEntered.IsReady());
  auto duringResponse = consume("during-response");
  io.poll();
  EXPECT_EQ(duringResponse.wait_for(std::chrono::seconds{0}), std::future_status::ready);
  duringResponse.get();
  EXPECT_EQ(state->begins, 1U);
  EXPECT_EQ(failures.count(), 0);
  EXPECT_EQ(state->calls.front()->writes, (std::vector<std::string>{"first", "during-response"}));
  state->releaseOldResponse.Send();
  io.poll();
  EXPECT_TRUE(state->oldEndEntered.IsReady());
  EXPECT_TRUE(state->ends.empty());
  auto duringEnd = consume("during-end");
  io.poll();
  EXPECT_EQ(duringEnd.wait_for(std::chrono::seconds{0}), std::future_status::ready);
  EXPECT_EQ(state->begins, 1U);
  EXPECT_EQ(failures.count(), 2);
  state->releaseOldEnd.Send();
  io.poll();
  EXPECT_NO_THROW(duringEnd.get());
  response.get();
  EXPECT_EQ(state->ends, (std::vector<unsigned>{1}));

  auto reused = consume("next-first");
  io.poll();
  reused.get();
  auto nextMessage = consume("next-second");
  io.poll();
  nextMessage.get();
  EXPECT_EQ(state->begins, 2U);
  EXPECT_EQ(state->calls.size(), 2U);
  if (state->calls.size() >= 2) {
    EXPECT_EQ(state->calls[1]->writes, (std::vector<std::string>{"next-first", "next-second"}));
  }
  for (std::size_t index = 1; index < state->calls.size(); ++index) {
    state->calls[index]->response("next-result");
    state->calls[index]->finish();
  }
  io.poll();
  endpoint.stop({});
  work.reset();
  io.run();
  EXPECT_EQ(state->ends, (std::vector<unsigned>{1, 2}));
  EXPECT_EQ(failures.count(), 2);
}

TEST(GrpcDataSink, BidiStreamingReservesIdThroughEndRequestWithoutRejectingOpenMessages) {
  CheckBidiStreamingActiveReservation(false);
}

TEST(GrpcDataSink, BidiStreamingResponseFailureReleasesIdAfterEndRequest) {
  CheckBidiStreamingActiveReservation(true);
}

struct FailedStreamingStartControl {
  unsigned begins{}, opens{}, ends{};
  std::function<void()> reenter;
  servicelib::detail::SingleUseEvent endEntered, releaseEnd;
};

struct FailedStreamingStartRpc {
  void write(std::string) {}
  void done() {}
  void cancel() {}
  void WriteAndCheck(const std::string&) {}
  std::string Finish() { return {}; }
  bool Read(std::string&) { return false; }
  bool WritesDone() { return true; }
};

struct FailedStreamingStartLegacyClient {
  std::shared_ptr<FailedStreamingStartControl> control;
  FailedStreamingStartRpc operator()(servicelib::datasink::grpc::CallOptions) {
    ++control->opens;
    throw std::logic_error("start failed");
  }
};

struct FailedStreamingStartAsyncClient {
  using AsyncSession = std::shared_ptr<FailedStreamingStartRpc>;
  std::shared_ptr<FailedStreamingStartControl> control;
  AsyncSession start(servicelib::datasink::grpc::CallOptions,
                     std::function<void(std::string)>,
                     std::function<void(std::exception_ptr)>) {
    ++control->opens;
    throw std::logic_error("start failed");
  }
};

struct FailedStreamingStartHandler {
  using State = unsigned;
  std::shared_ptr<FailedStreamingStartControl> control;
  servicelib::BeginResult<State> beginRequest(servicelib::MessageContext context, auto&) {
    return {std::move(context), ++control->begins};
  }
  void consumeMessage(servicelib::MessageContext, auto&, State&,
                      const std::string&, auto&, auto) {
    ADD_FAILURE() << "ConsumeMessage ran after RPC creation failed";
  }
  void handleResponse(servicelib::MessageContext, auto&, State&, const std::string&) {
    ADD_FAILURE() << "HandleResponse ran after RPC creation failed";
  }
  void endRequest(servicelib::MessageContext, auto&, std::exception_ptr error, State& state) {
    ASSERT_NE(error, nullptr);
    try {
      std::rethrow_exception(error);
    } catch (const std::logic_error& failure) {
      EXPECT_STREQ(failure.what(), "start failed");
    } catch (...) {
      ADD_FAILURE() << "wrong creation error passed to EndRequest";
    }
    if (state == 1) {
      control->reenter();
      control->endEntered.Send();
      control->releaseEnd.Wait();
    }
    ++control->ends;
  }
};

template <bool Bidi, bool Async>
void CheckFailedStreamingStartReservation() {
  boost::asio::io_context io;
  auto work = boost::asio::make_work_guard(io);
  struct RestoreExecutor {
    boost::asio::any_io_executor previous = servicelib::detail::ParallelExecutorRegistry::Get();
    ~RestoreExecutor() { servicelib::detail::ParallelExecutorRegistry::Set(previous); }
  } restore;
  servicelib::detail::ParallelExecutorRegistry::Set(io.get_executor());
  TestEnvironment environment;
  TestSinkEndpointStream<std::string, std::string> stream{environment, Bidi ? 4 : 3};
  auto control = std::make_shared<FailedStreamingStartControl>();
  using Client = std::conditional_t<Async, FailedStreamingStartAsyncClient, FailedStreamingStartLegacyClient>;
  using Endpoint = std::conditional_t<Bidi,
      servicelib::datasink::grpc::BidirectionalStreamingEndpoint<
          std::string, std::string, std::string, std::string, FailedStreamingStartHandler, Client>,
      servicelib::datasink::grpc::ClientStreamingEndpoint<
          std::string, std::string, std::string, std::string, FailedStreamingStartHandler, Client>>;
  Endpoint endpoint{stream, FailedStreamingStartHandler{control}, Client{control}};
  endpoint.start({});
  auto consumeDirect = [&] {
    endpoint.consume(servicelib::MessageContext{}.withStreamId("failed-start"),
                     servicelib::Payload<std::string>::make("request"));
  };
  control->reenter = consumeDirect;
  auto consume = [&] {
    return boost::asio::co_spawn(io,
        servicelib::detail::CooperativeExecution::Run(consumeDirect), boost::asio::use_future);
  };
  auto& failures = static_cast<servicelib::testmetrics::TestMetrics&>(environment.getMetrics()).counter(
      "datasink_endpoint.events_total", {{"connector", "grpc"}, {"endpoint", Bidi ? "grpc-4" : "grpc-3"},
      {"protocol", "grpc"}, {"event", "begin_request_failed"}});
  auto first = consume();
  io.poll();
  EXPECT_TRUE(control->endEntered.IsReady());
  EXPECT_EQ(control->ends, 0U);
  auto duringEnd = consume();
  io.poll();
  EXPECT_EQ(duringEnd.wait_for(std::chrono::seconds{0}), std::future_status::ready);
  EXPECT_EQ(control->begins, 1U);
  EXPECT_EQ(control->opens, 1U);
  EXPECT_EQ(failures.count(), 2);
  control->releaseEnd.Send();
  io.poll();
  EXPECT_NO_THROW(first.get());
  EXPECT_NO_THROW(duringEnd.get());
  EXPECT_EQ(control->ends, 1U);
  auto retry = consume();
  io.poll();
  EXPECT_NO_THROW(retry.get());
  endpoint.stop({});
  work.reset();
  io.run();
  EXPECT_EQ(control->begins, 2U);
  EXPECT_EQ(control->opens, 2U);
  EXPECT_EQ(control->ends, 2U);
  EXPECT_EQ(failures.count(), 2);
}

TEST(GrpcDataSink, ClientStreamingAsyncStartFailureReservesIdThroughEnd) {
  CheckFailedStreamingStartReservation<false, true>();
}
TEST(GrpcDataSink, ClientStreamingLegacyStartFailureReservesIdThroughEnd) {
  CheckFailedStreamingStartReservation<false, false>();
}
TEST(GrpcDataSink, BidiStreamingAsyncStartFailureReservesIdThroughEnd) {
  CheckFailedStreamingStartReservation<true, true>();
}
TEST(GrpcDataSink, BidiStreamingLegacyStartFailureReservesIdThroughEnd) {
  CheckFailedStreamingStartReservation<true, false>();
}

}  // namespace

namespace {

class CoordinatedStopEndpoint final : public servicelib::datasink::grpc::IEndpoint {
 public:
  CoordinatedStopEndpoint(int endpointId, std::function<void()> stop)
      : id_(endpointId), stop_(std::move(stop)) {}
  int id() const noexcept override { return id_; }
  void start(servicelib::Context) override {}
  void stop(servicelib::Context) override { stop_(); }
 private:
  int id_;
  std::function<void()> stop_;
};

TEST(GrpcDataSink, StartsEveryEndpointStopBeforeWaitingForOne) {
  TestEnvironment environment;
  TestSinkEndpointStream<std::string, std::string> stream{environment, 1};
  auto sink = servicelib::datasink::grpc::DataSink::make(stream);
  servicelib::detail::SingleUseEvent secondStopping;
  std::atomic<bool> firstObservedSecond{};
  std::atomic<unsigned> stopped{};
  sink->addEndpoint(std::make_shared<CoordinatedStopEndpoint>(1, [&] {
    firstObservedSecond = secondStopping.WaitUntil(
        std::chrono::steady_clock::now() + std::chrono::seconds{1});
    stopped.fetch_add(1);
  }));
  sink->addEndpoint(std::make_shared<CoordinatedStopEndpoint>(2, [&] {
    secondStopping.Send();
    stopped.fetch_add(1);
  }));
  sink->start(servicelib::Context{});
  sink->stop(servicelib::Context{});
  EXPECT_TRUE(firstObservedSecond.load());
  EXPECT_EQ(stopped.load(), 2U);
}

TEST(GrpcDataSink, StopsAllEndpointsBeforePropagatingFailure) {
  TestEnvironment environment;
  TestSinkEndpointStream<std::string, std::string> stream{environment, 1};
  auto sink = servicelib::datasink::grpc::DataSink::make(stream);
  servicelib::detail::SingleUseEvent firstStopping;
  std::atomic<bool> secondStopped{};
  sink->addEndpoint(std::make_shared<CoordinatedStopEndpoint>(1, [&] {
    firstStopping.Send();
    throw std::logic_error("endpoint stop failed");
  }));
  sink->addEndpoint(std::make_shared<CoordinatedStopEndpoint>(2, [&] {
    secondStopped = firstStopping.WaitUntil(
        std::chrono::steady_clock::now() + std::chrono::seconds{1});
  }));
  sink->start(servicelib::Context{});
  EXPECT_THROW(sink->stop(servicelib::Context{}), std::logic_error);
  EXPECT_TRUE(secondStopped.load());
}

}  // namespace

namespace {

struct ActiveRequestProbe {
  std::atomic<unsigned> begins{};
  std::function<void(unsigned, std::exception_ptr)> end;
};

struct ActiveRequestHandler {
  using State = unsigned;
  using StreamContext = servicelib::SourceStreamContext<
      std::string, std::string, std::exception_ptr>;
  std::shared_ptr<ActiveRequestProbe> probe;

  servicelib::BeginResult<State> beginRequest(
      servicelib::MessageContext context, StreamContext&) {
    return {std::move(context), ++probe->begins};
  }
  void endRequest(servicelib::MessageContext, StreamContext&,
                  std::exception_ptr error, State& state) {
    if (probe->end) probe->end(state, error);
  }
  std::string getMessageId(servicelib::MessageContext, StreamContext&,
                          State&, const std::string&) {
    return "message";
  }
};

class ActiveRequestEndpoint final
    : public servicelib::datasource::grpc::Endpoint<
          std::string, std::string, std::string, std::string,
          ActiveRequestHandler> {
 public:
  using Base = servicelib::datasource::grpc::Endpoint<
      std::string, std::string, std::string, std::string,
      ActiveRequestHandler>;
  ActiveRequestEndpoint(TestEnvironment& environment, bool hasResult,
                        std::shared_ptr<ActiveRequestProbe> probe,
                        int endpointId = 1,
                        servicelib::api::GrpcMethodType method =
                            servicelib::api::GrpcMethodType::kNoStreaming)
      : Base(environment, endpointId, method,
             ActiveRequestHandler{std::move(probe)},
             [](servicelib::MessageContext, servicelib::Payload<std::string>) {},
             hasResult) {}

  std::shared_ptr<Request> open(std::string id) {
    return begin(servicelib::MessageContext{}.withStreamId(std::move(id)),
                 std::make_shared<servicelib::datasource::grpc::Sender<std::string>>(
                     [](std::string) {}), {});
  }
};

TEST(GrpcDataSource, RejectsDuplicateActiveIdWithAndWithoutResults) {
  for (const bool hasResult : {false, true}) {
    SCOPED_TRACE(hasResult);
    TestEnvironment environment;
    auto probe = std::make_shared<ActiveRequestProbe>();
    ActiveRequestEndpoint endpoint{environment, hasResult, probe};
    auto first = endpoint.open("active-id");
    endpoint.activate(first);
    auto duplicate = endpoint.open("active-id");
    EXPECT_THROW(endpoint.activate(duplicate), servicelib::store::DuplicateKeyError);
    auto rejection = std::make_exception_ptr(std::logic_error("duplicate"));
    endpoint.finish(duplicate, rejection);
    // Cleaning up the rejected request must not release the original ID.
    auto another = endpoint.open("active-id");
    EXPECT_THROW(endpoint.activate(another), servicelib::store::DuplicateKeyError);
    endpoint.finish(another, rejection);
    std::exception_ptr success;
    endpoint.finish(first, success);
    auto reused = endpoint.open("active-id");
    EXPECT_NO_THROW(endpoint.activate(reused));
    endpoint.finish(reused, success);
  }
}

TEST(GrpcDataSource, ActiveIdRemainsReservedUntilEndRequestReturns) {
  for (const bool hasResult : {false, true}) {
    SCOPED_TRACE(hasResult);
    TestEnvironment environment;
    auto probe = std::make_shared<ActiveRequestProbe>();
    servicelib::detail::SingleUseEvent entered;
    servicelib::detail::SingleUseEvent release;
    probe->end = [&](unsigned state, std::exception_ptr) {
      if (state == 1) {
        entered.Send();
        release.Wait();
      }
    };
    ActiveRequestEndpoint endpoint{environment, hasResult, probe};
    auto first = endpoint.open("finishing-id");
    endpoint.activate(first);
    auto finishing = std::async(std::launch::async, [&] {
      std::exception_ptr error;
      endpoint.finish(first, error);
    });
    EXPECT_TRUE(entered.WaitUntil(
        std::chrono::steady_clock::now() + std::chrono::seconds{5}));
    auto duplicate = endpoint.open("finishing-id");
    EXPECT_THROW(endpoint.activate(duplicate), servicelib::store::DuplicateKeyError);
    auto rejection = std::make_exception_ptr(std::logic_error("duplicate"));
    endpoint.finish(duplicate, rejection);
    release.Send();
    finishing.get();
    auto reused = endpoint.open("finishing-id");
    EXPECT_NO_THROW(endpoint.activate(reused));
    std::exception_ptr success;
    endpoint.finish(reused, success);
  }
}

TEST(GrpcDataSource, EndRequestExceptionReleasesActiveId) {
  TestEnvironment environment;
  auto probe = std::make_shared<ActiveRequestProbe>();
  probe->end = [](unsigned state, std::exception_ptr) {
    if (state == 1) throw std::logic_error("end failed");
  };
  ActiveRequestEndpoint endpoint{environment, true, probe};
  auto first = endpoint.open("failed-end-id");
  endpoint.activate(first);
  std::exception_ptr success;
  EXPECT_THROW(endpoint.finish(first, success), std::logic_error);
  auto reused = endpoint.open("failed-end-id");
  EXPECT_NO_THROW(endpoint.activate(reused));
  endpoint.finish(reused, success);
}

TEST(GrpcDataSource, CompetingOpensAdmitExactlyOneRequest) {
  for (const bool hasResult : {false, true}) {
    SCOPED_TRACE(hasResult);
    TestEnvironment environment;
    auto probe = std::make_shared<ActiveRequestProbe>();
    ActiveRequestEndpoint endpoint{environment, hasResult, probe};
    std::vector<std::shared_ptr<ActiveRequestEndpoint::Request>> requests;
    std::vector<std::future<bool>> opens;
    servicelib::detail::SingleUseEvent start;
    for (unsigned i = 0; i < 8; ++i) {
      requests.push_back(endpoint.open("racing-id"));
      opens.push_back(std::async(std::launch::async,
          [&, request = requests.back()] {
            start.Wait();
            try {
              endpoint.activate(request);
              return true;
            } catch (const servicelib::store::DuplicateKeyError&) {
              return false;
            }
          }));
    }
    start.Send();
    unsigned admitted = 0;
    for (auto& open : opens) admitted += open.get() ? 1U : 0U;
    EXPECT_EQ(admitted, 1U);
    for (const auto& request : requests) {
      std::exception_ptr error;
      endpoint.finish(request, error);
    }
  }
}

TEST(GrpcDataSource, ActiveIdsAreScopedToEndpointsForAllGrpcModes) {
  for (const bool hasResult : {false, true}) {
    TestEnvironment environment;
    auto probe = std::make_shared<ActiveRequestProbe>();
    const std::vector<servicelib::api::GrpcMethodType> methods{
        servicelib::api::GrpcMethodType::kNoStreaming,
        servicelib::api::GrpcMethodType::kServerStreaming,
        servicelib::api::GrpcMethodType::kClientStreaming,
        servicelib::api::GrpcMethodType::kBidirectionalStreaming};
    std::vector<std::unique_ptr<ActiveRequestEndpoint>> endpoints;
    std::vector<std::shared_ptr<ActiveRequestEndpoint::Request>> requests;
    for (std::size_t index = 0; index < methods.size(); ++index) {
      endpoints.push_back(std::make_unique<ActiveRequestEndpoint>(
          environment, hasResult, probe, static_cast<int>(index + 1),
          methods[index]));
      requests.push_back(endpoints.back()->open("shared-id"));
      EXPECT_NO_THROW(endpoints.back()->activate(requests.back()));
      auto duplicate = endpoints.back()->open("shared-id");
      EXPECT_THROW(endpoints.back()->activate(duplicate),
                   servicelib::store::DuplicateKeyError);
      auto rejection = std::make_exception_ptr(servicelib::store::DuplicateKeyError{});
      endpoints.back()->finish(duplicate, rejection);
    }
    for (std::size_t index = 0; index < endpoints.size(); ++index) {
      std::exception_ptr error;
      endpoints[index]->finish(requests[index], error);
    }
  }
}

TEST(GrpcDataSource, ResultsDuringEndRequestAreDiscardedWithoutWaiting) {
  TestEnvironment environment;
  auto probe = std::make_shared<ActiveRequestProbe>();
  ActiveRequestEndpoint endpoint{environment, true, probe};
  auto first = endpoint.open("late-result-id");
  endpoint.activate(first);
  unsigned callbacks = 0;
  ActiveRequestEndpoint::ResultCtx{first}.setResultCallback("message",
      [&](auto, auto&, auto&, const auto&, auto&) {
        ++callbacks;
        return false;
      });
  endpoint.consumeResult(first->context,
      servicelib::Payload<std::string>::make(std::string{"before"}));
  EXPECT_EQ(callbacks, 1U);
  probe->end = [&](unsigned, std::exception_ptr) {
    endpoint.consumeResult(first->context,
        servicelib::Payload<std::string>::make(std::string{"during"}));
  };
  std::exception_ptr error;
  endpoint.finish(first, error);
  endpoint.consumeResult(first->context,
      servicelib::Payload<std::string>::make(std::string{"after"}));
  EXPECT_EQ(callbacks, 1U);
}

}  // namespace

namespace {

struct EndResponseHandler {
  using StreamContext = servicelib::SourceStreamContext<
      std::string, std::string, std::exception_ptr>;
  using Sender = servicelib::datasource::grpc::Sender<std::string>;
  struct State {
    Sender* sender{};
  };
  using ResultContext = servicelib::datasource::grpc::ResultContext<
      State, std::string, std::string, std::string>;

  servicelib::BeginResult<State> beginRequest(
      servicelib::MessageContext context, StreamContext&) {
    return {std::move(context), State{}};
  }
  void consumeMessage(servicelib::MessageContext, StreamContext&, State& state,
                      const std::string&, ResultContext, Sender& sender) {
    state.sender = &sender;
  }
  void eof(servicelib::MessageContext, StreamContext&, State&) {}
  void endRequest(servicelib::MessageContext, StreamContext&,
                  std::exception_ptr error, State& state) {
    if (!error && state.sender) state.sender->send("from-end");
  }
};

struct EndResponseTransport {
  unsigned remaining{2};
  std::vector<std::string> responses;
  bool Read(std::string& value) {
    if (remaining == 0) return false;
    --remaining;
    value = "request";
    return true;
  }
  void Write(std::string value) { responses.push_back(std::move(value)); }
};

TEST(GrpcDataSource, GrpcModesRespectEndRequestResponseSemanticsWithoutResultStream) {
  TestEnvironment environment;
  const auto context = servicelib::MessageContext{}.withStreamId("end-response");
  const auto output = [](servicelib::MessageContext,
                         servicelib::Payload<std::string>) {};
  {
    servicelib::datasource::grpc::NoStreamingEndpoint<
        std::string, std::string, std::string, std::string, EndResponseHandler>
        endpoint{environment, 1, EndResponseHandler{}, output, false};
    EXPECT_EQ(endpoint.handle(context, std::string{"request"}), "from-end");
  }
  {
    servicelib::datasource::grpc::ServerStreamingEndpoint<
        std::string, std::string, std::string, std::string, EndResponseHandler>
        endpoint{environment, 2, EndResponseHandler{}, output, false};
    EndResponseTransport transport;
    endpoint.handle(context, std::string{"request"}, transport);
    EXPECT_EQ(transport.responses, (std::vector<std::string>{"from-end"}));
  }
  {
    servicelib::datasource::grpc::ClientStreamingEndpoint<
        std::string, std::string, std::string, std::string, EndResponseHandler>
        endpoint{environment, 3, EndResponseHandler{}, output, false};
    EndResponseTransport transport;
    EXPECT_EQ(endpoint.handle(context, transport), "");
  }
  {
    servicelib::datasource::grpc::BidirectionalStreamingEndpoint<
        std::string, std::string, std::string, std::string, EndResponseHandler>
        endpoint{environment, 4, EndResponseHandler{}, output, false};
    EndResponseTransport transport;
    endpoint.handle(context, transport);
    EXPECT_EQ(transport.responses, (std::vector<std::string>{"from-end"}));
  }
}

TEST(GrpcDataSource, EndRequestMaySendAndAlwaysClosesSenderAfterwards) {
  for (const bool failEnd : {false, true}) {
    SCOPED_TRACE(failEnd);
    TestEnvironment environment;
    auto probe = std::make_shared<ActiveRequestProbe>();
    ActiveRequestEndpoint endpoint{environment, false, probe};
    std::vector<std::string> responses;
    auto sender = std::make_shared<servicelib::datasource::grpc::Sender<std::string>>(
        [&](std::string value) { responses.push_back(std::move(value)); });
    auto request = endpoint.begin(
        servicelib::MessageContext{}.withStreamId("end-sender"), sender, {});
    endpoint.activate(request);
    probe->end = [&](unsigned, std::exception_ptr) {
      sender->send("from-end");
      if (failEnd) throw std::logic_error("end failed after sending");
    };
    std::exception_ptr error;
    if (failEnd) {
      EXPECT_THROW(endpoint.finish(request, error), std::logic_error);
    } else {
      EXPECT_NO_THROW(endpoint.finish(request, error));
    }
    EXPECT_EQ(responses, (std::vector<std::string>{"from-end"}));
    EXPECT_THROW(sender->send("late"), std::runtime_error);
    probe->end = {};
    auto reused = endpoint.open("end-sender");
    EXPECT_NO_THROW(endpoint.activate(reused));
    endpoint.finish(reused, error);
  }
}

}  // namespace

#include <servicelib/runtime/detail/grpc_streaming.hpp>

namespace {

struct EndAsyncResponseTransport {
  using Request = std::string;
  using Response = std::string;
  unsigned remaining{2};
  std::vector<std::string> responses;
  std::shared_ptr<servicelib::detail::SingleUseEvent> releaseWrite;
  servicelib::detail::SingleUseEvent writeEntered;
  bool failWrite{};

  boost::asio::awaitable<bool> read(
      Request& value, boost::asio::use_awaitable_t<>) {
    co_await boost::asio::post(boost::asio::use_awaitable);
    if (remaining == 0) co_return false;
    --remaining;
    value = "request";
    co_return true;
  }
  boost::asio::awaitable<bool> write(
      const Response& value, boost::asio::use_awaitable_t<>) {
    writeEntered.Send();
    if (releaseWrite) {
      co_await releaseWrite->AsyncWait(servicelib::MessageContext{});
    } else {
      co_await boost::asio::post(boost::asio::use_awaitable);
    }
    if (failWrite) co_return false;
    responses.push_back(value);
    co_return true;
  }
};

TEST(GrpcDataSource, CooperativeAdaptersRespectEndRequestResponseSemantics) {
  TestEnvironment environment;
  const auto context = servicelib::MessageContext{}.withStreamId("async-end");
  const auto output = [](servicelib::MessageContext,
                         servicelib::Payload<std::string>) {};
  {
    boost::asio::io_context io;
    servicelib::datasource::grpc::NoStreamingEndpoint<
        std::string, std::string, std::string, std::string, EndResponseHandler>
        endpoint{environment, 1, EndResponseHandler{}, output, false};
    const std::string request{"request"};
    auto result = boost::asio::co_spawn(
        io, endpoint.asyncHandle(context, request), boost::asio::use_future);
    io.run();
    EXPECT_EQ(result.get(), "from-end");
  }
  {
    boost::asio::io_context io;
    servicelib::datasource::grpc::ServerStreamingEndpoint<
        std::string, std::string, std::string, std::string, EndResponseHandler>
        endpoint{environment, 2, EndResponseHandler{}, output, false};
    EndAsyncResponseTransport transport;
    const std::string request{"request"};
    auto result = boost::asio::co_spawn(io,
        servicelib::grpc_transport::HandleServerStreamingSource(
            endpoint, transport, request, context), boost::asio::use_future);
    io.run();
    result.get();
    EXPECT_EQ(transport.responses, (std::vector<std::string>{"from-end"}));
  }
  {
    boost::asio::io_context io;
    servicelib::datasource::grpc::ClientStreamingEndpoint<
        std::string, std::string, std::string, std::string, EndResponseHandler>
        endpoint{environment, 3, EndResponseHandler{}, output, false};
    EndAsyncResponseTransport transport;
    auto result = boost::asio::co_spawn(io,
        servicelib::grpc_transport::HandleClientStreamingSource(
            endpoint, transport, context), boost::asio::use_future);
    io.run();
    EXPECT_EQ(result.get(), "");
  }
  {
    boost::asio::io_context io;
    servicelib::datasource::grpc::BidirectionalStreamingEndpoint<
        std::string, std::string, std::string, std::string, EndResponseHandler>
        endpoint{environment, 4, EndResponseHandler{}, output, false};
    EndAsyncResponseTransport transport;
    auto result = boost::asio::co_spawn(io,
        servicelib::grpc_transport::HandleBidirectionalStreamingSource(
            endpoint, transport, context), boost::asio::use_future);
    io.run();
    result.get();
    EXPECT_EQ(transport.responses, (std::vector<std::string>{"from-end"}));
  }
}

struct SendDuringConsumeHandler : EndResponseHandler {
  std::shared_ptr<std::atomic<bool>> sendReturned;
  void consumeMessage(servicelib::MessageContext, StreamContext&, State& state,
                      const std::string&, ResultContext, Sender& sender) {
    state.sender = &sender;
    sender.send("during-consume");
    sendReturned->store(true);
  }
};

TEST(GrpcDataSource, CooperativeStreamingSendWaitsForWriteAndPropagatesFailure) {
  for (const bool failWrite : {false, true}) {
    SCOPED_TRACE(failWrite);
    TestEnvironment environment;
    boost::asio::io_context io;
    auto returned = std::make_shared<std::atomic<bool>>(false);
    SendDuringConsumeHandler handler;
    handler.sendReturned = returned;
    servicelib::datasource::grpc::ServerStreamingEndpoint<
        std::string, std::string, std::string, std::string, SendDuringConsumeHandler>
        endpoint{environment, 2, handler,
                 [](servicelib::MessageContext, servicelib::Payload<std::string>) {},
                 false};
    EndAsyncResponseTransport transport;
    transport.releaseWrite = std::make_shared<servicelib::detail::SingleUseEvent>();
    transport.failWrite = failWrite;
    const std::string request{"request"};
    auto result = boost::asio::co_spawn(io,
        servicelib::grpc_transport::HandleServerStreamingSource(
            endpoint, transport, request,
            servicelib::MessageContext{}.withStreamId("slow-write")),
        boost::asio::use_future);
    io.poll();
    EXPECT_TRUE(transport.writeEntered.IsReady());
    EXPECT_FALSE(returned->load());
    EXPECT_TRUE(transport.responses.empty());
    EXPECT_NE(result.wait_for(std::chrono::seconds{0}), std::future_status::ready);
    transport.releaseWrite->Send();
    io.restart();
    io.run();
    if (failWrite) {
      EXPECT_THROW(result.get(), std::runtime_error);
      EXPECT_FALSE(returned->load());
      EXPECT_TRUE(transport.responses.empty());
    } else {
      EXPECT_NO_THROW(result.get());
      EXPECT_TRUE(returned->load());
      EXPECT_EQ(transport.responses,
                (std::vector<std::string>{"during-consume", "from-end"}));
    }
  }
}

}  // namespace

#include "connector_test.grpc.pb.h"
#include <servicelib/runtime/detail/grpc_runtime.hpp>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>

namespace {

constexpr unsigned kLiveResponseCount = 128;
constexpr std::size_t kLiveResponseBytes = 256 * 1024;

struct LiveSourceProbe {
  std::atomic<unsigned> begins{};
  std::atomic<unsigned> consumes{};
  std::atomic<unsigned> sent{};
  std::promise<void> entered;
  std::promise<std::exception_ptr> ended;
};

struct LiveSourceHandler {
  using StreamContext = servicelib::SourceStreamContext<
      std::string, std::string, std::exception_ptr>;
  using Sender = servicelib::datasource::grpc::Sender<servicelib::test::EchoResponse>;
  struct State {
    unsigned ordinal{};
    Sender* sender{};
  };
  using ResultContext = servicelib::datasource::grpc::ResultContext<
      State, std::string, servicelib::test::EchoResponse, std::string>;
  std::shared_ptr<LiveSourceProbe> probe;

  servicelib::BeginResult<State> beginRequest(
      servicelib::MessageContext context, StreamContext&) {
    return {std::move(context), State{++probe->begins, nullptr}};
  }
  void consumeMessage(servicelib::MessageContext, StreamContext&, State& state,
                      const servicelib::test::EchoRequest&, ResultContext,
                      Sender& sender) {
    state.sender = &sender;
    ++probe->consumes;
    if (state.ordinal == 1) probe->entered.set_value();
    for (unsigned index = 0; index < kLiveResponseCount; ++index) {
      servicelib::test::EchoResponse response;
      response.set_value(std::string(kLiveResponseBytes,
                                     static_cast<char>('a' + index % 26)));
      sender.send(std::move(response));
      ++probe->sent;
    }
  }
  void eof(servicelib::MessageContext, StreamContext&, State&) {}
  void endRequest(servicelib::MessageContext, StreamContext&,
                  std::exception_ptr error, State& state) {
    if (state.ordinal != 1) return;
    if (!error && state.sender) {
      servicelib::test::EchoResponse response;
      response.set_value("end-response");
      try {
        state.sender->send(std::move(response));
      } catch (...) {
        probe->ended.set_value(std::current_exception());
        throw;
      }
    }
    probe->ended.set_value(error);
  }
};

class LiveSourceHarness final {
  // Declared before runtime-owning members so restoration happens only after
  // their destruction, including constructor failure paths.
  struct RestoreExecutors final {
    boost::asio::any_io_executor parallel;
    boost::asio::any_io_executor blocking;
    RestoreExecutors() {
      try { parallel = servicelib::detail::ParallelExecutorRegistry::Get(); }
      catch (const std::logic_error&) {}
      try { blocking = servicelib::detail::BlockingExecutorRegistry::Get(); }
      catch (const std::logic_error&) {}
    }
    ~RestoreExecutors() {
      if (parallel) servicelib::detail::ParallelExecutorRegistry::Set(parallel);
      else servicelib::detail::ParallelExecutorRegistry::Clear();
      if (blocking) servicelib::detail::BlockingExecutorRegistry::Set(blocking);
      else servicelib::detail::BlockingExecutorRegistry::Clear();
    }
  } restoreExecutors_;

 public:
  using Request = servicelib::test::EchoRequest;
  using Response = servicelib::test::EchoResponse;
  using ServerEndpoint = servicelib::datasource::grpc::ServerStreamingEndpoint<
      Request, Response, std::string, std::string, LiveSourceHandler>;
  using BidiEndpoint = servicelib::datasource::grpc::BidirectionalStreamingEndpoint<
      Request, Response, std::string, std::string, LiveSourceHandler>;

  LiveSourceHarness() {
    std::unique_ptr<grpc::ServerCompletionQueue> completionQueue;
    try {
    grpc::ServerBuilder builder;
    int port = 0;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
    builder.RegisterService(&service_);
    completionQueue = builder.AddCompletionQueue();
    server_ = builder.BuildAndStart();
    if (!server_ || port == 0) throw std::runtime_error("live gRPC server failed to start");
    runtime = std::make_unique<servicelib::async::GrpcRuntime>(
        servicelib::async::GrpcRuntime::Options{.workers = 1},
        std::move(completionQueue));
    runtime->Start();
    const auto output = [](servicelib::MessageContext,
                           servicelib::Payload<std::string>) {};
    serverEndpoint_ = std::make_unique<ServerEndpoint>(
        environment_, 2, LiveSourceHandler{probe}, output, false);
    bidiEndpoint_ = std::make_unique<BidiEndpoint>(
        environment_, 4, LiveSourceHandler{probe}, output, false);
    servicelib::grpc_transport::RegisterServerStreamingSource<
        &servicelib::test::ConnectorTest::AsyncService::RequestServerStreaming>(
        runtime->grpcContext(), service_,
        [this](auto& rpc, const Request& value,
               servicelib::MessageContext context) -> boost::asio::awaitable<void> {
          co_await servicelib::grpc_transport::HandleServerStreamingSource(
              *serverEndpoint_, rpc, value, std::move(context));
        }, runtime->executor());
    servicelib::grpc_transport::RegisterBidirectionalStreamingSource<
        &servicelib::test::ConnectorTest::AsyncService::RequestBidirectionalStreaming>(
        runtime->grpcContext(), service_,
        [this](auto& rpc,
               servicelib::MessageContext context) -> boost::asio::awaitable<void> {
          co_await servicelib::grpc_transport::HandleBidirectionalStreamingSource(
              *bidiEndpoint_, rpc, std::move(context));
        }, runtime->executor());
    stub = servicelib::test::ConnectorTest::NewStub(grpc::CreateChannel(
        "127.0.0.1:" + std::to_string(port), grpc::InsecureChannelCredentials()));
    } catch (...) {
      shutdown();
      if (completionQueue) {
        completionQueue->Shutdown();
        void* tag = nullptr;
        bool ok = false;
        while (completionQueue->Next(&tag, &ok)) {}
      }
      throw;
    }
  }

  ~LiveSourceHarness() { shutdown(); }

  std::shared_ptr<LiveSourceProbe> probe = std::make_shared<LiveSourceProbe>();
  std::unique_ptr<servicelib::async::GrpcRuntime> runtime;
  std::unique_ptr<servicelib::test::ConnectorTest::Stub> stub;

 private:
  void shutdown() noexcept {
    if (server_) server_->Shutdown(
        std::chrono::system_clock::now() + std::chrono::seconds{1});
    if (runtime) {
      runtime->Stop();
      runtime->Join();
    }
    serverEndpoint_.reset();
    bidiEndpoint_.reset();
    stub.reset();
    server_.reset();
    runtime.reset();
  }
  TestEnvironment environment_;
  servicelib::test::ConnectorTest::AsyncService service_;
  std::unique_ptr<grpc::Server> server_;
  std::unique_ptr<ServerEndpoint> serverEndpoint_;
  std::unique_ptr<BidiEndpoint> bidiEndpoint_;
};

void CheckLiveSourceFlowControl(bool bidi, bool cancel) {
  LiveSourceHarness harness;
  auto entered = harness.probe->entered.get_future();
  auto ended = harness.probe->ended.get_future();
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds{15});
  context.AddMetadata("x-stream-id", "live-active-id");
  LiveSourceHarness::Request request;
  request.set_value("request");
  std::unique_ptr<grpc::ClientReader<LiveSourceHarness::Response>> reader;
  std::unique_ptr<grpc::ClientReaderWriter<LiveSourceHarness::Request,
                                          LiveSourceHarness::Response>> duplex;
  if (bidi) {
    duplex = harness.stub->BidirectionalStreaming(&context);
    ASSERT_TRUE(duplex->Write(request));
    ASSERT_TRUE(duplex->WritesDone());
  } else {
    reader = harness.stub->ServerStreaming(&context, request);
  }
  ASSERT_EQ(entered.wait_for(std::chrono::seconds{3}), std::future_status::ready);

  auto sibling = std::make_shared<std::promise<void>>();
  auto siblingReady = sibling->get_future();
  boost::asio::post(harness.runtime->executor(), [sibling] { sibling->set_value(); });
  EXPECT_EQ(siblingReady.wait_for(std::chrono::seconds{1}), std::future_status::ready);
  EXPECT_EQ(ended.wait_for(std::chrono::milliseconds{50}), std::future_status::timeout);
  EXPECT_LT(harness.probe->sent.load(), kLiveResponseCount);

  // The first RPC is still alive and its peer has not read its responses.
  // A duplicate must fail before invoking ConsumeMessage in the same endpoint.
  {
    grpc::ClientContext duplicateContext;
    duplicateContext.set_deadline(
        std::chrono::system_clock::now() + std::chrono::seconds{3});
    duplicateContext.AddMetadata("x-stream-id", "live-active-id");
    LiveSourceHarness::Response unexpected;
    grpc::Status status;
    if (bidi) {
      auto duplicate = harness.stub->BidirectionalStreaming(&duplicateContext);
      static_cast<void>(duplicate->Write(request));
      static_cast<void>(duplicate->WritesDone());
      EXPECT_FALSE(duplicate->Read(&unexpected));
      status = duplicate->Finish();
    } else {
      auto duplicate = harness.stub->ServerStreaming(&duplicateContext, request);
      EXPECT_FALSE(duplicate->Read(&unexpected));
      status = duplicate->Finish();
    }
    EXPECT_EQ(status.error_code(), grpc::StatusCode::INTERNAL);
    EXPECT_NE(status.error_message().find("duplicate key"), std::string::npos);
    EXPECT_EQ(harness.probe->consumes.load(), 1U);
  }

  if (cancel) context.TryCancel();
  LiveSourceHarness::Response response;
  unsigned received = 0;
  while (bidi ? duplex->Read(&response) : reader->Read(&response)) {
    if (!cancel) {
      if (received < kLiveResponseCount) {
        ASSERT_EQ(response.value().size(), kLiveResponseBytes);
        EXPECT_EQ(response.value(), std::string(
            kLiveResponseBytes, static_cast<char>('a' + received % 26)));
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
      } else {
        EXPECT_EQ(response.value(), "end-response");
      }
    }
    ++received;
  }
  const auto status = bidi ? duplex->Finish() : reader->Finish();
  ASSERT_EQ(ended.wait_for(std::chrono::seconds{3}), std::future_status::ready);
  const auto endError = ended.get();
  if (cancel) {
    EXPECT_EQ(status.error_code(), grpc::StatusCode::CANCELLED);
    EXPECT_TRUE(static_cast<bool>(endError));
    EXPECT_LT(harness.probe->sent.load(), kLiveResponseCount);
  } else {
    EXPECT_TRUE(status.ok()) << status.error_message();
    EXPECT_FALSE(static_cast<bool>(endError));
    EXPECT_EQ(received, kLiveResponseCount + 1);
    EXPECT_EQ(harness.probe->sent.load(), kLiveResponseCount);
  }
}

TEST(GrpcDataSourceLive, ServerStreamingSlowReaderAndDuplicateId) {
  CheckLiveSourceFlowControl(false, false);
}
TEST(GrpcDataSourceLive, BidiSlowReaderAndDuplicateId) {
  CheckLiveSourceFlowControl(true, false);
}
TEST(GrpcDataSourceLive, ServerStreamingCancellationDuringBackpressure) {
  CheckLiveSourceFlowControl(false, true);
}
TEST(GrpcDataSourceLive, BidiCancellationDuringBackpressure) {
  CheckLiveSourceFlowControl(true, true);
}

}  // namespace

namespace {

struct MissingResultProbe {
  std::function<void()> complete;
  std::function<void()> resultHook;
  bool eof{};
  bool ended{};
  bool endCancelled{};
};

struct MissingResultHandler {
  using State = unsigned;
  using StreamContext = servicelib::SourceStreamContext<
      std::string, std::string, std::exception_ptr>;
  using Sender = servicelib::datasource::grpc::Sender<std::string>;
  using ResultContext = servicelib::datasource::grpc::ResultContext<
      State, std::string, std::string, std::string>;
  std::shared_ptr<MissingResultProbe> probe;

  servicelib::BeginResult<State> beginRequest(
      servicelib::MessageContext context, StreamContext&) {
    return {std::move(context), 0};
  }
  void consumeMessage(servicelib::MessageContext, StreamContext&, State&,
                      const std::string&, ResultContext result, Sender&) {
    probe->complete = [result]() mutable { result.done(); };
    result.setResultCallback("message",
        [weak = std::weak_ptr<MissingResultProbe>{probe}](
            auto, auto&, auto&, const auto&, auto&) {
          if (const auto owner = weak.lock(); owner && owner->resultHook) {
            owner->resultHook();
          }
          return true;
        });
  }
  std::string getMessageId(servicelib::MessageContext, StreamContext&,
                          State&, const std::string&) { return "message"; }
  void eof(servicelib::MessageContext, StreamContext&, State&) { probe->eof = true; }
  void endRequest(servicelib::MessageContext, StreamContext&,
                  std::exception_ptr error, State&) {
    probe->ended = true;
    if (error) {
      try {
        std::rethrow_exception(error);
      } catch (const servicelib::datasource::grpc::RpcCancelledError&) {
        probe->endCancelled = true;
      } catch (...) {
      }
    }
  }
};

template <typename Start>
void CheckMissingResultCancellation(
    boost::asio::io_context& io,
    const std::shared_ptr<MissingResultProbe>& probe, Start&& start) {
  std::stop_source stop;
  auto context = servicelib::MessageContext{}
                     .withStreamId("missing-result")
                     .withStopToken(stop.get_token());
  auto result = boost::asio::co_spawn(io, start(context), boost::asio::use_future);
  io.poll();
  EXPECT_TRUE(probe->eof);
  EXPECT_FALSE(probe->ended);
  stop.request_stop();
  io.restart();
  io.poll();
  const bool cancelled =
      result.wait_for(std::chrono::seconds{0}) == std::future_status::ready;
  EXPECT_TRUE(cancelled) << "cancelled RPC still waits for a missing graph result";
  // Rescue the current implementation so a regression is an assertion failure,
  // not a hung io_context or destruction of a live cooperative stack.
  if (!cancelled && probe->complete) probe->complete();
  io.restart();
  io.run();
  EXPECT_THROW(result.get(), servicelib::datasource::grpc::RpcCancelledError);
  EXPECT_TRUE(probe->ended);
  EXPECT_TRUE(probe->endCancelled);
}

TEST(GrpcDataSource, ClientStreamingCancellationDoesNotWaitForMissingResult) {
  TestEnvironment environment;
  boost::asio::io_context io;
  auto probe = std::make_shared<MissingResultProbe>();
  servicelib::datasource::grpc::ClientStreamingEndpoint<
      std::string, std::string, std::string, std::string, MissingResultHandler>
      endpoint{environment, 3, MissingResultHandler{probe},
               [](servicelib::MessageContext, servicelib::Payload<std::string>) {}, true};
  EndAsyncResponseTransport transport;
  CheckMissingResultCancellation(io, probe, [&](servicelib::MessageContext context) {
    return servicelib::grpc_transport::HandleClientStreamingSource(
        endpoint, transport, std::move(context));
  });
}

TEST(GrpcDataSource, ServerStreamingCancellationDoesNotWaitForMissingResult) {
  TestEnvironment environment;
  boost::asio::io_context io;
  auto probe = std::make_shared<MissingResultProbe>();
  servicelib::datasource::grpc::ServerStreamingEndpoint<
      std::string, std::string, std::string, std::string, MissingResultHandler>
      endpoint{environment, 2, MissingResultHandler{probe},
               [](servicelib::MessageContext, servicelib::Payload<std::string>) {}, true};
  EndAsyncResponseTransport transport;
  const std::string request{"request"};
  CheckMissingResultCancellation(io, probe, [&](servicelib::MessageContext context) {
    return servicelib::grpc_transport::HandleServerStreamingSource(
        endpoint, transport, request, std::move(context));
  });
}

TEST(GrpcDataSource, BidiCancellationDoesNotWaitForMissingResult) {
  TestEnvironment environment;
  boost::asio::io_context io;
  auto probe = std::make_shared<MissingResultProbe>();
  servicelib::datasource::grpc::BidirectionalStreamingEndpoint<
      std::string, std::string, std::string, std::string, MissingResultHandler>
      endpoint{environment, 4, MissingResultHandler{probe},
               [](servicelib::MessageContext, servicelib::Payload<std::string>) {}, true};
  EndAsyncResponseTransport transport;
  CheckMissingResultCancellation(io, probe, [&](servicelib::MessageContext context) {
    return servicelib::grpc_transport::HandleBidirectionalStreamingSource(
        endpoint, transport, std::move(context));
  });
}

}  // namespace

namespace {

enum class StreamingWaitCase {
  kReadyBeforeCancel,
  kReadyAfterCancel,
  kDeadline,
  kAdmittedResult,
};

template <typename Endpoint, typename Start>
void CheckStreamingWaitOutcome(
    boost::asio::io_context& io, Endpoint& endpoint,
    const std::shared_ptr<MissingResultProbe>& probe, Start&& start,
    StreamingWaitCase scenario) {
  std::stop_source stop;
  auto context = servicelib::MessageContext{}
                     .withStreamId("wait-outcome")
                     .withStopToken(stop.get_token());
  if (scenario == StreamingWaitCase::kDeadline) {
    context = std::move(context).withDeadline(
        std::chrono::steady_clock::now() + std::chrono::milliseconds{20});
  }
  auto result = boost::asio::co_spawn(io, start(context), boost::asio::use_future);
  io.poll();
  EXPECT_TRUE(probe->eof);
  EXPECT_FALSE(probe->ended);
  servicelib::detail::SingleUseEvent callbackEntered;
  servicelib::detail::SingleUseEvent releaseCallback;
  std::optional<std::future<void>> callback;
  if (scenario == StreamingWaitCase::kAdmittedResult) {
    probe->resultHook = [&] {
      callbackEntered.Send();
      releaseCallback.Wait();
      probe->complete();
    };
    io.restart();
    callback.emplace(boost::asio::co_spawn(io,
        servicelib::detail::CooperativeExecution::Run([&] {
          endpoint.consumeResult(context,
              servicelib::Payload<std::string>::make(std::string{"result"}));
        }), boost::asio::use_future));
    io.poll();
    EXPECT_TRUE(callbackEntered.IsReady());
    stop.request_stop();
    io.restart();
    io.poll();
    EXPECT_FALSE(probe->ended);
    EXPECT_NE(result.wait_for(std::chrono::seconds{0}), std::future_status::ready);
    releaseCallback.Send();
  } else if (scenario == StreamingWaitCase::kReadyBeforeCancel) {
    probe->complete();
    stop.request_stop();
  } else if (scenario == StreamingWaitCase::kReadyAfterCancel) {
    stop.request_stop();
    probe->complete();
  }
  io.restart();
  io.run_for(std::chrono::milliseconds{500});
  const bool ready =
      result.wait_for(std::chrono::seconds{0}) == std::future_status::ready;
  EXPECT_TRUE(ready);
  if (!ready && probe->complete) {
    probe->complete();
    releaseCallback.Send();
    io.restart();
    io.run();
  }
  if (scenario == StreamingWaitCase::kDeadline) {
    EXPECT_THROW(result.get(), servicelib::datasource::grpc::RpcCancelledError);
    EXPECT_TRUE(probe->endCancelled);
  } else {
    EXPECT_NO_THROW(result.get());
    EXPECT_FALSE(probe->endCancelled);
  }
  if (callback) {
    EXPECT_NO_THROW(callback->get());
  }
  EXPECT_TRUE(probe->ended);
  probe->resultHook = {};
  // A completed invocation must not leave a reservation for this endpoint.
  auto request = endpoint.begin(context,
      std::make_shared<servicelib::datasource::grpc::Sender<std::string>>(
          [](std::string) {}), {});
  EXPECT_NO_THROW(endpoint.activate(request));
  std::exception_ptr error;
  endpoint.finish(request, error);
}

void CheckStreamingWaitModes(StreamingWaitCase scenario) {
  for (const int mode : {2, 3, 4}) {
    SCOPED_TRACE(mode);
    TestEnvironment environment;
    boost::asio::io_context io;
    auto probe = std::make_shared<MissingResultProbe>();
    EndAsyncResponseTransport transport;
    const auto output = [](servicelib::MessageContext,
                           servicelib::Payload<std::string>) {};
    if (mode == 2) {
      servicelib::datasource::grpc::ServerStreamingEndpoint<
          std::string, std::string, std::string, std::string, MissingResultHandler>
          endpoint{environment, mode, MissingResultHandler{probe}, output, true};
      const std::string value{"request"};
      CheckStreamingWaitOutcome(io, endpoint, probe,
          [&](servicelib::MessageContext context) {
            return servicelib::grpc_transport::HandleServerStreamingSource(
                endpoint, transport, value, std::move(context));
          }, scenario);
    } else if (mode == 3) {
      servicelib::datasource::grpc::ClientStreamingEndpoint<
          std::string, std::string, std::string, std::string, MissingResultHandler>
          endpoint{environment, mode, MissingResultHandler{probe}, output, true};
      CheckStreamingWaitOutcome(io, endpoint, probe,
          [&](servicelib::MessageContext context) {
            return servicelib::grpc_transport::HandleClientStreamingSource(
                endpoint, transport, std::move(context));
          }, scenario);
    } else {
      servicelib::datasource::grpc::BidirectionalStreamingEndpoint<
          std::string, std::string, std::string, std::string, MissingResultHandler>
          endpoint{environment, mode, MissingResultHandler{probe}, output, true};
      CheckStreamingWaitOutcome(io, endpoint, probe,
          [&](servicelib::MessageContext context) {
            return servicelib::grpc_transport::HandleBidirectionalStreamingSource(
                endpoint, transport, std::move(context));
          }, scenario);
    }
  }
}

TEST(GrpcDataSource, StreamingResultReadyBeforeCancellationWins) {
  CheckStreamingWaitModes(StreamingWaitCase::kReadyBeforeCancel);
}
TEST(GrpcDataSource, StreamingResultReadyBeforeCancellationResumesWins) {
  CheckStreamingWaitModes(StreamingWaitCase::kReadyAfterCancel);
}
TEST(GrpcDataSource, StreamingDeadlineReleasesMissingResultAndId) {
  CheckStreamingWaitModes(StreamingWaitCase::kDeadline);
}
TEST(GrpcDataSource, StreamingAdmittedResultCompletesBeforeCancelledEndRequest) {
  CheckStreamingWaitModes(StreamingWaitCase::kAdmittedResult);
}

}  // namespace

namespace {
struct RpcScopeObservations final {
  servicelib::ContextKey<int> key;
  unsigned begins{};
  unsigned ends{};
  unsigned expectedEnds{};
  std::vector<std::pair<int, int>> messages;
  std::vector<std::pair<int, int>> responses;
  std::vector<std::string> wireIds;
  servicelib::detail::SingleUseEvent ended;
};

struct RpcScopeHandler final {
  using State = int;
  std::shared_ptr<RpcScopeObservations> observed;
  servicelib::BeginResult<State> beginRequest(servicelib::MessageContext context, auto&) {
    ++observed->begins;
    const int marker = *context.localValue(observed->key);
    return {std::move(context), marker};
  }
  void consumeMessage(servicelib::MessageContext context, auto&, State& state,
                      const std::string& value, auto& sender, auto) {
    observed->messages.emplace_back(state, *context.localValue(observed->key));
    sender.send(value);
  }
  void handleResponse(servicelib::MessageContext context, auto&, State& state,
                      const std::string&) {
    observed->responses.emplace_back(state, *context.localValue(observed->key));
  }
  void endRequest(servicelib::MessageContext, auto&, std::exception_ptr error, State&) noexcept {
    EXPECT_FALSE(error);
    if (++observed->ends == observed->expectedEnds) observed->ended.Send();
  }
};

struct RpcScopeClient final {
  using AsyncSession = CorrelationReuseClient::AsyncSession;
  std::shared_ptr<CorrelationReuseState> transport;
  std::shared_ptr<RpcScopeObservations> observed;
  AsyncSession start(servicelib::datasink::grpc::CallOptions options,
                     std::function<void(std::string)> response,
                     std::function<void(std::exception_ptr)> completion) {
    observed->wireIds.emplace_back(options.context.streamId());
    return CorrelationReuseClient{transport}.start(
        std::move(options), std::move(response), std::move(completion));
  }
};

template <bool Bidi>
void CheckStreamingRpcContext() {
  boost::asio::io_context io;
  struct RestoreExecutor final {
    boost::asio::any_io_executor previous = servicelib::detail::ParallelExecutorRegistry::Get();
    ~RestoreExecutor() { servicelib::detail::ParallelExecutorRegistry::Set(previous); }
  } restore;
  servicelib::detail::ParallelExecutorRegistry::Set(io.get_executor());
  TestEnvironment environment;
  TestSinkEndpointStream<std::string, std::string> stream{environment, Bidi ? 4 : 3};
  auto transport = std::make_shared<CorrelationReuseState>();
  auto observed = std::make_shared<RpcScopeObservations>();
  using ClientEndpoint = servicelib::datasink::grpc::ClientStreamingEndpoint<
      std::string, std::string, std::string, std::string, RpcScopeHandler, RpcScopeClient>;
  using BidiEndpoint = servicelib::datasink::grpc::BidirectionalStreamingEndpoint<
      std::string, std::string, std::string, std::string, RpcScopeHandler, RpcScopeClient>;
  using Endpoint = std::conditional_t<Bidi, BidiEndpoint, ClientEndpoint>;
  Endpoint endpoint{stream, RpcScopeHandler{observed}, RpcScopeClient{transport, observed}};
  endpoint.start({});
  auto pending = boost::asio::co_spawn(io,
      servicelib::detail::CooperativeExecution::Run([&] {
        const auto consume = [&](std::string id, int marker, std::string value) {
          endpoint.consume(servicelib::MessageContext{}.withStreamId(std::move(id))
              .withLocalValue(observed->key, std::make_shared<int>(marker)),
              servicelib::Payload<std::string>::make(std::move(value)));
        };
        // Different message-local values do not implicitly open a second RPC.
        consume("shared", 1, "first");
        consume("shared", 2, "second");
        EXPECT_EQ(transport->calls.size(), 1);
        EXPECT_EQ(observed->begins, 1);
        EXPECT_EQ(observed->messages,
                  (std::vector<std::pair<int, int>>{{1, 1}, {1, 1}}));
        if (!transport->calls.empty()) {
          EXPECT_EQ(transport->calls.front()->writes,
                    (std::vector<std::string>{"first", "second"}));
        }
        consume("third", 3, "third");
        consume("fourth", 4, "fourth");
        EXPECT_EQ(transport->calls.size(), 3);
        EXPECT_EQ(observed->begins, 3);
        EXPECT_EQ(observed->messages,
                  (std::vector<std::pair<int, int>>{{1, 1}, {1, 1}, {3, 3}, {4, 4}}));
        if (observed->wireIds.size() == 3) {
          EXPECT_NE(observed->wireIds[0], observed->wireIds[1]);
          EXPECT_NE(observed->wireIds[0], observed->wireIds[2]);
          EXPECT_NE(observed->wireIds[1], observed->wireIds[2]);
        }
        observed->expectedEnds = static_cast<unsigned>(transport->calls.size());
        for (const auto& call : transport->calls) {
          call->response("reply");
          call->finish();
        }
        if (!transport->calls.empty()) observed->ended.Wait();
        EXPECT_EQ(observed->responses,
                  (std::vector<std::pair<int, int>>{{1, 1}, {3, 3}, {4, 4}}));
        endpoint.stop({});
      }), boost::asio::use_future);
  io.run();
  pending.get();
}
}  // namespace

TEST(GrpcDataSink, ClientStreamingMessagesPreserveRpcContext) {
  CheckStreamingRpcContext<false>();
}

TEST(GrpcDataSink, BidiStreamingMessagesPreserveRpcContext) {
  CheckStreamingRpcContext<true>();
}

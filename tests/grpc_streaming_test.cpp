#include "connector_test.grpc.pb.h"

#include <servicelib/runtime/detail/grpc_streaming.hpp>
#include <servicelib/runtime/detail/grpc_client.hpp>
#include <servicelib/runtime/detail/grpc_runtime.hpp>
#include <servicelib/datasink/grpc/serverstreaming.hpp>
#include <servicelib/transformation/streams.hpp>

#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>

#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <chrono>
#include <future>
#include <mutex>
#include <set>
#include <stop_token>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace asio = boost::asio;
using namespace std::chrono_literals;

namespace {

void Require(bool condition, const std::string& message) {
  if (!condition) throw std::runtime_error(message);
}

struct AcceptedCancellation final {
  std::promise<servicelib::MessageContext> accepted;
  servicelib::detail::SingleUseEvent release;
};

struct ServerSubStreamProbe final {
  static constexpr int calls = 16;
  servicelib::ContextKey<int> callerKey;
  std::mutex mutex;
  std::set<std::string> requestIds;
  std::atomic<int> accepted{0};
  std::promise<void> allAccepted;
  servicelib::detail::SingleUseEvent release;
};

struct MethodTranscript final {
  std::vector<std::string> responses;
  grpc::StatusCode status{grpc::StatusCode::UNKNOWN};
  std::string message;
  std::string details;
  bool eof{};

  bool operator==(const MethodTranscript&) const = default;
};

struct StreamingTranscript final {
  MethodTranscript client;
  MethodTranscript server;
  MethodTranscript bidirectional;

  bool operator==(const StreamingTranscript&) const = default;
};

MethodTranscript Transcript(grpc::Status status,
                            std::vector<std::string> responses,
                            bool eof = true) {
  return {
      .responses = std::move(responses),
      .status = status.error_code(),
      .message = status.error_message(),
      .details = status.error_details(),
      .eof = eof,
  };
}

asio::awaitable<StreamingTranscript> FrameworkStreamingTranscript(
    servicelib::async::GrpcRuntime& runtime,
    servicelib::test::ConnectorTest::Stub& stub,
    const servicelib::MessageContext& context,
    const std::vector<servicelib::test::EchoRequest>& requests) {
  StreamingTranscript transcript;
  auto client = co_await servicelib::grpc_transport::ClientStreamCall<
      &servicelib::test::ConnectorTest::Stub::PrepareAsyncClientStreaming>(
      runtime.grpcContext(), stub, context, requests);
  transcript.client = Transcript(
      std::move(client.status), {client.response.SerializeAsString()});

  auto server = co_await servicelib::grpc_transport::ServerStreamCall<
      &servicelib::test::ConnectorTest::Stub::PrepareAsyncServerStreaming>(
      runtime.grpcContext(), stub, context, requests.front());
  std::vector<std::string> serverResponses;
  for (const auto& response : server.responses) {
    serverResponses.push_back(response.SerializeAsString());
  }
  transcript.server =
      Transcript(std::move(server.status), std::move(serverResponses));

  auto bidi =
      co_await servicelib::grpc_transport::BidirectionalStreamCall<
          &servicelib::test::ConnectorTest::Stub::
              PrepareAsyncBidirectionalStreaming>(
          runtime.grpcContext(), stub, context, requests);
  std::vector<std::string> bidiResponses;
  for (const auto& response : bidi.responses) {
    bidiResponses.push_back(response.SerializeAsString());
  }
  transcript.bidirectional =
      Transcript(std::move(bidi.status), std::move(bidiResponses));
  co_return transcript;
}

StreamingTranscript NativeStreamingTranscript(
    servicelib::test::ConnectorTest::Stub& stub,
    const std::vector<servicelib::test::EchoRequest>& requests) {
  StreamingTranscript transcript;
  {
    grpc::ClientContext context;
    servicelib::test::EchoResponse response;
    auto writer = stub.ClientStreaming(&context, &response);
    for (const auto& request : requests) {
      Require(writer->Write(request), "native client-streaming write failed");
    }
    Require(writer->WritesDone(),
            "native client-streaming writes_done failed");
    transcript.client =
        Transcript(writer->Finish(), {response.SerializeAsString()});
  }
  {
    grpc::ClientContext context;
    auto reader = stub.ServerStreaming(&context, requests.front());
    servicelib::test::EchoResponse response;
    std::vector<std::string> responses;
    while (reader->Read(&response)) {
      responses.push_back(response.SerializeAsString());
    }
    transcript.server = Transcript(reader->Finish(), std::move(responses));
  }
  {
    grpc::ClientContext context;
    auto stream = stub.BidirectionalStreaming(&context);
    for (const auto& request : requests) {
      Require(stream->Write(request), "native bidirectional write failed");
    }
    Require(stream->WritesDone(), "native bidirectional writes_done failed");
    servicelib::test::EchoResponse response;
    std::vector<std::string> responses;
    while (stream->Read(&response)) {
      responses.push_back(response.SerializeAsString());
    }
    transcript.bidirectional =
        Transcript(stream->Finish(), std::move(responses));
  }
  return transcript;
}

void RequirePropagatedContext(const servicelib::MessageContext& context,
                              const std::string& name) {
  Require(context.streamId() == "streaming-1",
          name + " stream ID was not propagated");
  Require(!context.hasPriority(),
          name + " priority escaped process-local context");
  Require(context.deadline().has_value(),
          name + " deadline was not propagated");
  Require(context.samplingEnabled(),
          name + " sampling marker was not propagated");
  Require(context.trace().traceId ==
                  "4bf92f3577b34da6a3ce929d0e0e4736" &&
              context.trace().spanId == "00f067aa0ba902b7" &&
              context.trace().traceState == "vendor=value" &&
              context.trace().baggage == "tenant=acme",
          name + " W3C trace context or baggage was not propagated");
}

void CancelAccepted(std::future<servicelib::MessageContext>& accepted,
                    std::stop_source& cancellation,
                    servicelib::detail::SingleUseEvent& release,
                    const std::string& name) {
  Require(accepted.wait_for(3s) == std::future_status::ready,
          name + " handler did not accept the request");
  auto context = accepted.get();
  cancellation.request_stop();
  const auto deadline = std::chrono::steady_clock::now() + 2s;
  while (!context.cancelled() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  Require(context.cancelled(),
          name + " cancellation did not reach MessageContext");
  release.Send();
}

asio::awaitable<void> RunCancelledServerStream(
    servicelib::async::GrpcRuntime& runtime,
    servicelib::test::ConnectorTest::Stub& stub,
    servicelib::MessageContext context,
    servicelib::test::EchoRequest value,
    std::promise<servicelib::grpc_transport::StreamResult<
        servicelib::test::EchoResponse>>& completed) {
  completed.set_value(
      co_await servicelib::grpc_transport::ServerStreamCall<
          &servicelib::test::ConnectorTest::Stub::PrepareAsyncServerStreaming>(
          runtime.grpcContext(), stub, std::move(context), value));
}

asio::awaitable<void> RunCancelledClientStream(
    servicelib::async::GrpcRuntime& runtime,
    servicelib::test::ConnectorTest::Stub& stub,
    servicelib::MessageContext context,
    std::vector<servicelib::test::EchoRequest> values,
    std::promise<servicelib::grpc_transport::UnaryResult<
        servicelib::test::EchoResponse>>& completed) {
  completed.set_value(
      co_await servicelib::grpc_transport::ClientStreamCall<
          &servicelib::test::ConnectorTest::Stub::PrepareAsyncClientStreaming>(
          runtime.grpcContext(), stub, std::move(context), values));
}

asio::awaitable<void> RunCancelledBidirectionalStream(
    servicelib::async::GrpcRuntime& runtime,
    servicelib::test::ConnectorTest::Stub& stub,
    servicelib::MessageContext context,
    std::vector<servicelib::test::EchoRequest> values,
    std::promise<servicelib::grpc_transport::StreamResult<
        servicelib::test::EchoResponse>>& completed) {
  completed.set_value(
      co_await servicelib::grpc_transport::BidirectionalStreamCall<
          &servicelib::test::ConnectorTest::Stub::
              PrepareAsyncBidirectionalStreaming>(
          runtime.grpcContext(), stub, std::move(context), values));
}

void CheckWriteCompletion() {
  using Queue = servicelib::grpc_transport::detail::ClientWriteQueue<int>;
  asio::io_context io;
  auto queue = std::make_shared<Queue>();
  bool returned = false;
  auto sender = asio::co_spawn(io, servicelib::detail::CooperativeExecution::Run([&] {
    queue->push(42);
    returned = true;
  }), asio::use_future);
  auto transport = [&]() -> asio::awaitable<void> {
    auto write = co_await queue->pop();
    Require(write && write->value == 42, "queued request changed");
    co_await asio::post(asio::use_awaitable);
    Require(!returned, "Send returned before its transport write completed");
    queue->complete(write);
  };
  auto sent = asio::co_spawn(io, transport(), asio::use_future);
  io.run();
  sender.get(); sent.get();
  Require(returned, "acknowledged Send did not return");

  using OwnedQueue = servicelib::grpc_transport::detail::ClientWriteQueue<std::unique_ptr<int>>;
  auto owned = std::make_shared<OwnedQueue>();
  auto sendOwned = [&](int value) {
    try {
      owned->push(std::make_unique<int>(value));
      return false;
    } catch (const std::logic_error& error) {
      return std::string{error.what()} == "write failed";
    }
  };
  auto first = std::async(std::launch::async, sendOwned, 17);
  io.restart();
  auto pending = asio::co_spawn(io, owned->pop(), asio::use_future);
  io.run();
  auto active = pending.get();
  auto second = std::async(std::launch::async, sendOwned, 23);
  Require(first.wait_for(20ms) == std::future_status::timeout,
          "in-flight Send returned before acknowledgement");
  Require(second.wait_for(20ms) == std::future_status::timeout,
          "queued Send returned before acknowledgement");
  owned->fail(std::make_exception_ptr(std::logic_error("write failed")));
  Require(first.get() && second.get(), "write failure did not reach every sender");
  Require(active && active->value && *active->value == 17,
          "cancellation freed an in-flight write payload");
  owned->complete(active);
  Require(sendOwned(29), "subsequent Send lost the terminal transport error");

  io.restart();
  auto empty = std::make_shared<Queue>();
  std::stop_source stop;
  auto read = asio::co_spawn(io,
      empty->pop(servicelib::MessageContext{}.withStopToken(stop.get_token())),
      asio::use_future);
  auto cancel = asio::co_spawn(io, servicelib::detail::CooperativeExecution::Run([&] {
    stop.request_stop();
  }), asio::use_future);
  io.run();
  cancel.get();
  Require(!read.get(), "cancelled empty writer did not wake");
}

template <bool Bidi>
void CheckCooperativePooledWrites(
    servicelib::grpc_transport::ClientPool<servicelib::test::ConnectorTest::Stub>& pool,
    const std::vector<servicelib::test::EchoRequest>& requests) {
  asio::io_context io;
  struct RestoreExecutor final {
    asio::any_io_executor previous = servicelib::detail::ParallelExecutorRegistry::Get();
    ~RestoreExecutor() { servicelib::detail::ParallelExecutorRegistry::Set(previous); }
  } restore;
  servicelib::detail::ParallelExecutorRegistry::Set(io.get_executor());
  std::vector<std::string> values;
  auto completed = std::make_shared<std::promise<std::exception_ptr>>();
  auto result = completed->get_future();
  auto response = [&](servicelib::test::EchoResponse value) {
    Require(servicelib::detail::CooperativeExecution::Active(),
            "streaming response is not cooperative");
    servicelib::detail::CooperativeExecution::Await([] { return asio::post(asio::use_awaitable); });
    values.push_back(value.value());
  };
  auto completion = [completed](std::exception_ptr error) { completed->set_value(error); };
  auto options = servicelib::datasink::grpc::callOptions(
      servicelib::MessageContext{}.withDeadline(std::chrono::steady_clock::now() + 3s));
  auto writer = [&] {
    if constexpr (Bidi) {
      return pool.template asyncBidirectionalStreaming<
          &servicelib::test::ConnectorTest::Stub::PrepareAsyncBidirectionalStreaming,
          servicelib::test::EchoRequest, servicelib::test::EchoResponse>(options, response, completion);
    } else {
      return pool.template asyncClientStreaming<
          &servicelib::test::ConnectorTest::Stub::PrepareAsyncClientStreaming,
          servicelib::test::EchoRequest, servicelib::test::EchoResponse>(options, response, completion);
    }
  }();
  auto sent = asio::co_spawn(io, servicelib::detail::CooperativeExecution::Run([&] {
    for (const auto& request : requests) writer->write(request);
    writer->done();
  }), asio::use_future);
  std::jthread worker([&] { io.run(); });
  const bool ready = result.wait_for(5s) == std::future_status::ready;
  if (!ready) writer->cancel();
  worker.join();
  sent.get();
  Require(ready, "cooperative streaming sends did not complete on one worker");
  Require(!result.get(), "cooperative streaming RPC returned an error");
  if constexpr (Bidi) {
    Require(values == std::vector<std::string>{"bidi:a", "bidi:b"}, "bidi responses changed");
  } else {
    Require(values == std::vector<std::string>{"ab"}, "client-streaming response changed");
  }
}

struct ServerSubStreamConfig final : servicelib::config::IConfig {
  explicit ServerSubStreamConfig(std::string address) {
    connector.id = 600; connector.name = "server-substream";
    connector.address = std::move(address);
    endpoint.id = 601; endpoint.name = "server-streaming";
    endpoint.idDataConnector = 600;
    endpoint.grpcMethodType = servicelib::api::GrpcMethodType::kServerStreaming;
    endpoint.methodName = "ServerStreaming";
  }
  std::vector<const servicelib::config::ServiceConfig*> GetServices() const override { return {}; }
  std::vector<servicelib::config::StreamConfigRef> GetStreams() const override { return {}; }
  std::vector<servicelib::config::DataConnectorConfigRef> GetDataConnectors() const override { return {connector}; }
  std::vector<servicelib::config::EndpointConfigRef> GetEndpoints() const override { return {endpoint}; }
  std::vector<const servicelib::config::PoolConfig*> GetPools() const override { return {}; }
  std::vector<const servicelib::config::LinkConfig*> GetLinks() const override { return {}; }
  std::vector<const servicelib::config::ModuleConfig*> GetModules() const override { return {}; }
  std::vector<const servicelib::config::TypeConfig*> GetTypes() const override { return {}; }
  servicelib::config::GrpcDataConnectorConfig connector;
  servicelib::config::GrpcEndpointConfig endpoint;
};

using ServerSubStreamOutput = std::function<void(servicelib::MessageContext, const std::string&)>;
using ServerSubStreamInvoke = std::function<void(servicelib::MessageContext, int, ServerSubStreamOutput)>;
struct ServerSubStreamCall final {
  ServerSubStreamOutput output;
  bool ended{};
  int responses{};
  std::exception_ptr error;
};
struct ServerSubStreamTypes { template <typename> struct DataType {}; };
struct ServerSubStreamWork final {
  ServerSubStreamInvoke* invoke;
  template <typename Output>
  void operator()(servicelib::MessageContext context, servicelib::StreamBase&,
                  int& value, Output&& output) const {
    (*invoke)(std::move(context), value, [&](servicelib::MessageContext delivered, const std::string& result) {
      output.out(std::move(delivered), result);
    });
  }
};
class ServerSubStreamApp final
    : public servicelib::StreamExecutionEnvironment<ServerSubStreamApp, ServerSubStreamTypes> {
 public:
  using Entry = servicelib::SubStream<int, std::string, ServerSubStreamApp>;
  explicit ServerSubStreamApp(std::string address)
      : config(std::move(address)), snapshot(std::make_shared<servicelib::config::RuntimeConfig>(config)) {}
  std::shared_ptr<const servicelib::config::RuntimeConfig> getRuntimeConfigSnapshot() const override { return snapshot; }
  void init() {
    servicelib::config::SubStreamConfig entryConfig;
    entryConfig.id = 611; entryConfig.name = "server-substream";
    entry = servicelib::makeSubStream<int, std::string, ServerSubStreamApp>(entryConfig, *this);
    servicelib::config::MapStreamConfig workConfig;
    workConfig.id = 612; workConfig.name = "invoke-server-streaming";
    auto& result = entry->map(workConfig, servicelib::StreamType<std::string>{},
                             servicelib::StreamFunction(ServerSubStreamWork{&invoke}));
    entry->setSource(result);
    static_cast<void>(getExecutionRuntime<>());
  }
  ServerSubStreamConfig config;
  std::shared_ptr<const servicelib::config::RuntimeConfig> snapshot;
  servicelib::ContextKey<ServerSubStreamCall> callKey;
  ServerSubStreamInvoke invoke;
  std::shared_ptr<Entry> entry;
};

class ServerSubStreamSink final : public servicelib::SinkEndpointStream<int, std::string> {
 public:
  explicit ServerSubStreamSink(ServerSubStreamApp& app) : app_(app) {}
  servicelib::IServiceEnvironment& environment() const override { return app_; }
  int endpointId() const noexcept override { return 601; }
  std::size_t streamConfigId() const noexcept override { return 612; }
  void collectResult(servicelib::MessageContext context, servicelib::Payload<std::string> value) override {
    auto call = context.localValue(app_.callKey);
    Require(call && static_cast<bool>(call->output), "server-streaming lost its invocation or returned early");
    call->output(std::move(context), value.get());
  }
  void collectError(servicelib::MessageContext, servicelib::Payload<std::exception_ptr> error) override {
    std::rethrow_exception(error.get());
  }
 private:
  ServerSubStreamApp& app_;
};

struct ServerSubStreamHandler final {
  using State = std::shared_ptr<ServerSubStreamCall>;
  servicelib::ContextKey<ServerSubStreamCall>* key;
  servicelib::BeginResult<State> beginRequest(servicelib::MessageContext context, auto&) {
    auto call = context.localValue(*key);
    Require(static_cast<bool>(call), "server-streaming begin lost call state");
    return {std::move(context), std::move(call)};
  }
  void consumeMessage(servicelib::MessageContext, auto&, State&, const int& value,
                      auto& sender, auto) {
    servicelib::test::EchoRequest request;
    request.set_value("substream-server-" + std::to_string(value) + ":");
    sender.send(std::move(request));
  }
  void handleResponse(servicelib::MessageContext context, auto& stream, State& state,
                      const servicelib::test::EchoResponse& response) {
    ++state->responses;
    stream.collect(std::move(context), response.value());
  }
  void endRequest(servicelib::MessageContext, auto&, std::exception_ptr error, State& state) {
    state->error = std::move(error);
    state->ended = true;
  }
};

struct ServerSubStreamClient final {
  servicelib::grpc_transport::ClientPool<servicelib::test::ConnectorTest::Stub>* pool;
  void async(servicelib::test::EchoRequest request, servicelib::datasink::grpc::CallOptions options,
             std::function<void(servicelib::test::EchoResponse)> response,
             std::function<void(std::exception_ptr)> completion) {
    pool->asyncServerStreaming<
        &servicelib::test::ConnectorTest::Stub::PrepareAsyncServerStreaming,
        servicelib::test::EchoRequest, servicelib::test::EchoResponse>(
            std::move(request), std::move(options), std::move(response), std::move(completion));
  }
};

void CheckNestedServerSubStreams(
    servicelib::grpc_transport::ClientPool<servicelib::test::ConnectorTest::Stub>& pool,
    int port, ServerSubStreamProbe& probe) {
  asio::io_context io;
  auto work = asio::make_work_guard(io);
  struct RestoreExecutor final {
    asio::any_io_executor previous = servicelib::detail::ParallelExecutorRegistry::Get();
    ~RestoreExecutor() { servicelib::detail::ParallelExecutorRegistry::Set(previous); }
  } restore;
  servicelib::detail::ParallelExecutorRegistry::Set(io.get_executor());
  ServerSubStreamApp app{"127.0.0.1:" + std::to_string(port)};
  app.init();
  ServerSubStreamSink stream{app};
  servicelib::datasink::grpc::ServerStreamingEndpoint<
      servicelib::test::EchoRequest, servicelib::test::EchoResponse, int, std::string,
      ServerSubStreamHandler, ServerSubStreamClient> endpoint{
          stream, ServerSubStreamHandler{&app.callKey}, ServerSubStreamClient{&pool}};
  endpoint.start({});
  app.invoke = [&](servicelib::MessageContext context, int value, ServerSubStreamOutput output) {
    if (value >= 1000) {
      int received = 0;
      auto collector = std::make_shared<servicelib::SubStreamCollectorFunc<std::string>>(
          [&](servicelib::MessageContext delivered, const std::string& result) {
            output(std::move(delivered), "nested:" + result);
            return ++received == 3;
          });
      app.entry->consume(std::move(context), servicelib::Payload<int>::make(value - 1000), collector);
      Require(received == 3, "nested server-streaming returned before three results");
      return;
    }
    auto call = std::make_shared<ServerSubStreamCall>();
    call->output = std::move(output);
    struct ClearOutput final {
      std::shared_ptr<ServerSubStreamCall> call;
      ~ClearOutput() { call->output = {}; }
    } clear{call};
    endpoint.consume(context.withLocalValue(app.callKey, call), servicelib::Payload<int>::make(value));
    Require(call->ended, "server-streaming Consume returned before EndRequest");
    if (call->error) std::rethrow_exception(call->error);
    Require(call->responses == 3, "server-streaming endpoint lost responses");
  };
  auto allAccepted = probe.allAccepted.get_future();
  std::vector<std::future<void>> calls;
  for (int index = 0; index < ServerSubStreamProbe::calls; ++index) {
    calls.push_back(asio::co_spawn(io, servicelib::detail::CooperativeExecution::Run([&, index] {
      std::vector<std::string> results;
      auto context = servicelib::MessageContext{}.withStreamId("server-substream-parent")
          .withLocalValue(probe.callerKey, std::make_shared<int>(index))
          .withDeadline(std::chrono::steady_clock::now() + 8s);
      auto collector = std::make_shared<servicelib::SubStreamCollectorFunc<std::string>>(
          [&](servicelib::MessageContext delivered, const std::string& result) {
            Require(delivered.streamId() == "server-substream-parent", "nested collector lost parent ID");
            Require(delivered.localValue(probe.callerKey) && *delivered.localValue(probe.callerKey) == index,
                    "server-streaming delivered another invocation's local context");
            servicelib::detail::CooperativeExecution::Await([] { return asio::post(asio::use_awaitable); });
            results.push_back(result);
            return results.size() == 3;
          });
      app.entry->consume(std::move(context), servicelib::Payload<int>::make(1000 + index), collector);
      std::vector<std::string> expected;
      for (int response = 0; response < 3; ++response)
        expected.push_back("nested:substream-server-" + std::to_string(index) + ":" + std::to_string(response));
      Require(results == expected, "nested server-streaming results crossed calls or changed order");
    }), asio::use_future));
  }
  std::jthread worker([&] { io.run(); });
  const bool accepted = allAccepted.wait_for(3s) == std::future_status::ready;
  bool returnedEarly = false;
  for (auto& call : calls) returnedEarly |= call.wait_for(0ms) == std::future_status::ready;
  probe.release.Send();
  bool completed = true;
  for (auto& call : calls) completed &= call.wait_for(10s) == std::future_status::ready;
  endpoint.stop({});
  work.reset();
  worker.join();
  Require(accepted && !returnedEarly, "nested server-streaming calls did not overlap before release");
  Require(completed, "nested server-streaming calls did not drain");
  for (auto& call : calls) call.get();
  std::lock_guard lock(probe.mutex);
  Require(probe.requestIds.size() == ServerSubStreamProbe::calls, "server-streaming reused a wire request ID");
}

}  // namespace

int main() {
  CheckWriteCompletion();
  servicelib::test::ConnectorTest::AsyncService service;
  grpc::ServerBuilder builder;
  int port{};
  builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(),
                           &port);
  builder.RegisterService(&service);
  auto queue = builder.AddCompletionQueue();
  auto server = builder.BuildAndStart();
  Require(static_cast<bool>(server), "gRPC streaming server did not start");
  Require(port > 0, "gRPC streaming server did not allocate a port");

  servicelib::async::GrpcRuntime runtime(
      {.workers = 2, .unhandledException = {}}, std::move(queue));
  AcceptedCancellation clientCancellation;
  AcceptedCancellation serverCancellation;
  AcceptedCancellation bidiCancellation;
  AcceptedCancellation shutdownCancellation;
  ServerSubStreamProbe serverSubStreams;
  std::promise<servicelib::MessageContext> clientContext;
  std::promise<servicelib::MessageContext> serverContext;
  std::promise<servicelib::MessageContext> bidiContext;
  std::atomic<bool> clientContextSent{false};
  std::atomic<bool> serverContextSent{false};
  std::atomic<bool> bidiContextSent{false};
  servicelib::grpc_transport::RegisterClientStreamingSource<
      &servicelib::test::ConnectorTest::AsyncService::RequestClientStreaming>(
      runtime.grpcContext(), service,
      [&clientCancellation, &clientContext, &clientContextSent](
         servicelib::MessageContext context,
         std::vector<servicelib::test::EchoRequest> requests)
          -> asio::awaitable<servicelib::test::EchoResponse> {
        if (!clientContextSent.exchange(true)) clientContext.set_value(context);
        if (!requests.empty() && requests.front().value() == "cancel-client") {
          clientCancellation.accepted.set_value(context);
          co_await clientCancellation.release.AsyncWait(context);
        } else if (!requests.empty() &&
                   requests.front().value() == "fail-client") {
          throw std::runtime_error("client-streaming handler failed");
        } else if (!requests.empty() &&
                   requests.front().value() == "deadline-client") {
          asio::steady_timer timer(co_await asio::this_coro::executor, 250ms);
          co_await timer.async_wait(asio::use_awaitable);
        }
        servicelib::test::EchoResponse response;
        std::string joined;
        for (const auto& request : requests) joined += request.value();
        response.set_value(std::move(joined));
        co_return response;
      }, runtime.grpcExecutor());
  servicelib::grpc_transport::RegisterServerStreamingSource<
      &servicelib::test::ConnectorTest::AsyncService::RequestServerStreaming>(
      runtime.grpcContext(), service,
      [&serverCancellation, &shutdownCancellation, &serverSubStreams, &serverContext,
       &serverContextSent](
         servicelib::MessageContext context,
         const servicelib::test::EchoRequest& request)
          -> asio::awaitable<std::vector<servicelib::test::EchoResponse>> {
        if (!serverContextSent.exchange(true)) serverContext.set_value(context);
        if (request.value().starts_with("substream-server-")) {
          Require(!context.localValue(serverSubStreams.callerKey), "local SubStream context escaped over gRPC");
          Require(context.streamId() != "server-substream-parent", "server-streaming did not create an RPC ID");
          {
            std::lock_guard lock(serverSubStreams.mutex);
            Require(serverSubStreams.requestIds.emplace(context.streamId()).second, "duplicate active server-streaming RPC ID");
          }
          if (serverSubStreams.accepted.fetch_add(1) + 1 == ServerSubStreamProbe::calls)
            serverSubStreams.allAccepted.set_value();
          co_await serverSubStreams.release.AsyncWait(context);
        }
        if (request.value() == "cancel-server") {
          serverCancellation.accepted.set_value(context);
          co_await serverCancellation.release.AsyncWait(context);
        } else if (request.value() == "shutdown-server") {
          shutdownCancellation.accepted.set_value(context);
          co_await shutdownCancellation.release.AsyncWait(context);
        } else if (request.value() == "fail-server") {
          throw std::runtime_error("server-streaming handler failed");
        } else if (request.value() == "deadline-server") {
          asio::steady_timer timer(co_await asio::this_coro::executor, 250ms);
          co_await timer.async_wait(asio::use_awaitable);
        }
        std::vector<servicelib::test::EchoResponse> responses(3);
        for (std::size_t index = 0; index < responses.size(); ++index)
          responses[index].set_value(request.value() + std::to_string(index));
        co_return responses;
      }, runtime.grpcExecutor());
  servicelib::grpc_transport::RegisterBidirectionalStreamingSource<
      &servicelib::test::ConnectorTest::AsyncService::
          RequestBidirectionalStreaming>(
      runtime.grpcContext(), service,
      [&bidiCancellation, &bidiContext, &bidiContextSent](
         servicelib::MessageContext context,
         std::vector<servicelib::test::EchoRequest> requests)
          -> asio::awaitable<std::vector<servicelib::test::EchoResponse>> {
        if (!bidiContextSent.exchange(true)) bidiContext.set_value(context);
        if (!requests.empty() && requests.front().value() == "cancel-bidi") {
          bidiCancellation.accepted.set_value(context);
          co_await bidiCancellation.release.AsyncWait(context);
        } else if (!requests.empty() &&
                   requests.front().value() == "fail-bidi") {
          throw std::runtime_error("bidirectional handler failed");
        } else if (!requests.empty() &&
                   requests.front().value() == "deadline-bidi") {
          asio::steady_timer timer(co_await asio::this_coro::executor, 250ms);
          co_await timer.async_wait(asio::use_awaitable);
        }
        std::vector<servicelib::test::EchoResponse> responses;
        for (const auto& request : requests) {
          auto& response = responses.emplace_back();
          response.set_value("bidi:" + request.value());
        }
        co_return responses;
      }, runtime.grpcExecutor());
  runtime.Start();

  auto channel = grpc::CreateChannel("127.0.0.1:" + std::to_string(port),
                                     grpc::InsecureChannelCredentials());
  auto stub = servicelib::test::ConnectorTest::NewStub(channel);
  std::promise<bool> finished;
  auto future = finished.get_future();
  std::vector<servicelib::test::EchoRequest> requests(2);
  requests[0].set_value("a");
  requests[1].set_value("b");
  auto context = servicelib::MessageContext{}
                     .withStreamId("streaming-1")
                     .withPriority(7)
                     .withSampling(true)
                     .withTrace(
                         {"4bf92f3577b34da6a3ce929d0e0e4736",
                          "00f067aa0ba902b7", true, "vendor=value",
                          "tenant=acme"})
                     .withDeadline(std::chrono::steady_clock::now() + 3s);

  auto runLegacyCalls = [&]() -> asio::awaitable<void> {
    auto client = co_await servicelib::grpc_transport::ClientStreamCall<
        &servicelib::test::ConnectorTest::Stub::PrepareAsyncClientStreaming>(
        runtime.grpcContext(), *stub, context, requests);
    if (!client.ok() || client.response.value() != "ab") {
      finished.set_value(false);
      co_return;
    }

    auto serverStream = co_await servicelib::grpc_transport::ServerStreamCall<
        &servicelib::test::ConnectorTest::Stub::PrepareAsyncServerStreaming>(
        runtime.grpcContext(), *stub, context, requests[0]);
    if (!serverStream.ok() || serverStream.responses.size() != 3 ||
        serverStream.responses[2].value() != "a2") {
      finished.set_value(false);
      co_return;
    }

    auto bidi =
        co_await servicelib::grpc_transport::BidirectionalStreamCall<
            &servicelib::test::ConnectorTest::Stub::
                PrepareAsyncBidirectionalStreaming>(
            runtime.grpcContext(), *stub, context, requests);
    finished.set_value(bidi.ok() && bidi.responses.size() == 2 &&
                       bidi.responses[0].value() == "bidi:a" &&
                       bidi.responses[1].value() == "bidi:b");
  };
  runtime.SpawnGrpc(runLegacyCalls());

  Require(future.wait_for(5s) == std::future_status::ready,
          "legacy streaming calls did not complete");
  Require(future.get(), "legacy streaming payload/status contract differs");
  RequirePropagatedContext(clientContext.get_future().get(),
                           "client-streaming");
  RequirePropagatedContext(serverContext.get_future().get(),
                           "server-streaming");
  RequirePropagatedContext(bidiContext.get_future().get(),
                           "bidirectional-streaming");

  {
    std::promise<StreamingTranscript> completed;
    auto frameworkFuture = completed.get_future();
    auto runFramework = [&]() -> asio::awaitable<void> {
      completed.set_value(co_await FrameworkStreamingTranscript(
          runtime, *stub, servicelib::MessageContext{}, requests));
    };
    runtime.SpawnGrpc(runFramework());
    Require(frameworkFuture.wait_for(5s) == std::future_status::ready,
            "framework/native streaming transcript did not complete");
    const auto framework = frameworkFuture.get();
    auto nativeFuture = std::async(std::launch::async, [&] {
      return NativeStreamingTranscript(*stub, requests);
    });
    Require(nativeFuture.wait_for(5s) == std::future_status::ready,
            "native synchronous streaming transcript did not complete");
    Require(framework == nativeFuture.get(),
            "framework/native streaming payload, EOF or status differs");
  }

  {
    std::promise<bool> checked;
    auto checkedFuture = checked.get_future();
    auto verifyErrors = [&]() -> asio::awaitable<void> {
      std::vector<servicelib::test::EchoRequest> values(1);
      values[0].set_value("fail-client");
      auto client = co_await servicelib::grpc_transport::ClientStreamCall<
          &servicelib::test::ConnectorTest::Stub::PrepareAsyncClientStreaming>(
          runtime.grpcContext(), *stub, servicelib::MessageContext{}, values);
      servicelib::test::EchoRequest serverRequest;
      serverRequest.set_value("fail-server");
      auto serverStream =
          co_await servicelib::grpc_transport::ServerStreamCall<
              &servicelib::test::ConnectorTest::Stub::
                  PrepareAsyncServerStreaming>(
              runtime.grpcContext(), *stub, servicelib::MessageContext{},
              serverRequest);
      values[0].set_value("fail-bidi");
      auto bidi =
          co_await servicelib::grpc_transport::BidirectionalStreamCall<
              &servicelib::test::ConnectorTest::Stub::
                  PrepareAsyncBidirectionalStreaming>(
              runtime.grpcContext(), *stub, servicelib::MessageContext{},
              values);
      checked.set_value(
          client.status.error_code() == grpc::StatusCode::INTERNAL &&
          client.status.error_message() ==
              "client-streaming handler failed" &&
          serverStream.status.error_code() == grpc::StatusCode::INTERNAL &&
          serverStream.status.error_message() ==
              "server-streaming handler failed" &&
          bidi.status.error_code() == grpc::StatusCode::INTERNAL &&
          bidi.status.error_message() == "bidirectional handler failed");
    };
    runtime.SpawnGrpc(verifyErrors());
    Require(checkedFuture.wait_for(5s) == std::future_status::ready,
            "streaming error mapping did not complete");
    Require(checkedFuture.get(), "streaming INTERNAL status mapping differs");
  }

  {
    std::promise<bool> checked;
    auto checkedFuture = checked.get_future();
    auto verifyDeadlines = [&]() -> asio::awaitable<void> {
      const auto deadlineContext = [] {
        return servicelib::MessageContext{}.withDeadline(
            std::chrono::steady_clock::now() + 25ms);
      };
      std::vector<servicelib::test::EchoRequest> values(1);
      values[0].set_value("deadline-client");
      auto client = co_await servicelib::grpc_transport::ClientStreamCall<
          &servicelib::test::ConnectorTest::Stub::PrepareAsyncClientStreaming>(
          runtime.grpcContext(), *stub, deadlineContext(), values);
      servicelib::test::EchoRequest serverRequest;
      serverRequest.set_value("deadline-server");
      auto serverStream =
          co_await servicelib::grpc_transport::ServerStreamCall<
              &servicelib::test::ConnectorTest::Stub::
                  PrepareAsyncServerStreaming>(
              runtime.grpcContext(), *stub, deadlineContext(), serverRequest);
      values[0].set_value("deadline-bidi");
      auto bidi =
          co_await servicelib::grpc_transport::BidirectionalStreamCall<
              &servicelib::test::ConnectorTest::Stub::
                  PrepareAsyncBidirectionalStreaming>(
              runtime.grpcContext(), *stub, deadlineContext(), values);
      checked.set_value(
          client.status.error_code() ==
                  grpc::StatusCode::DEADLINE_EXCEEDED &&
          serverStream.status.error_code() ==
                  grpc::StatusCode::DEADLINE_EXCEEDED &&
          bidi.status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED);
    };
    runtime.SpawnGrpc(verifyDeadlines());
    Require(checkedFuture.wait_for(5s) == std::future_status::ready,
            "streaming deadline mapping did not complete");
    Require(checkedFuture.get(),
            "streaming DEADLINE_EXCEEDED status mapping differs");
  }

  {
    std::stop_source cancellation;
    auto accepted = serverCancellation.accepted.get_future();
    std::promise<servicelib::grpc_transport::StreamResult<
        servicelib::test::EchoResponse>> completed;
    auto result = completed.get_future();
    servicelib::test::EchoRequest value;
    value.set_value("cancel-server");
    runtime.SpawnGrpc(RunCancelledServerStream(
        runtime, *stub,
        servicelib::MessageContext{}
            .withStopToken(cancellation.get_token())
            .withDeadline(std::chrono::steady_clock::now() + 3s),
        std::move(value), completed));
    CancelAccepted(accepted, cancellation, serverCancellation.release,
                   "server-streaming");
    Require(result.wait_for(3s) == std::future_status::ready,
            "cancelled server-streaming call did not complete");
    Require(result.get().status.error_code() == grpc::StatusCode::CANCELLED,
            "cancelled server-streaming status differs");
  }

  {
    std::stop_source cancellation;
    auto accepted = clientCancellation.accepted.get_future();
    std::promise<servicelib::grpc_transport::UnaryResult<
        servicelib::test::EchoResponse>> completed;
    auto result = completed.get_future();
    std::vector<servicelib::test::EchoRequest> values(1);
    values[0].set_value("cancel-client");
    runtime.SpawnGrpc(RunCancelledClientStream(
        runtime, *stub,
        servicelib::MessageContext{}
            .withStopToken(cancellation.get_token())
            .withDeadline(std::chrono::steady_clock::now() + 3s),
        std::move(values), completed));
    CancelAccepted(accepted, cancellation, clientCancellation.release,
                   "client-streaming");
    Require(result.wait_for(3s) == std::future_status::ready,
            "cancelled client-streaming call did not complete");
    Require(result.get().status.error_code() == grpc::StatusCode::CANCELLED,
            "cancelled client-streaming status differs");
  }

  {
    std::stop_source cancellation;
    auto accepted = bidiCancellation.accepted.get_future();
    std::promise<servicelib::grpc_transport::StreamResult<
        servicelib::test::EchoResponse>> completed;
    auto result = completed.get_future();
    std::vector<servicelib::test::EchoRequest> values(1);
    values[0].set_value("cancel-bidi");
    runtime.SpawnGrpc(RunCancelledBidirectionalStream(
        runtime, *stub,
        servicelib::MessageContext{}
            .withStopToken(cancellation.get_token())
            .withDeadline(std::chrono::steady_clock::now() + 3s),
        std::move(values), completed));
    CancelAccepted(accepted, cancellation, bidiCancellation.release,
                   "bidirectional-streaming");
    Require(result.wait_for(3s) == std::future_status::ready,
            "cancelled bidirectional call did not complete");
    Require(result.get().status.error_code() == grpc::StatusCode::CANCELLED,
            "cancelled bidirectional status differs");
  }

  {
    std::atomic<std::size_t> createdClients{0};
    servicelib::grpc_transport::ClientPool<
        servicelib::test::ConnectorTest::Stub>
        pool{runtime.grpcContext(), "127.0.0.1:" + std::to_string(port), 3,
             [&createdClients](std::shared_ptr<grpc::Channel> value) {
               createdClients.fetch_add(1, std::memory_order_relaxed);
               return servicelib::test::ConnectorTest::NewStub(
                   std::move(value));
             }};
    Require(createdClients.load(std::memory_order_relaxed) == 3,
            "gRPC connectionsCount did not create the configured channels");

    {
      std::vector<std::string> values;
      asio::io_context oneWorker;
      struct RestoreExecutor final {
        asio::any_io_executor previous = servicelib::detail::ParallelExecutorRegistry::Get();
        ~RestoreExecutor() { servicelib::detail::ParallelExecutorRegistry::Set(previous); }
      } restoreExecutor;
      servicelib::detail::ParallelExecutorRegistry::Set(oneWorker.get_executor());
      servicelib::detail::SingleUseEvent entered, resume;
      auto sibling = asio::co_spawn(oneWorker,
          servicelib::detail::CooperativeExecution::Run([&] {
            entered.Wait();
            resume.Send();
          }), asio::use_future);
      auto completed = std::make_shared<std::promise<std::exception_ptr>>();
      auto result = completed->get_future();
      pool.asyncServerStreaming<
          &servicelib::test::ConnectorTest::Stub::PrepareAsyncServerStreaming,
          servicelib::test::EchoRequest, servicelib::test::EchoResponse>(
          requests[0], servicelib::datasink::grpc::callOptions(context),
          [&](servicelib::test::EchoResponse response) {
            entered.Send();
            Require(servicelib::detail::CooperativeExecution::Active(),
                    "server-streaming response is not on a cooperative stack");
            resume.Wait();
            values.push_back(response.value());
          },
          [completed](std::exception_ptr error) {
            completed->set_value(error);
          });
      std::jthread worker([&] { oneWorker.run(); });
      const bool ready = result.wait_for(5s) == std::future_status::ready;
      // Unblock cleanup even if the callback accidentally blocks the worker.
      entered.Send();
      resume.Send();
      worker.join();
      sibling.get();
      Require(ready,
              "pooled server-streaming call did not complete");
      Require(!result.get(), "pooled server-streaming call returned an error");
      Require((values == std::vector<std::string>{"a0", "a1", "a2"}),
              "pooled server-streaming payload/EOF contract differs");
    }

    {
      int delivered = 0;
      auto completed = std::make_shared<std::promise<std::exception_ptr>>();
      auto result = completed->get_future();
      pool.asyncServerStreaming<
          &servicelib::test::ConnectorTest::Stub::PrepareAsyncServerStreaming,
          servicelib::test::EchoRequest, servicelib::test::EchoResponse>(
          requests[0], servicelib::datasink::grpc::callOptions(context),
          [&](servicelib::test::EchoResponse) {
            ++delivered;
            throw std::logic_error("business response failure");
          },
          [completed](std::exception_ptr error) { completed->set_value(error); });
      Require(result.wait_for(5s) == std::future_status::ready,
              "server-streaming handler failure did not drain the RPC");
      auto error = result.get();
      Require(static_cast<bool>(error), "server-streaming handler failure was lost");
      try {
        std::rethrow_exception(error);
      } catch (const std::logic_error& failure) {
        Require(std::string{failure.what()} == "business response failure",
                "RPC cancellation replaced the business error");
      }
      Require(delivered == 1, "responses continued after handler failure");
    }

    {
      std::string value;
      auto completed = std::make_shared<std::promise<std::exception_ptr>>();
      auto result = completed->get_future();
      auto writer = pool.asyncClientStreaming<
          &servicelib::test::ConnectorTest::Stub::PrepareAsyncClientStreaming,
          servicelib::test::EchoRequest, servicelib::test::EchoResponse>(
          servicelib::datasink::grpc::callOptions(context),
          [&](servicelib::test::EchoResponse response) {
            value = response.value();
          },
          [completed](std::exception_ptr error) {
            completed->set_value(error);
          });
      writer->write(requests[0]);
      writer->write(requests[1]);
      writer->done();
      Require(result.wait_for(5s) == std::future_status::ready,
              "pooled client-streaming call did not complete");
      Require(!result.get(), "pooled client-streaming call returned an error");
      Require(value == "ab",
              "pooled client-streaming payload/EOF contract differs");
    }

    {
      std::vector<std::string> values;
      auto completed = std::make_shared<std::promise<std::exception_ptr>>();
      auto result = completed->get_future();
      auto writer = pool.asyncBidirectionalStreaming<
          &servicelib::test::ConnectorTest::Stub::
              PrepareAsyncBidirectionalStreaming,
          servicelib::test::EchoRequest, servicelib::test::EchoResponse>(
          servicelib::datasink::grpc::callOptions(context),
          [&](servicelib::test::EchoResponse response) {
            values.push_back(response.value());
          },
          [completed](std::exception_ptr error) {
            completed->set_value(error);
          });
      writer->write(requests[0]);
      writer->write(requests[1]);
      writer->done();
      Require(result.wait_for(5s) == std::future_status::ready,
              "pooled bidirectional call did not complete");
      Require(!result.get(), "pooled bidirectional call returned an error");
      Require((values == std::vector<std::string>{"bidi:a", "bidi:b"}),
              "pooled bidirectional payload/EOF contract differs");
    }
    CheckCooperativePooledWrites<false>(pool, requests);
    CheckCooperativePooledWrites<true>(pool, requests);
    CheckNestedServerSubStreams(pool, port, serverSubStreams);
  }

  {
    auto accepted = shutdownCancellation.accepted.get_future();
    std::promise<servicelib::grpc_transport::StreamResult<
        servicelib::test::EchoResponse>> completed;
    auto result = completed.get_future();
    servicelib::test::EchoRequest value;
    value.set_value("shutdown-server");
    runtime.SpawnGrpc(RunCancelledServerStream(
        runtime, *stub,
        servicelib::MessageContext{}.withDeadline(
            std::chrono::steady_clock::now() + 5s),
        std::move(value), completed));
    Require(accepted.wait_for(3s) == std::future_status::ready,
            "shutdown streaming handler did not accept the request");
    auto shutdownContext = accepted.get();
    server->Shutdown(std::chrono::system_clock::now() + 100ms);
    const auto cancellationDeadline = std::chrono::steady_clock::now() + 2s;
    while (!shutdownContext.cancelled() &&
           std::chrono::steady_clock::now() < cancellationDeadline) {
      std::this_thread::sleep_for(1ms);
    }
    Require(shutdownContext.cancelled(),
            "server shutdown did not cancel accepted streaming context");
    shutdownCancellation.release.Send();
    Require(result.wait_for(3s) == std::future_status::ready,
            "shutdown streaming client did not complete");
    Require(!result.get().ok(),
            "forced server shutdown returned an OK streaming status");
  }
  runtime.Stop();
  runtime.Join();
}

#include "connector_test.grpc.pb.h"

#include <servicelib/runtime/detail/grpc_client.hpp>
#include <servicelib/runtime/detail/grpc_transport.hpp>
#include <servicelib/runtime/detail/grpc_runtime.hpp>
#include <servicelib/runtime/detail/async_operations.hpp>
#include <servicelib/runtime/testmetrics/testmetrics.hpp>
#include <servicelib/datasink/grpc/nostreaming.hpp>
#include <servicelib/transformation/streams.hpp>
#include <servicelib/runtime/serviceapp.hpp>

#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>

#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <atomic>
#include <array>
#include <chrono>
#include <future>
#include <iostream>
#include <memory>
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

struct SubStreamServerProbe final {
  static constexpr int calls = 16;
  servicelib::ContextKey<int> callerKey;
  std::atomic<int> started{0};
  std::mutex mutex;
  std::set<std::string> ids;
  std::promise<void> ready;
  servicelib::detail::SingleUseEvent release;
};

struct SubStreamCancellationProbe final {
  servicelib::ContextKey<int> callerKey;
  std::array<std::promise<void>, 2> accepted;
  std::array<servicelib::detail::SingleUseEvent, 2> release;
  std::array<std::promise<void>, 2> lateReply;
};

struct EndpointStopProbe final {
  std::promise<void> accepted;
  servicelib::detail::SingleUseEvent release;
  std::atomic<int> rejectedRequestsSent{0};
};

struct SubStreamGrpcConfig final : servicelib::config::IConfig {
  servicelib::config::GrpcDataConnectorConfig connector;
  servicelib::config::GrpcEndpointConfig endpoint;
  explicit SubStreamGrpcConfig(const std::string& address) {
    connector.id = 500;
    connector.name = "substream-grpc";
    connector.address = address;
    endpoint.id = 501;
    endpoint.idDataConnector = connector.id;
    endpoint.name = "substream-unary";
    endpoint.methodName = "Unary";
    endpoint.grpcMethodType = servicelib::api::GrpcMethodType::kNoStreaming;
  }
  std::vector<const servicelib::config::ServiceConfig*> GetServices() const override { return {}; }
  std::vector<servicelib::config::StreamConfigRef> GetStreams() const override { return {}; }
  std::vector<servicelib::config::DataConnectorConfigRef> GetDataConnectors() const override { return {connector}; }
  std::vector<servicelib::config::EndpointConfigRef> GetEndpoints() const override { return {endpoint}; }
  std::vector<const servicelib::config::PoolConfig*> GetPools() const override { return {}; }
  std::vector<const servicelib::config::LinkConfig*> GetLinks() const override { return {}; }
  std::vector<const servicelib::config::ModuleConfig*> GetModules() const override { return {}; }
  std::vector<const servicelib::config::TypeConfig*> GetTypes() const override { return {}; }
};

using SubStreamForward = std::function<void(servicelib::MessageContext, const std::string&)>;
using SubStreamInvoke = std::function<void(servicelib::MessageContext, int, SubStreamForward)>;

struct SubStreamGrpcCall final {
  SubStreamForward output;
  bool ended{};
  std::exception_ptr error;
};

struct SubStreamGrpcWork final {
  SubStreamInvoke* invoke;
  template <typename Output>
  void operator()(servicelib::MessageContext context, servicelib::StreamBase&,
                  int& value, Output&& output) const {
    (*invoke)(std::move(context), value,
        [&output](servicelib::MessageContext resultContext, const std::string& result) {
          output.out(std::move(resultContext), result);
        });
  }
};

struct SubStreamGrpcTypes final {
  template <typename> struct DataType {};
};

class SubStreamGrpcApp final
    : public servicelib::StreamExecutionEnvironment<SubStreamGrpcApp, SubStreamGrpcTypes> {
 public:
  explicit SubStreamGrpcApp(const std::string& address) : config(address) {}
  SubStreamGrpcConfig config;
  std::shared_ptr<const servicelib::config::RuntimeConfig> snapshot{
      std::make_shared<const servicelib::config::RuntimeConfig>(config)};
  servicelib::ContextKey<SubStreamGrpcCall> callKey;
  SubStreamInvoke invoke;
  std::shared_ptr<servicelib::SubStream<int, std::string, SubStreamGrpcApp>> entry;

  std::shared_ptr<const servicelib::config::RuntimeConfig>
  getRuntimeConfigSnapshot() const override { return snapshot; }
  void delay(servicelib::Context, servicelib::pool::IDelayPool::Duration,
             std::function<void()> task) override { task(); }
  void init() {
    servicelib::config::SubStreamConfig root;
    root.id = 401;
    root.name = "grpc-substream";
    entry = servicelib::makeSubStream<int, std::string, SubStreamGrpcApp>(root, *this);
    servicelib::config::MapStreamConfig map;
    map.id = 402;
    map.name = "grpc-substream-call";
    auto& output = entry->map(map, servicelib::StreamType<std::string>{},
        servicelib::StreamFunction(SubStreamGrpcWork{&invoke}));
    entry->setSource(output);
    static_cast<void>(getExecutionRuntime<>());
  }
};

class SubStreamGrpcSink final : public servicelib::SinkEndpointStream<int, std::string> {
 public:
  explicit SubStreamGrpcSink(SubStreamGrpcApp& app) : app_(app) {}
  servicelib::IServiceEnvironment& environment() const override { return app_; }
  int endpointId() const noexcept override { return 501; }
  std::size_t streamConfigId() const noexcept override { return 402; }
  void collectResult(servicelib::MessageContext context,
                     servicelib::Payload<std::string> value) override {
    auto call = context.localValue(app_.callKey);
    Require(static_cast<bool>(call), "gRPC result lost its local invocation state");
    call->output(std::move(context), value.get());
  }
  void collectError(servicelib::MessageContext,
                    servicelib::Payload<std::exception_ptr> error) override {
    std::rethrow_exception(error.get());
  }
 private:
  SubStreamGrpcApp& app_;
};

struct SubStreamGrpcHandler final {
  using State = std::shared_ptr<SubStreamGrpcCall>;
  servicelib::ContextKey<SubStreamGrpcCall>* key;
  std::string prefix{"substream-"};
  servicelib::BeginResult<State> beginRequest(servicelib::MessageContext context, auto&) {
    auto state = context.localValue(*key);
    Require(static_cast<bool>(state), "gRPC request lost its local invocation state");
    return {std::move(context), std::move(state)};
  }
  void consumeMessage(servicelib::MessageContext, auto&, State&, const int& value,
                      auto& sender, servicelib::datasink::grpc::ResultContext) {
    servicelib::test::EchoRequest request;
    request.set_value(prefix + std::to_string(value));
    sender.send(std::move(request));
  }
  void handleResponse(servicelib::MessageContext context, auto& stream, State&,
                      const servicelib::test::EchoResponse& response) {
    stream.collect(std::move(context), response.value());
  }
  void endRequest(servicelib::MessageContext, auto&, std::exception_ptr error,
                  State& state) noexcept {
    state->error = std::move(error);
    state->ended = true;
  }
};

using SubStreamClientPool = servicelib::grpc_transport::ClientPool<
    servicelib::test::ConnectorTest::Stub>;
struct SubStreamGrpcClient final {
  SubStreamClientPool* pool;
  void async(servicelib::test::EchoRequest request,
             servicelib::datasink::grpc::CallOptions options,
             std::function<void(std::exception_ptr,
                 std::optional<servicelib::test::EchoResponse>)> completion) {
    pool->asyncUnary<&servicelib::test::ConnectorTest::Stub::PrepareAsyncUnary,
        servicelib::test::EchoRequest, servicelib::test::EchoResponse>(
            std::move(request), std::move(options), std::move(completion));
  }
};

template <typename Endpoint>
void ConfigureGrpcSubStreamBody(SubStreamGrpcApp& app, Endpoint& endpoint,
    std::function<void(int, std::weak_ptr<SubStreamGrpcCall>)> observe = {}) {
  app.invoke = [&app, &endpoint, observe = std::move(observe)](
      servicelib::MessageContext context, int value, SubStreamForward output) {
    if (value >= 1000) {
      auto collector = std::make_shared<servicelib::SubStreamCollectorFunc<std::string>>(
          [output = std::move(output)](servicelib::MessageContext resultContext,
                                       const std::string& result) {
            output(std::move(resultContext), "nested:" + result);
            return true;
          });
      app.entry->consume(std::move(context), servicelib::Payload<int>::make(value - 1000),
                         std::move(collector));
      return;
    }
    auto call = std::make_shared<SubStreamGrpcCall>();
    call->output = std::move(output);
    if (observe) observe(value, call);
    endpoint.consume(context.withLocalValue(app.callKey, call),
                     servicelib::Payload<int>::make(value));
    Require(call->ended, "gRPC Consume returned before EndRequest in SubStream");
    if (call->error) std::rethrow_exception(call->error);
  };
}

void CheckConcurrentNestedSubStreams(servicelib::async::GrpcRuntime& runtime,
                                    int port, SubStreamServerProbe& probe) {
  const auto address = "127.0.0.1:" + std::to_string(port);
  SubStreamClientPool pool{runtime.grpcContext(), address, 1,
      [](std::shared_ptr<grpc::Channel> channel) {
        return servicelib::test::ConnectorTest::NewStub(std::move(channel));
      }};
  SubStreamGrpcApp app{address};
  app.init();
  SubStreamGrpcSink stream{app};
  servicelib::datasink::grpc::NoStreamingEndpoint<servicelib::test::EchoRequest,
      servicelib::test::EchoResponse, int, std::string, SubStreamGrpcHandler,
      SubStreamGrpcClient> endpoint{stream, SubStreamGrpcHandler{&app.callKey},
                                    SubStreamGrpcClient{&pool}};
  endpoint.start({});
  ConfigureGrpcSubStreamBody(app, endpoint);
  auto accepted = probe.ready.get_future();
  std::vector<std::future<void>> completed;
  for (int index = 0; index < SubStreamServerProbe::calls; ++index) {
    auto promise = std::make_shared<std::promise<void>>();
    completed.push_back(promise->get_future());
    servicelib::detail::ParallelExecutorRegistry::Post([&, index, promise] {
      try {
        int collected = 0;
        auto collector = std::make_shared<servicelib::SubStreamCollectorFunc<std::string>>(
            [&](servicelib::MessageContext context, const std::string& result) {
              Require(context.streamId() == "substream-parent", "SubStream changed parent ID");
              auto owner = context.localValue(probe.callerKey);
              Require(owner && *owner == index, "gRPC response reached another SubStream invocation");
              Require(result == "nested:echo:substream-" + std::to_string(index),
                      "nested SubStream result differs");
              ++collected;
              return true;
            });
        app.entry->consume(servicelib::MessageContext{}.withStreamId("substream-parent")
            .withLocalValue(probe.callerKey, std::make_shared<int>(index))
            .withDeadline(std::chrono::steady_clock::now() + 8s),
            servicelib::Payload<int>::make(1000 + index), std::move(collector));
        Require(collected == 1, "SubStream returned without exactly one result");
        promise->set_value();
      } catch (...) {
        promise->set_exception(std::current_exception());
      }
    });
  }
  const bool allAccepted = accepted.wait_for(3s) == std::future_status::ready;
  probe.release.Send();
  for (auto& future : completed) {
    Require(future.wait_for(10s) == std::future_status::ready,
            "nested SubStream with gRPC did not finish");
    future.get();
  }
  endpoint.stop({});
  Require(allAccepted, "a gRPC SubStream blocked the only executor worker");
  {
    std::lock_guard lock(probe.mutex);
    Require(probe.ids.size() == SubStreamServerProbe::calls,
            "nested SubStreams reused a network request ID");
  }
  std::cout << "Concurrent nested SubStream/gRPC: 16 calls, one worker, passed\n";
}

void CheckCancelledNestedSubStream(servicelib::async::GrpcRuntime& runtime,
    int port, SubStreamCancellationProbe& probe, bool deadline) {
  const std::size_t trial = deadline ? 1U : 0U;
  const int failedValue = deadline ? 301 : 201;
  const auto expectedCode = deadline ? grpc::StatusCode::DEADLINE_EXCEEDED
                                     : grpc::StatusCode::CANCELLED;
  const auto address = "127.0.0.1:" + std::to_string(port);
  SubStreamClientPool pool{runtime.grpcContext(), address, 1,
      [](std::shared_ptr<grpc::Channel> channel) {
        return servicelib::test::ConnectorTest::NewStub(std::move(channel));
      }};
  SubStreamGrpcApp app{address};
  app.init();
  SubStreamGrpcSink stream{app};
  servicelib::datasink::grpc::NoStreamingEndpoint<servicelib::test::EchoRequest,
      servicelib::test::EchoResponse, int, std::string, SubStreamGrpcHandler,
      SubStreamGrpcClient> endpoint{stream, SubStreamGrpcHandler{&app.callKey, "isolation-"},
                                    SubStreamGrpcClient{&pool}};
  endpoint.start({});
  std::weak_ptr<SubStreamGrpcCall> failedState;
  ConfigureGrpcSubStreamBody(app, endpoint,
      [&failedState, failedValue](int value, std::weak_ptr<SubStreamGrpcCall> state) {
        if (value == failedValue) failedState = std::move(state);
      });
  auto failedCollections = std::make_shared<std::atomic<int>>(0);
  auto siblingCollections = std::make_shared<std::atomic<int>>(0);
  auto launch = [&](int value, servicelib::MessageContext context,
                     std::shared_ptr<std::atomic<int>> collections, bool expectFailure) {
    auto promise = std::make_shared<std::promise<void>>();
    auto future = promise->get_future();
    servicelib::detail::ParallelExecutorRegistry::Post(
        [&, value, context = std::move(context), collections, expectFailure, promise]() mutable {
          try {
            auto collector = std::make_shared<servicelib::SubStreamCollectorFunc<std::string>>(
                [&, value, collections](servicelib::MessageContext resultContext,
                                         const std::string& result) {
                  ++*collections;
                  Require(resultContext.streamId() == "substream-parent",
                          "cancelled sibling changed the successful parent ID");
                  auto owner = resultContext.localValue(probe.callerKey);
                  Require(owner && *owner == value, "nested cancellation mixed caller contexts");
                  Require(result == "nested:echo:isolation-" + std::to_string(value),
                          "nested cancellation changed the sibling result");
                  return true;
                });
            bool failed = false;
            try {
              app.entry->consume(std::move(context), servicelib::Payload<int>::make(1000 + value),
                                 std::move(collector));
            } catch (const servicelib::grpc_transport::StatusError& error) {
              failed = true;
              Require(expectFailure && error.code() == expectedCode,
                      "nested gRPC call returned an unexpected cancellation/deadline status");
            }
            Require(failed == expectFailure, "nested gRPC failure isolation differs");
            Require(collections->load() == (expectFailure ? 0 : 1),
                    "nested gRPC call delivered an unexpected number of results");
            promise->set_value();
          } catch (...) {
            promise->set_exception(std::current_exception());
          }
        });
    return future;
  };
  std::stop_source cancellation;
  auto context = servicelib::MessageContext{}.withStreamId("substream-parent");
  auto failedContext = context.withLocalValue(probe.callerKey, std::make_shared<int>(failedValue))
      .withStopToken(cancellation.get_token())
      .withDeadline(std::chrono::steady_clock::now() + (deadline ? 500ms : 8s));
  auto accepted = probe.accepted[trial].get_future();
  auto lateReply = probe.lateReply[trial].get_future();
  auto failed = launch(failedValue, std::move(failedContext), failedCollections, true);
  const bool wasAccepted = accepted.wait_for(3s) == std::future_status::ready;
  auto sibling = launch(failedValue + 1,
      context.withLocalValue(probe.callerKey, std::make_shared<int>(failedValue + 1))
          .withDeadline(std::chrono::steady_clock::now() + 5s),
      siblingCollections, false);
  if (!deadline) cancellation.request_stop();
  const bool failedReady = failed.wait_for(3s) == std::future_status::ready;
  const bool siblingReady = sibling.wait_for(3s) == std::future_status::ready;
  // Check after the completed cooperative call has left its stack, not while
  // promise publication still permits the current call frame to be alive.
  auto lifetime = std::make_shared<std::promise<bool>>();
  auto lifetimeFuture = lifetime->get_future();
  servicelib::detail::ParallelExecutorRegistry::Post([&failedState, lifetime] {
    lifetime->set_value(failedState.expired());
  });
  const bool lifetimeReady = lifetimeFuture.wait_for(3s) == std::future_status::ready;
  const bool stateReleased = lifetimeReady && lifetimeFuture.get();
  // Always release the delayed server path before reporting assertion failures.
  probe.release[trial].Send();
  const bool lateAttempted = lateReply.wait_for(3s) == std::future_status::ready;
  Require(wasAccepted, "cancel/deadline fired before the gRPC test request was accepted");
  Require(failedReady && siblingReady, "nested cancellation blocked the sibling or its own completion");
  failed.get();
  sibling.get();
  endpoint.stop({});
  Require(stateReleased, "finished nested gRPC request retained callback state until a late response");
  Require(lateAttempted, "the server did not execute the delayed response path");
  Require(failedCollections->load() == 0 && siblingCollections->load() == 1,
          "late gRPC response reached a completed collector");
  std::cout << "Nested SubStream/gRPC " << (deadline ? "deadline" : "cancellation")
            << ": isolated sibling, released state, late reply ignored, passed\n";
}

void CheckTransportDrainWaitersAndRestart() {
  servicelib::detail::AsyncOperations operations;
  for (int epoch = 0; epoch < 3; ++epoch) {
    operations.start();
    auto token = operations.acquire();
    Require(static_cast<bool>(token), "transport did not reopen admission");
    std::vector<std::future<void>> stopped;
    for (int waiter = 0; waiter < 3; ++waiter) {
      auto promise = std::make_shared<std::promise<void>>();
      stopped.push_back(promise->get_future());
      servicelib::detail::ParallelExecutorRegistry::Post([&operations, promise] {
        operations.stopAndWait();
        promise->set_value();
      });
    }
    std::promise<void> nativeStarted;
    auto nativeStartedFuture = nativeStarted.get_future();
    std::promise<void> nativeStopped;
    auto nativeStoppedFuture = nativeStopped.get_future();
    std::thread nativeWaiter([&] {
      nativeStarted.set_value();
      operations.stopAndWait();
      nativeStopped.set_value();
    });
    auto pulse = std::make_shared<std::promise<void>>();
    auto pulseFuture = pulse->get_future();
    servicelib::detail::ParallelExecutorRegistry::Post([pulse] { pulse->set_value(); });
    const bool progressed = pulseFuture.wait_for(1s) == std::future_status::ready;
    const bool nativeStartedInTime = nativeStartedFuture.wait_for(1s) == std::future_status::ready;
    const bool admissionClosed = !operations.acquire();
    bool stoppedEarly = nativeStoppedFuture.wait_for(0s) == std::future_status::ready;
    for (auto& future : stopped) {
      stoppedEarly = stoppedEarly || future.wait_for(0s) == std::future_status::ready;
    }
    token.reset();
    nativeWaiter.join();
    for (auto& future : stopped) {
      Require(future.wait_for(3s) == std::future_status::ready,
              "transport drain failed to wake every cooperative waiter");
      future.get();
    }
    nativeStoppedFuture.get();
    Require(progressed && nativeStartedInTime, "transport draining blocked executor progress");
    Require(admissionClosed && !stoppedEarly, "transport drain admission or lifetime differs");
    operations.stopAndWait();
  }
}

enum class StopScope { kEndpoint, kConnector, kLifecycle };

void CheckCooperativeEndpointStop(servicelib::async::GrpcRuntime& runtime,
                                 int port, EndpointStopProbe& probe, StopScope scope) {
  const std::string prefix = scope == StopScope::kEndpoint ? "shutdown-" :
      (scope == StopScope::kConnector ? "connector-shutdown-" : "service-shutdown-");
  const auto address = "127.0.0.1:" + std::to_string(port);
  SubStreamClientPool pool{runtime.grpcContext(), address, 1,
      [](std::shared_ptr<grpc::Channel> channel) {
        return servicelib::test::ConnectorTest::NewStub(std::move(channel));
      }};
  SubStreamGrpcApp app{address};
  app.init();
  SubStreamGrpcSink stream{app};
  using Endpoint = servicelib::datasink::grpc::NoStreamingEndpoint<servicelib::test::EchoRequest,
      servicelib::test::EchoResponse, int, std::string, SubStreamGrpcHandler,
      SubStreamGrpcClient>;
  auto owner = std::make_shared<Endpoint>(stream, SubStreamGrpcHandler{&app.callKey, prefix},
                                        SubStreamGrpcClient{&pool});
  auto& endpoint = *owner;
  auto connector = servicelib::datasink::grpc::DataSink::make(stream);
  connector->addEndpoint(owner);
  servicelib::ServiceLifecycle lifecycle;
  if (scope == StopScope::kLifecycle) {
    lifecycle.add(servicelib::ServiceComponentKind::kDataSink, connector);
    lifecycle.start({});
  } else {
    connector->start({});
  }
  auto stop = [&] {
    if (scope == StopScope::kLifecycle) lifecycle.stop({});
    else if (scope == StopScope::kConnector) connector->stop({});
    else endpoint.stop({});
  };
  ConfigureGrpcSubStreamBody(app, endpoint);
  auto launch = [&](int value, bool reject) {
    auto promise = std::make_shared<std::promise<void>>();
    auto future = promise->get_future();
    servicelib::detail::ParallelExecutorRegistry::Post([&, value, reject, promise] {
      try {
        int collected = 0;
        auto collector = std::make_shared<servicelib::SubStreamCollectorFunc<std::string>>(
            [&](servicelib::MessageContext context, const std::string& result) {
              Require(context.streamId() == "stop-parent", "stop changed the parent context");
              Require(result == "nested:echo:" + prefix + std::to_string(value),
                      "stop changed the active request result");
              ++collected;
              return true;
            });
        bool rejected = false;
        try {
          app.entry->consume(servicelib::MessageContext{}.withStreamId("stop-parent")
              .withDeadline(std::chrono::steady_clock::now() + 10s),
              servicelib::Payload<int>::make(1000 + value), std::move(collector));
        } catch (const std::runtime_error& error) {
          rejected = true;
          Require(reject && std::string(error.what()) == "gRPC sink endpoint is stopped",
                  "unexpected failure while draining gRPC endpoint");
        }
        Require(rejected == reject && collected == (reject ? 0 : 1),
                "stopped endpoint accepted a call or lost an admitted result");
        promise->set_value();
      } catch (...) {
        promise->set_exception(std::current_exception());
      }
    });
    return future;
  };
  auto accepted = probe.accepted.get_future();
  auto activeCall = launch(401, false);
  Require(accepted.wait_for(3s) == std::future_status::ready,
          "stop test server did not accept the active request");
  auto stopping = std::make_shared<std::promise<void>>();
  auto stoppingFuture = stopping->get_future();
  auto stopped = std::make_shared<std::promise<void>>();
  auto stoppedFuture = stopped->get_future();
  servicelib::detail::ParallelExecutorRegistry::Post([&stop, stopping, stopped] {
    stopping->set_value();
    try {
      stop();
      stopped->set_value();
    } catch (...) {
      stopped->set_exception(std::current_exception());
    }
  });
  Require(stoppingFuture.wait_for(3s) == std::future_status::ready,
          "endpoint stop task did not start");
  // Starting connector shutdown is not an admission-closed notification:
  // its control thread may not have entered endpoint.stop() yet. Probe reactor
  // progress independently, and check rejected admission after stop returns.
  auto pulse = std::make_shared<std::promise<void>>();
  auto pulseFuture = pulse->get_future();
  servicelib::detail::ParallelExecutorRegistry::Post([pulse] { pulse->set_value(); });
  const bool workerProgressed = pulseFuture.wait_for(1s) == std::future_status::ready;
  const bool stopReturnedEarly = stoppedFuture.wait_for(0s) == std::future_status::ready;
  const bool callReturnedEarly = activeCall.wait_for(0s) == std::future_status::ready;
  probe.release.Send();
  if (!workerProgressed) {
    // Baseline rescue only: allow the held completion to run on the test thread
    // so a blocking stop yields an assertion failure rather than hanging cleanup.
    runtime.ioContext().run_for(1s);
  }
  Require(activeCall.wait_for(3s) == std::future_status::ready &&
              stoppedFuture.wait_for(3s) == std::future_status::ready &&
              pulseFuture.wait_for(3s) == std::future_status::ready,
          "endpoint shutdown did not drain after releasing the server");
  activeCall.get();
  stoppedFuture.get();
  pulseFuture.get();
  Require(workerProgressed, "endpoint stop blocked the only executor worker");
  Require(!stopReturnedEarly && !callReturnedEarly,
          "endpoint stop returned before the active request finished");
  auto rejectedCall = launch(402, true);
  Require(rejectedCall.wait_for(3s) == std::future_status::ready,
          "stopped endpoint did not reject a new request");
  rejectedCall.get();
  Require(probe.rejectedRequestsSent.load() == 0,
          "closed endpoint admission still sent a network request");
  if (scope != StopScope::kLifecycle) {
    connector->start({});
    auto restarted = launch(403, false);
    Require(restarted.wait_for(3s) == std::future_status::ready,
            "endpoint did not accept a request after restart");
    restarted.get();
    stop();
  }
  std::cout << "Cooperative gRPC " << prefix << "stop: closed admission and drained request passed\n";
}

}  // namespace

int main() {
  servicelib::test::ConnectorTest::AsyncService service;
  grpc::ServerBuilder builder;
  int port{};
  builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(),
                           &port);
  builder.RegisterService(&service);
  auto queue = builder.AddCompletionQueue();
  auto server = builder.BuildAndStart();
  Require(static_cast<bool>(server), "gRPC server did not start");
  Require(port > 0, "gRPC server did not allocate a port");

  servicelib::testmetrics::TestMetrics metrics;
  servicelib::async::GrpcRuntime runtime(
      {.workers = 1, .unhandledException = {}, .metrics = &metrics},
      std::move(queue));
  std::promise<servicelib::MessageContext> receivedContext;
  auto receivedFuture = receivedContext.get_future();
  servicelib::detail::SingleUseEvent releaseFirstRequest;
  std::promise<servicelib::MessageContext> receivedCancelledContext;
  auto receivedCancelledFuture = receivedCancelledContext.get_future();
  servicelib::detail::SingleUseEvent releaseCancelledRequest;
  std::promise<servicelib::MessageContext> receivedPooledCancelledContext;
  auto receivedPooledCancelledFuture =
      receivedPooledCancelledContext.get_future();
  servicelib::detail::SingleUseEvent releasePooledCancelledRequest;
  std::atomic<bool> firstRequest{true};
  constexpr int concurrentRequests = 64;
  std::atomic<int> concurrentStarted{0};
  std::promise<void> concurrentReady;
  auto concurrentReadyFuture = concurrentReady.get_future();
  servicelib::detail::SingleUseEvent releaseConcurrentRequests;
  SubStreamServerProbe subStreamProbe;
  SubStreamCancellationProbe subStreamCancellationProbe;
  std::array<EndpointStopProbe, 3> endpointStopProbes;
  servicelib::grpc_transport::RegisterUnarySource<
      &servicelib::test::ConnectorTest::AsyncService::RequestUnary>(
      runtime.grpcContext(), service,
      [&receivedContext, &releaseFirstRequest, &receivedCancelledContext,
       &releaseCancelledRequest, &receivedPooledCancelledContext,
       &releasePooledCancelledRequest, &firstRequest, &runtime,
       &concurrentStarted, &concurrentReady, &releaseConcurrentRequests,
       &subStreamProbe, &subStreamCancellationProbe, &endpointStopProbes](
          servicelib::MessageContext context,
          const servicelib::test::EchoRequest& request)
          -> asio::awaitable<servicelib::test::EchoResponse> {
        Require(runtime.ioContext().get_executor().running_in_this_thread(),
                "gRPC business handler left its Asio executor");
        if (firstRequest.exchange(false)) {
          receivedContext.set_value(context);
          co_await releaseFirstRequest.AsyncWait();
        } else if (request.value() == "cancel-midflight") {
          receivedCancelledContext.set_value(context);
          co_await releaseCancelledRequest.AsyncWait(context);
        } else if (request.value() == "cancel-pooled") {
          receivedPooledCancelledContext.set_value(context);
          co_await releasePooledCancelledRequest.AsyncWait(context);
        } else if (request.value().starts_with("concurrent-")) {
          if (concurrentStarted.fetch_add(1) + 1 == concurrentRequests)
            concurrentReady.set_value();
          co_await releaseConcurrentRequests.AsyncWait();
        } else if (request.value() == "shutdown-401" ||
                   request.value() == "connector-shutdown-401" ||
                   request.value() == "service-shutdown-401") {
          const std::size_t index = request.value().starts_with("connector-") ? 1U :
              (request.value().starts_with("service-") ? 2U : 0U);
          endpointStopProbes[index].accepted.set_value();
          co_await endpointStopProbes[index].release.AsyncWait();
        } else if (request.value() == "shutdown-402" ||
                   request.value() == "connector-shutdown-402" ||
                   request.value() == "service-shutdown-402") {
          const std::size_t index = request.value().starts_with("connector-") ? 1U :
              (request.value().starts_with("service-") ? 2U : 0U);
          ++endpointStopProbes[index].rejectedRequestsSent;
        } else if (request.value() == "isolation-201" || request.value() == "isolation-301") {
          const std::size_t trial = request.value() == "isolation-301" ? 1U : 0U;
          Require(!context.localValue(subStreamCancellationProbe.callerKey),
                  "local cancellation state crossed the network");
          subStreamCancellationProbe.accepted[trial].set_value();
          co_await subStreamCancellationProbe.release[trial].AsyncWait();
          subStreamCancellationProbe.lateReply[trial].set_value();
        } else if (request.value().starts_with("substream-")) {
          Require(!context.localValue(subStreamProbe.callerKey),
                  "SubStream local values crossed the network");
          Require(!context.streamId().empty() && context.streamId() != "substream-parent",
                  "gRPC endpoint failed to create a network request ID");
          {
            std::lock_guard lock(subStreamProbe.mutex);
            Require(subStreamProbe.ids.insert(std::string(context.streamId())).second,
                    "concurrent SubStreams share a network request ID");
          }
          if (subStreamProbe.started.fetch_add(1) + 1 == SubStreamServerProbe::calls)
            subStreamProbe.ready.set_value();
          co_await subStreamProbe.release.AsyncWait(context);
        } else if (request.value() == "fail") {
          throw std::runtime_error("unary handler failed");
        } else if (context.streamId() == "expired") {
          asio::steady_timer timer(co_await asio::this_coro::executor, 250ms);
          boost::system::error_code error;
          co_await timer.async_wait(
              asio::redirect_error(asio::use_awaitable, error));
        }
        servicelib::test::EchoResponse response;
        response.set_value("echo:" + request.value());
        co_return response;
      }, runtime.grpcExecutor());
  runtime.Start();

  auto channel = grpc::CreateChannel("127.0.0.1:" + std::to_string(port),
                                     grpc::InsecureChannelCredentials());
  auto stub = servicelib::test::ConnectorTest::NewStub(channel);
  std::promise<servicelib::grpc_transport::UnaryResult<
      servicelib::test::EchoResponse>> resultPromise;
  auto resultFuture = resultPromise.get_future();
  std::atomic<bool> cancellationVerified{false};
  std::atomic<int> cancelledCode{-1};
  std::atomic<int> expiredCode{-1};
  std::atomic<int> midflightCancelledCode{-1};
  std::atomic<int> failedCode{-1};
  std::stop_source midflightCancellation;

  auto context = servicelib::MessageContext{}
                     .withStreamId("stream-42")
                     .withPriority(7)
                     .withSampling(true)
                     .withTrace(
                         {"4bf92f3577b34da6a3ce929d0e0e4736",
                          "00f067aa0ba902b7", true, "vendor=value",
                          "tenant=acme"})
                     .withDeadline(std::chrono::steady_clock::now() + 2s);
  servicelib::test::EchoRequest request;
  request.set_value("hello");
  auto runCalls = [&]() -> asio::awaitable<void> {
    auto result = co_await servicelib::grpc_transport::UnaryCall<
        &servicelib::test::ConnectorTest::Stub::PrepareAsyncUnary>(
        runtime.grpcContext(), *stub, context, request);

    servicelib::test::EchoRequest midflightRequest;
    midflightRequest.set_value("cancel-midflight");
    auto midflightCancelled =
        co_await servicelib::grpc_transport::UnaryCall<
            &servicelib::test::ConnectorTest::Stub::PrepareAsyncUnary>(
            runtime.grpcContext(), *stub,
            servicelib::MessageContext{}
                .withStreamId("cancel-midflight")
                .withStopToken(midflightCancellation.get_token())
                .withDeadline(std::chrono::steady_clock::now() + 3s),
            midflightRequest);

    std::stop_source cancellation;
    auto cancelledContext =
        servicelib::MessageContext{}
            .withStreamId("cancelled")
            .withStopToken(cancellation.get_token())
            .withDeadline(std::chrono::steady_clock::now() + 2s);
    cancellation.request_stop();
    auto cancelled = co_await servicelib::grpc_transport::UnaryCall<
        &servicelib::test::ConnectorTest::Stub::PrepareAsyncUnary>(
        runtime.grpcContext(), *stub, cancelledContext, request);

    auto expired = co_await servicelib::grpc_transport::UnaryCall<
        &servicelib::test::ConnectorTest::Stub::PrepareAsyncUnary>(
        runtime.grpcContext(), *stub,
        servicelib::MessageContext{}
            .withStreamId("expired")
            .withDeadline(std::chrono::steady_clock::now() + 25ms),
        request);
    servicelib::test::EchoRequest failedRequest;
    failedRequest.set_value("fail");
    auto failed = co_await servicelib::grpc_transport::UnaryCall<
        &servicelib::test::ConnectorTest::Stub::PrepareAsyncUnary>(
        runtime.grpcContext(), *stub,
        servicelib::MessageContext{}.withStreamId("failed"), failedRequest);
    cancelledCode.store(static_cast<int>(cancelled.status.error_code()),
                        std::memory_order_release);
    expiredCode.store(static_cast<int>(expired.status.error_code()),
                      std::memory_order_release);
    midflightCancelledCode.store(
        static_cast<int>(midflightCancelled.status.error_code()),
        std::memory_order_release);
    failedCode.store(static_cast<int>(failed.status.error_code()),
                     std::memory_order_release);
    cancellationVerified.store(
        !midflightCancelled.ok() &&
            midflightCancelled.status.error_code() ==
                grpc::StatusCode::CANCELLED &&
            !cancelled.ok() &&
            cancelled.status.error_code() == grpc::StatusCode::CANCELLED &&
            !expired.ok() &&
            expired.status.error_code() ==
                grpc::StatusCode::DEADLINE_EXCEEDED &&
            !failed.ok() &&
            failed.status.error_code() == grpc::StatusCode::INTERNAL &&
            failed.status.error_message() == "unary handler failed",
        std::memory_order_release);
    resultPromise.set_value(std::move(result));
  };
  runtime.SpawnGrpc(runCalls());

  Require(receivedFuture.wait_for(3s) == std::future_status::ready,
          "unary server did not accept the first request");
  const auto suspensionDeadline = std::chrono::steady_clock::now() + 2s;
  while (metrics.observableGauge("runtime.worker_utilization", {}).value() !=
             0 &&
         std::chrono::steady_clock::now() < suspensionDeadline) {
    std::this_thread::sleep_for(1ms);
  }
  Require(metrics.observableGauge("runtime.worker_utilization", {}).value() ==
              0,
          "a suspended gRPC coroutine inflated worker utilization");
  releaseFirstRequest.Send();

  Require(receivedCancelledFuture.wait_for(3s) == std::future_status::ready,
          "unary server did not accept the cancellable request");
  auto receivedCancelled = receivedCancelledFuture.get();
  midflightCancellation.request_stop();
  const auto cancellationDeadline = std::chrono::steady_clock::now() + 2s;
  while (!receivedCancelled.cancelled() &&
         std::chrono::steady_clock::now() < cancellationDeadline) {
    std::this_thread::sleep_for(1ms);
  }
  Require(receivedCancelled.cancelled(),
          "accepted unary cancellation did not reach MessageContext");
  releaseCancelledRequest.Send();

  Require(resultFuture.wait_for(3s) == std::future_status::ready,
          "unary client calls did not complete");
  auto result = resultFuture.get();
  Require(result.ok(), "successful unary call returned an error");
  Require(result.response.value() == "echo:hello",
          "successful unary response payload differs");
  if (!cancellationVerified.load(std::memory_order_acquire)) {
    std::cerr << "cancelled code="
              << cancelledCode.load(std::memory_order_acquire)
              << ", midflight code="
              << midflightCancelledCode.load(std::memory_order_acquire)
              << ", expired code="
              << expiredCode.load(std::memory_order_acquire)
              << ", failed code="
              << failedCode.load(std::memory_order_acquire) << '\n';
  }
  Require(cancellationVerified.load(std::memory_order_acquire),
          "unary cancellation/deadline status differs");

  auto received = receivedFuture.get();
  Require(received.streamId() == "stream-42",
          "unary stream ID was not propagated");
  Require(!received.hasPriority(),
          "unary priority escaped process-local context");
  Require(received.deadline().has_value(),
          "unary deadline was not propagated");
  Require(received.samplingEnabled(),
          "unary sampling marker was not propagated");
  Require(received.trace().traceId == "4bf92f3577b34da6a3ce929d0e0e4736" &&
              received.trace().spanId == "00f067aa0ba902b7" &&
              received.trace().traceState == "vendor=value" &&
              received.trace().baggage == "tenant=acme",
          "unary W3C trace context or baggage was not propagated");

  {
    servicelib::grpc_transport::ClientPool<
        servicelib::test::ConnectorTest::Stub>
        pool{runtime.grpcContext(), "127.0.0.1:" + std::to_string(port), 2,
             [](std::shared_ptr<grpc::Channel> value) {
               return servicelib::test::ConnectorTest::NewStub(
                   std::move(value));
             }};

    servicelib::test::EchoRequest pooledRequest;
    pooledRequest.set_value("pooled");
    std::promise<std::pair<std::exception_ptr,
                           std::optional<servicelib::test::EchoResponse>>>
        pooledCompletion;
    auto pooledFuture = pooledCompletion.get_future();
    pool.asyncUnary<
        &servicelib::test::ConnectorTest::Stub::PrepareAsyncUnary,
        servicelib::test::EchoRequest, servicelib::test::EchoResponse>(
        std::move(pooledRequest),
        servicelib::datasink::grpc::callOptions(
            servicelib::MessageContext{}.withStreamId("pooled")),
        [&pooledCompletion](
            std::exception_ptr error,
            std::optional<servicelib::test::EchoResponse> response) {
          pooledCompletion.set_value(
              {std::move(error), std::move(response)});
        });
    Require(pooledFuture.wait_for(3s) == std::future_status::ready,
            "pooled unary call did not complete");
    auto [pooledError, pooledResponse] = pooledFuture.get();
    Require(!pooledError, "pooled unary call returned an error");
    Require(pooledResponse && pooledResponse->value() == "echo:pooled",
            "pooled unary response payload differs");

    using Completion = std::pair<std::exception_ptr,
                                  std::optional<servicelib::test::EchoResponse>>;
    std::vector<std::future<Completion>> concurrentResults;
    for (int index = 0; index < concurrentRequests; ++index) {
      auto completion = std::make_shared<std::promise<Completion>>();
      concurrentResults.push_back(completion->get_future());
      servicelib::test::EchoRequest concurrentRequest;
      concurrentRequest.set_value("concurrent-" + std::to_string(index));
      pool.asyncUnary<
          &servicelib::test::ConnectorTest::Stub::PrepareAsyncUnary,
          servicelib::test::EchoRequest, servicelib::test::EchoResponse>(
          std::move(concurrentRequest),
          servicelib::datasink::grpc::callOptions(
              servicelib::MessageContext{}.withDeadline(
                  std::chrono::steady_clock::now() + 5s)),
          [completion](std::exception_ptr error,
                       std::optional<servicelib::test::EchoResponse> response) {
            completion->set_value({std::move(error), std::move(response)});
          });
    }
    const bool acceptedAll =
        concurrentReadyFuture.wait_for(3s) == std::future_status::ready;
    releaseConcurrentRequests.Send();
    Require(acceptedAll, "gRPC acceptance waited for a previous business result");
    for (std::size_t index = 0; index < concurrentResults.size(); ++index) {
      Require(concurrentResults[index].wait_for(3s) == std::future_status::ready,
              "concurrent gRPC response did not complete");
      auto [error, response] = concurrentResults[index].get();
      Require(!error && response &&
                  response->value() == "echo:concurrent-" + std::to_string(index),
              "concurrent gRPC request ownership or response differs");
    }

    servicelib::test::EchoRequest failedPooledRequest;
    failedPooledRequest.set_value("fail");
    std::promise<std::exception_ptr> failedPooledCompletion;
    auto failedPooledFuture = failedPooledCompletion.get_future();
    pool.asyncUnary<
        &servicelib::test::ConnectorTest::Stub::PrepareAsyncUnary,
        servicelib::test::EchoRequest, servicelib::test::EchoResponse>(
        std::move(failedPooledRequest),
        servicelib::datasink::grpc::callOptions(servicelib::MessageContext{}),
        [&failedPooledCompletion](
            std::exception_ptr error,
            std::optional<servicelib::test::EchoResponse>) {
          failedPooledCompletion.set_value(std::move(error));
        });
    Require(failedPooledFuture.wait_for(3s) == std::future_status::ready,
            "failed pooled unary call did not complete");
    auto failedPooledError = failedPooledFuture.get();
    Require(static_cast<bool>(failedPooledError),
            "failed pooled unary call returned no error");
    try {
      std::rethrow_exception(failedPooledError);
    } catch (const servicelib::grpc_transport::StatusError& error) {
      Require(error.code() == grpc::StatusCode::INTERNAL,
              "failed pooled unary status differs");
    }

    servicelib::test::EchoRequest cancelledPooledRequest;
    cancelledPooledRequest.set_value("cancel-pooled");
    std::stop_source pooledCancellation;
    std::stop_source extraCancellation;
    std::stop_source lastCancellation;
    std::promise<std::exception_ptr> cancelledPooledCompletion;
    auto cancelledPooledFuture = cancelledPooledCompletion.get_future();
    pool.asyncUnary<
        &servicelib::test::ConnectorTest::Stub::PrepareAsyncUnary,
        servicelib::test::EchoRequest, servicelib::test::EchoResponse>(
        std::move(cancelledPooledRequest),
        servicelib::datasink::grpc::callOptions(
            servicelib::MessageContext{}
                .withStopToken(pooledCancellation.get_token())
                .withExternalCancellation(extraCancellation.get_token())
                .withExternalCancellation(lastCancellation.get_token())
                .withDeadline(std::chrono::steady_clock::now() + 3s)),
        [&cancelledPooledCompletion](
            std::exception_ptr error,
            std::optional<servicelib::test::EchoResponse>) {
          cancelledPooledCompletion.set_value(std::move(error));
        });
    Require(receivedPooledCancelledFuture.wait_for(3s) ==
                std::future_status::ready,
            "cancellable pooled unary call was not accepted");
    lastCancellation.request_stop();
    releasePooledCancelledRequest.Send();
    Require(cancelledPooledFuture.wait_for(3s) == std::future_status::ready,
            "cancelled pooled unary call did not complete");
    auto cancelledPooledError = cancelledPooledFuture.get();
    Require(static_cast<bool>(cancelledPooledError),
            "cancelled pooled unary call returned no error");
    try {
      std::rethrow_exception(cancelledPooledError);
    } catch (const servicelib::grpc_transport::StatusError& error) {
      Require(error.code() == grpc::StatusCode::CANCELLED,
              "cancelled pooled unary status differs");
    }

    // Every registered source must cancel, including the inline registration
    // and additional stable registrations, even when stopped before creation.
    for (std::size_t stopped = 0; stopped < 4; ++stopped) {
      std::array<std::stop_source, 4> sources;
      auto cancelledContext = servicelib::MessageContext{}
          .withStopToken(sources[0].get_token())
          .withExternalCancellation(sources[1].get_token())
          .withExternalCancellation(sources[2].get_token())
          .withExternalCancellation(sources[3].get_token())
          .withDeadline(std::chrono::steady_clock::now() + 3s);
      sources[stopped].request_stop();
      servicelib::test::EchoRequest cancelledRequest;
      cancelledRequest.set_value("already-cancelled");
      std::promise<std::exception_ptr> completion;
      auto future = completion.get_future();
      pool.asyncUnary<
          &servicelib::test::ConnectorTest::Stub::PrepareAsyncUnary,
          servicelib::test::EchoRequest, servicelib::test::EchoResponse>(
          std::move(cancelledRequest),
          servicelib::datasink::grpc::callOptions(cancelledContext),
          [&completion](std::exception_ptr error,
                        std::optional<servicelib::test::EchoResponse>) {
            completion.set_value(std::move(error));
          });
      Require(future.wait_for(3s) == std::future_status::ready,
              "an already-stopped cancellation source was ignored");
      auto error = future.get();
      Require(static_cast<bool>(error), "already-cancelled call succeeded");
      try {
        std::rethrow_exception(error);
      } catch (const servicelib::grpc_transport::StatusError& status) {
        Require(status.code() == grpc::StatusCode::CANCELLED,
                "already-cancelled unary status differs");
      }
    }
  }

  CheckConcurrentNestedSubStreams(runtime, port, subStreamProbe);
  CheckCancelledNestedSubStream(runtime, port, subStreamCancellationProbe, false);
  CheckCancelledNestedSubStream(runtime, port, subStreamCancellationProbe, true);
  bool shutdownFailed = false;
  const std::array scopes{StopScope::kEndpoint, StopScope::kConnector, StopScope::kLifecycle};
  for (std::size_t index = 0; index < scopes.size(); ++index) {
    try {
      CheckCooperativeEndpointStop(runtime, port, endpointStopProbes[index], scopes[index]);
    } catch (const std::exception& error) {
      std::cerr << "shutdown scope " << index << ": " << error.what() << '\n';
      shutdownFailed = true;
    }
  }
  Require(!shutdownFailed, "one or more gRPC shutdown scopes failed");
  CheckTransportDrainWaitersAndRestart();

  server->Shutdown();
  runtime.Stop();
  runtime.Join();
}

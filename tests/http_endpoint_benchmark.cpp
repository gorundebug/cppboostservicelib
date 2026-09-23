#include "connector_test.grpc.pb.h"
#include <servicelib/datasource/http/beast.hpp>
#include <servicelib/runtime/detail/grpc_client.hpp>
#include <servicelib/runtime/detail/grpc_runtime.hpp>
#include <servicelib/runtime/detail/grpc_transport.hpp>
#include <servicelib/runtime/environment/environment.hpp>
#include <servicelib/runtime/pool/delaypool.hpp>

#include <atomic>
#include <chrono>
#include <csignal>
#include <iostream>
#include <memory>
#include <optional>
#include <thread>

namespace asio = boost::asio;
namespace http = servicelib::http;
namespace source = servicelib::datasource::http;
using Context = servicelib::MessageContext;
using Service = servicelib::test::ConnectorTest;
using Pool = servicelib::grpc_transport::ClientPool<Service::Stub>;
using namespace std::chrono_literals;

namespace {
volatile std::sig_atomic_t stopped = 0;
void Stop(int) { stopped = 1; }

Context Deadline(Context context) {
  const auto deadline = std::chrono::steady_clock::now() + 5s;
  if (!context.deadline() || *context.deadline() > deadline)
    context = context.withDeadline(deadline);
  return context;
}

class Config final : public servicelib::config::IConfig {
 public:
  Config() {
    service.id = 1;
    service.name = "http-layer-diagnostic";
    connector.id = 10;
    connector.name = "http";
    connector.host = "0.0.0.0";
    connector.port = 9091;
    endpoint.id = 1;
    endpoint.name = "echo";
    endpoint.idDataConnector = connector.id;
    endpoint.httpMethodType = servicelib::api::HTTPMethodType::kPOST;
    endpoint.path = "/endpoint";
    remote = endpoint;
    remote.id = 2;
    remote.name = "echo-grpc";
    remote.path = "/endpoint-grpc";
    delayed = remote;
    delayed.id = 3;
    delayed.name = "echo-grpc-delay";
    delayed.path = "/endpoint-grpc-delay";
  }
  std::vector<const servicelib::config::ServiceConfig*> GetServices() const override { return {&service}; }
  std::vector<servicelib::config::StreamConfigRef> GetStreams() const override { return {}; }
  std::vector<servicelib::config::DataConnectorConfigRef> GetDataConnectors() const override { return {connector}; }
  std::vector<servicelib::config::EndpointConfigRef> GetEndpoints() const override { return {endpoint, remote, delayed}; }
  std::vector<const servicelib::config::PoolConfig*> GetPools() const override { return {}; }
  std::vector<const servicelib::config::LinkConfig*> GetLinks() const override { return {}; }
  std::vector<const servicelib::config::ModuleConfig*> GetModules() const override { return {}; }
  std::vector<const servicelib::config::TypeConfig*> GetTypes() const override { return {}; }
  servicelib::config::ServiceConfig service;
  servicelib::config::HttpDataConnectorConfig connector;
  servicelib::config::HttpEndpointConfig endpoint, remote, delayed;
};

class Environment final : public servicelib::IRuntimeEnvironment {
 public:
  Environment() : runtime_(std::make_shared<const servicelib::config::RuntimeConfig>(config_)),
                  service_(std::make_shared<const servicelib::config::ServiceConfig>(config_.service)) {}
  servicelib::pool::ITaskPool* getTaskPool(const std::string&) override { return nullptr; }
  servicelib::pool::IPriorityTaskPool* getPriorityTaskPool(const std::string&) override { return nullptr; }
  std::shared_ptr<const servicelib::config::RuntimeConfig> getRuntimeConfigSnapshot() const override { return runtime_; }
  std::shared_ptr<const servicelib::config::ServiceConfig> getServiceConfigSnapshot() const override { return service_; }
  servicelib::log::Logger& getLogger() override { return servicelib::log::NoopLogger::instance(); }
  servicelib::metrics::Metrics& getMetrics() override { return servicelib::metrics::NoopMetrics::instance(); }
  servicelib::tracing::Tracing* getTracing() override { return nullptr; }
 private:
  Config config_;
  std::shared_ptr<const servicelib::config::RuntimeConfig> runtime_;
  std::shared_ptr<const servicelib::config::ServiceConfig> service_;
};

struct Reply {
  std::string value;
  bool ok{};
};

struct Handler {
  using State = int;
  using Request = std::string;
  using Response = Reply;
  servicelib::BeginResult<State> beginRequest(Context context, auto&, source::HandlerData&) {
    return {Deadline(std::move(context)), 0};
  }
  void consumeMessage(Context context, auto& stream, State&, source::HandlerData& data, auto result) {
    result.setResultCallback("result", [result](Context, auto&, State&, const Reply& reply,
                                                source::HandlerData& response) mutable {
      response.response.status = reply.ok ? 200 : 502;
      response.response.contentType = "text/plain";
      response.setResponseBody(reply.value);
      result.done();
      return true;
    });
    stream.collect(std::move(context), data.request.body);
  }
  std::string getMessageId(Context, auto&, State&, const Reply&) { return "result"; }
  void endRequest(Context, auto&, std::exception_ptr error, State&, source::HandlerData& data) noexcept {
    if (error) data.response.status = 504;
  }
};
using Endpoint = source::BeastEndpoint<std::string, Reply, Handler>;

class EchoClient {
 public:
  explicit EchoClient(Pool& pool) : pool_(pool) {}
  template <class Callback>
  void Call(Context context, std::string value, Callback callback) {
    servicelib::test::EchoRequest request;
    request.set_value(std::move(value));
    active_.fetch_add(1);
    try {
      pool_.asyncUnary<&Service::Stub::PrepareAsyncUnary, servicelib::test::EchoRequest,
                       servicelib::test::EchoResponse>(
          std::move(request), servicelib::datasink::grpc::callOptions(context),
          [this, callback = std::move(callback)](std::exception_ptr error,
                              std::optional<servicelib::test::EchoResponse> response) mutable {
            struct Release {
              std::atomic<unsigned>& active;
              ~Release() { active.fetch_sub(1); }
            } release{active_};
            callback(Reply{response ? response->value() : std::string{}, !error && response.has_value()});
          });
    } catch (...) {
      active_.fetch_sub(1);
      throw;
    }
  }
  void Drain() const {
    const auto deadline = std::chrono::steady_clock::now() + 10s;
    while (active_.load() != 0) {
      if (std::chrono::steady_clock::now() > deadline) std::abort();
      std::this_thread::sleep_for(1ms);
    }
  }
 private:
  Pool& pool_;
  std::atomic<unsigned> active_{};
};

struct Exchange {
  Reply reply;
  servicelib::detail::SingleUseEvent done;
};
}  // namespace

int main(int argc, char** argv) {
  if (argc != 2) throw std::runtime_error("expected gRPC server address");
  servicelib::async::GrpcRuntime runtime({.workers = 2});
  runtime.Start();
  Environment environment;
  Pool pool(runtime.grpcContext(), argv[1], 2,
            [](std::shared_ptr<grpc::Channel> channel) { return Service::NewStub(std::move(channel)); });
  EchoClient client(pool);
  std::vector<std::unique_ptr<Service::Stub>> directStubs;
  for (int index = 0; index < 2; ++index)
    directStubs.push_back(Service::NewStub(grpc::CreateChannel(argv[1], grpc::InsecureChannelCredentials())));
  std::atomic<std::size_t> nextDirectStub{};
  std::unique_ptr<servicelib::pool::DelayPoolImpl> delays;
  std::atomic<std::uint64_t> scheduled{}, cancelled{}, expired{};
  std::stop_source shutdown;
  auto router = std::make_shared<http::Router>();
  // Deliberately use Add, not AddSync: every variant observes disconnects.
  router->Add("POST", "/plain", [&](http::Request request, Context context) -> asio::awaitable<http::Response> {
    context = Deadline(context.withExternalCancellation(shutdown.get_token()));
    co_return http::Response{context.cancelled() ? 504 : 200, {}, request.body, "text/plain", request.keepAlive};
  });
  auto directHandler = [&](http::Request request, Context context) -> asio::awaitable<http::Response> {
    context = Deadline(context.withExternalCancellation(shutdown.get_token()));
    // Diagnostic controls only: never remove correlation in production.
    if (request.path == "/grpc-await-no-id")
      context = std::move(context).withStreamId("");
    else if (request.path == "/grpc-await-fixed-id")
      context = std::move(context).withStreamId("diagnostic-fixed-id");
    servicelib::test::EchoRequest message;
    message.set_value(std::move(request.body));
    auto& stub = *directStubs[nextDirectStub.fetch_add(1, std::memory_order_relaxed) % directStubs.size()];
    auto result = co_await servicelib::grpc_transport::UnaryCall<&Service::Stub::PrepareAsyncUnary>(
        runtime.grpcContext(), stub, context, message);
    const bool cancelled = result.status.error_code() == grpc::StatusCode::CANCELLED ||
                           result.status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED;
    co_return http::Response{result.ok() ? 200 : (cancelled ? 504 : 502), {}, result.response.value(),
                            "text/plain", request.keepAlive};
  };
  router->Add("POST", "/grpc-await", directHandler);
  router->Add("POST", "/grpc-await-no-id", directHandler);
  router->Add("POST", "/grpc-await-fixed-id", directHandler);
  router->Add("POST", "/grpc", [&](http::Request request, Context context) -> asio::awaitable<http::Response> {
    context = Deadline(context.withExternalCancellation(shutdown.get_token()));
    auto exchange = std::make_shared<Exchange>();
    client.Call(context, std::move(request.body), [exchange](Reply reply) {
      exchange->reply = std::move(reply);
      exchange->done.Send();
    });
    try {
      co_await exchange->done.AsyncWait(context);
    } catch (...) {
      co_return http::Response{504, {}, {}, "text/plain", request.keepAlive};
    }
    if (!exchange->done.IsReady())
      co_return http::Response{504, {}, {}, "text/plain", request.keepAlive};
    co_return http::Response{exchange->reply.ok ? 200 : 502, {}, exchange->reply.value,
                            "text/plain", request.keepAlive};
  });
  auto immediateObserver = std::make_shared<std::weak_ptr<Endpoint>>();
  auto immediate = std::make_shared<Endpoint>(environment, 1, Handler{},
      [immediateObserver](Context context, servicelib::Payload<std::string> value) {
        if (auto endpoint = immediateObserver->lock())
          endpoint->consumeResult(std::move(context), servicelib::Payload<Reply>::make(Reply{value.get(), true}));
      }, true);
  *immediateObserver = immediate;
  auto remoteObserver = std::make_shared<std::weak_ptr<Endpoint>>();
  auto remote = std::make_shared<Endpoint>(environment, 2, Handler{},
      [&client, remoteObserver](Context context, servicelib::Payload<std::string> value) {
        client.Call(context, value.get(), [remoteObserver, context](Reply reply) {
          if (auto endpoint = remoteObserver->lock())
            endpoint->consumeResult(context, servicelib::Payload<Reply>::make(std::move(reply)));
        });
      }, true);
  *remoteObserver = remote;
  auto delayedObserver = std::make_shared<std::weak_ptr<Endpoint>>();
  auto delayed = std::make_shared<Endpoint>(environment, 3, Handler{},
      [&](Context context, servicelib::Payload<std::string> value) {
        client.Call(context, value.get(), [delayedObserver, context](Reply reply) {
          if (auto endpoint = delayedObserver->lock())
            endpoint->consumeResult(context, servicelib::Payload<Reply>::make(std::move(reply)));
        });
        // Same scheduler/cancellation path as Delay after a Split branch.
        // This is a layer diagnostic, not a substitute for the canonical graph.
        const auto remaining = *context.deadline() - std::chrono::steady_clock::now() - 1s;
        delays->delay(context, std::max(remaining, std::chrono::steady_clock::duration::zero()),
            [&, context, delayedObserver] {
              if (context.cancelled()) {
                cancelled.fetch_add(1, std::memory_order_relaxed);
              } else {
                expired.fetch_add(1, std::memory_order_relaxed);
                if (auto endpoint = delayedObserver->lock())
                  endpoint->consumeResult(context, servicelib::Payload<Reply>::make(Reply{"timeout", false}));
              }
            });
        scheduled.fetch_add(1, std::memory_order_relaxed);
      }, true);
  *delayedObserver = delayed;
  router->Add("POST", "/endpoint", [immediate](http::Request request, Context context) {
    return immediate->handle(std::move(request), std::move(context));
  });
  router->Add("POST", "/endpoint-grpc", [remote](http::Request request, Context context) {
    return remote->handle(std::move(request), std::move(context));
  });
  router->Add("POST", "/endpoint-grpc-delay", [delayed](http::Request request, Context context) {
    return delayed->handle(std::move(request), std::move(context));
  });
  router->AddSync("GET", "/stats", [&](http::Request, Context) {
    return http::Response{200, {},
        "scheduled=" + std::to_string(scheduled.load()) +
        " cancelled=" + std::to_string(cancelled.load()) +
        " expired=" + std::to_string(expired.load()) +
        " active=" + std::to_string(delays->activeTasksApprox()) + "\n", "text/plain", true};
  });
  http::Server::Options options;
  options.address = "0.0.0.0";
  options.port = 9091;
  http::Server server(runtime.executor(), router, options);
  delays = std::make_unique<servicelib::pool::DelayPoolImpl>(environment);
  delays->start(servicelib::Context{});
  immediate->start(servicelib::Context{});
  remote->start(servicelib::Context{});
  delayed->start(servicelib::Context{});
  server.Start();
  std::signal(SIGTERM, Stop);
  std::signal(SIGINT, Stop);
  std::cout << "ready" << std::endl;
  while (!stopped) std::this_thread::sleep_for(100ms);
  shutdown.request_stop();
  immediate->stop(servicelib::Context{});
  remote->stop(servicelib::Context{});
  delayed->stop(servicelib::Context{});
  server.Stop();
  client.Drain();
  delays->stop(servicelib::Context{});
  runtime.Stop();
  runtime.Join();
}

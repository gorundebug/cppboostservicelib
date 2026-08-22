#include <boost/asio/co_spawn.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/write.hpp>
#include <boost/asio/use_future.hpp>

#include <gtest/gtest.h>

#include <servicelib/datasource/http/beast.hpp>
#include <servicelib/datasink/http/beast.hpp>
#include <servicelib/runtime/detail/asio_dispatch.hpp>
#include <servicelib/runtime/environment/environment.hpp>
#include <servicelib/runtime/testlog/testlog.hpp>
#include <servicelib/runtime/testmetrics/testmetrics.hpp>
#include <servicelib/runtime/testtracing/testtracing.hpp>

#include "test_sink_endpoint_stream.hpp"

#include <chrono>
#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <exception>
#include <future>
#include <iostream>
#include <memory>
#include <optional>
#include <stop_token>
#include <string>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

namespace {

TEST(HttpTypes, GeneratedStreamIdsPreserveHexFormatAndUniqueness) {
  std::unordered_set<std::string> ids;
  for (std::size_t index = 0; index < 4096; ++index) {
    auto id = servicelib::http::NewStreamId();
    const auto separator = id.find('-');
    ASSERT_NE(separator, std::string::npos);
    ASSERT_NE(separator, 0);
    ASSERT_NE(separator + 1, id.size());
    const auto separatorIt =
        id.begin() + static_cast<std::string::difference_type>(separator);
    EXPECT_TRUE(std::all_of(id.begin(), separatorIt,
                            [](unsigned char value) {
                              return std::isxdigit(value) != 0 &&
                                     !std::isupper(value);
                            }));
    EXPECT_TRUE(std::all_of(separatorIt + 1, id.end(),
                            [](unsigned char value) {
                              return std::isxdigit(value) != 0 &&
                                     !std::isupper(value);
                            }));
    EXPECT_TRUE(ids.emplace(std::move(id)).second);
  }
}

boost::asio::awaitable<std::optional<servicelib::http::ClientErrorCode>>
CaptureClientError(
    boost::asio::awaitable<servicelib::http::Response> operation) {
  try {
    static_cast<void>(co_await std::move(operation));
    co_return std::nullopt;
  } catch (const servicelib::http::ClientError& error) {
    co_return error.code();
  }
}

class TestConfig final : public servicelib::config::IConfig {
 public:
  TestConfig() {
    connector.id = 10;
    connector.name = "http";
    connector.host = "127.0.0.1";
    connector.port = 8080;
    endpoint.id = 1;
    endpoint.name = "http-source";
    endpoint.idDataConnector = connector.id;
    endpoint.httpMethodType = servicelib::api::HTTPMethodType::kPOST;
    endpoint.path = "/orders";
  }

  std::vector<const servicelib::config::ServiceConfig*> GetServices()
      const override { return {}; }
  std::vector<servicelib::config::StreamConfigRef> GetStreams() const override {
    return {};
  }
  std::vector<servicelib::config::DataConnectorConfigRef> GetDataConnectors()
      const override { return {connector}; }
  std::vector<servicelib::config::EndpointConfigRef> GetEndpoints()
      const override { return {endpoint}; }
  std::vector<const servicelib::config::PoolConfig*> GetPools() const override {
    return {};
  }
  std::vector<const servicelib::config::LinkConfig*> GetLinks() const override {
    return {};
  }
  std::vector<const servicelib::config::ModuleConfig*> GetModules()
      const override { return {}; }
  std::vector<const servicelib::config::TypeConfig*> GetTypes() const override {
    return {};
  }

  servicelib::config::HttpDataConnectorConfig connector;
  servicelib::config::HttpEndpointConfig endpoint;
};

class TestEnvironment final : public servicelib::IRuntimeEnvironment {
 public:
  explicit TestEnvironment(
      servicelib::testtracing::TestTracing* tracing = nullptr)
      : runtimeConfig_(config_), tracing_(tracing) {
    service_.name = "http-test";
  }
  servicelib::pool::ITaskPool* getTaskPool(const std::string&) override {
    return nullptr;
  }
  servicelib::pool::IPriorityTaskPool* getPriorityTaskPool(
      const std::string&) override { return nullptr; }
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
  servicelib::tracing::Tracing* getTracing() override { return tracing_; }
  servicelib::testmetrics::TestMetrics& metrics() noexcept { return metrics_; }

 private:
  TestConfig config_;
  servicelib::config::RuntimeConfig runtimeConfig_;
  servicelib::config::ServiceConfig service_;
  servicelib::testlog::TestLog log_;
  servicelib::testmetrics::TestMetrics metrics_;
  servicelib::testtracing::TestTracing* tracing_{};
};

struct Handler final {
  using State = int;
  using Request = std::string;
  using Response = std::string;

  servicelib::BeginResult<State> beginRequest(
      servicelib::MessageContext context, auto&,
      servicelib::datasource::http::HandlerData&) {
    return {std::move(context), 0};
  }

  void consumeMessage(
      servicelib::MessageContext context, auto& stream, State&,
      servicelib::datasource::http::HandlerData& data, auto result) {
    result.setResultCallback(
        "result", [result](servicelib::MessageContext, auto&, State&,
                           const std::string& value,
                           servicelib::datasource::http::HandlerData& response)
                      mutable {
          response.setResponseBody("reply:" + value);
          result.done();
          return true;
        });
    stream.collect(std::move(context), data.request.body);
  }

  std::string getMessageId(servicelib::MessageContext, auto&, State&,
                           const std::string&) { return "result"; }

  void endRequest(servicelib::MessageContext, auto&, std::exception_ptr,
                  State&, servicelib::datasource::http::HandlerData&) noexcept {
  }
};

TEST(HttpDataSource, PreservesCanonicalHandlerAndCorrelationContract) {
  boost::asio::io_context io;
  servicelib::detail::ParallelExecutorRegistry::Set(io.get_executor());
  servicelib::testtracing::TestTracing tracing;
  TestEnvironment environment{&tracing};
  using Endpoint = servicelib::datasource::http::BeastEndpoint<
      std::string, std::string, Handler>;
  Endpoint* endpointPointer{};
  Endpoint endpoint{
      environment, 1, Handler{},
      [&](servicelib::MessageContext context,
          servicelib::Payload<std::string> value) {
        endpointPointer->consumeResult(
            std::move(context),
            servicelib::Payload<std::string>::make(value.get()));
      },
      true};
  endpointPointer = &endpoint;
  endpoint.start(servicelib::Context{});

  servicelib::http::Request request;
  request.method = "POST";
  request.target = "/orders";
  request.path = "/orders";
  request.body = "one";
  auto response = boost::asio::co_spawn(
      io,
      endpoint.handle(
          std::move(request),
          servicelib::tracing::EnableSampling(
              servicelib::MessageContext{}.withStreamId("http-stream"))),
      boost::asio::use_future);
  while (response.wait_for(std::chrono::milliseconds{0}) !=
         std::future_status::ready) {
    ASSERT_GT(io.run_one(), 0U);
  }

  EXPECT_EQ(response.get().body, "reply:one");
  const servicelib::metrics::Labels labels{{"connector", "http"},
                                            {"endpoint", "http-source"}};
  EXPECT_EQ(environment.metrics()
                .counter("datasource_endpoint.messages_total", labels)
                .count(),
            1);
  EXPECT_EQ(environment.metrics()
                .gauge("datasource_endpoint.active_requests", labels)
                .value(),
            0);
  EXPECT_EQ(environment.metrics()
                .gauge("datasource_endpoint.pending_requests", labels)
                .value(),
            0);
  EXPECT_EQ(environment.metrics()
                .histogram("datasource_endpoint.request_duration_seconds",
                           labels)
                .count(),
            1);
  const auto spans = tracing.spans();
  ASSERT_EQ(spans.size(), 1);
  EXPECT_EQ(spans.front().name, "http.input");
  EXPECT_EQ(spans.front().statusCode,
            servicelib::tracing::StatusCode::kUnset);
  EXPECT_TRUE(std::any_of(
      spans.front().events.begin(), spans.front().events.end(),
      [](const auto& event) { return event.name == "done_received"; }));
  endpoint.stop(servicelib::Context{});
  servicelib::detail::ParallelExecutorRegistry::Clear();
}

TEST(HttpDataSource, StopCancelsPendingCanonicalRequest) {
  boost::asio::io_context io;
  servicelib::detail::ParallelExecutorRegistry::Set(io.get_executor());
  {
    TestEnvironment environment;
    servicelib::MessageContext pipelineContext;
    using Endpoint = servicelib::datasource::http::BeastEndpoint<
        std::string, std::string, Handler>;
    Endpoint endpoint{
        environment, 1, Handler{},
        [&](servicelib::MessageContext context,
            servicelib::Payload<std::string>) {
          pipelineContext = std::move(context);
        },
        true};
    endpoint.start(servicelib::Context{});
    servicelib::http::Request request;
    request.method = "POST";
    request.target = "/orders";
    request.path = "/orders";
    request.body = "one";
    auto response = boost::asio::co_spawn(
        io, endpoint.handle(std::move(request), servicelib::MessageContext{}),
        boost::asio::use_future);
    while (pipelineContext.streamId().empty()) ASSERT_GT(io.run_one(), 0U);
    EXPECT_FALSE(pipelineContext.cancelled());
    endpoint.stop(servicelib::Context{});
    io.restart();
    while (response.wait_for(std::chrono::milliseconds{0}) !=
           std::future_status::ready) {
      ASSERT_GT(io.run_one(), 0U);
    }
    static_cast<void>(response.get());
    EXPECT_TRUE(pipelineContext.cancelled());
  }
  servicelib::detail::ParallelExecutorRegistry::Clear();
}

struct DisconnectHandler final {
  using State = int;
  using Request = std::string;
  using Response = std::string;

  int* endCalls{};
  int* resultCallbacks{};
  bool* endCancelled{};
  bool* endHadError{};
  servicelib::detail::SingleUseEvent* completed{};

  servicelib::BeginResult<State> beginRequest(
      servicelib::MessageContext context, auto&,
      servicelib::datasource::http::HandlerData&) {
    return {std::move(context), 0};
  }

  void consumeMessage(
      servicelib::MessageContext context, auto& stream, State&,
      servicelib::datasource::http::HandlerData& data, auto result) {
    result.setResultCallback(
        "result", [result, calls = resultCallbacks](
                      servicelib::MessageContext, auto&, State&,
                      const std::string&,
                      servicelib::datasource::http::HandlerData&) mutable {
          ++*calls;
          result.done();
          return true;
        });
    stream.collect(std::move(context), data.request.body);
  }

  std::string getMessageId(servicelib::MessageContext, auto&, State&,
                           const std::string&) {
    return "result";
  }

  void endRequest(servicelib::MessageContext context, auto&,
                  std::exception_ptr error, State&,
                  servicelib::datasource::http::HandlerData&) noexcept {
    ++*endCalls;
    *endCancelled = context.cancelled();
    *endHadError = static_cast<bool>(error);
    completed->Send();
  }
};

boost::asio::awaitable<void> SendThenDisconnect(
    std::uint16_t port, servicelib::detail::SingleUseEvent& accepted) {
  const auto executor = co_await boost::asio::this_coro::executor;
  boost::asio::ip::tcp::socket socket(executor);
  co_await socket.async_connect(
      {boost::asio::ip::make_address("127.0.0.1"), port},
      boost::asio::use_awaitable);
  const std::string request =
      "POST /orders HTTP/1.1\r\nHost: localhost\r\nContent-Length: 3\r\n"
      "Connection: keep-alive\r\n\r\none";
  co_await boost::asio::async_write(socket, boost::asio::buffer(request),
                                    boost::asio::use_awaitable);
  co_await accepted.AsyncWait();
  boost::system::error_code ignored;
  socket.shutdown(boost::asio::ip::tcp::socket::shutdown_both, ignored);
  socket.close(ignored);
}

TEST(HttpDataSource,
     ClientDisconnectCancelsAcceptedRequestAndRetiresCorrelation) {
  boost::asio::io_context io;
  auto workGuard = boost::asio::make_work_guard(io);
  servicelib::detail::ParallelExecutorRegistry::Set(io.get_executor());
  TestEnvironment environment;
  servicelib::detail::SingleUseEvent accepted;
  servicelib::detail::SingleUseEvent completed;
  int endCalls = 0;
  int resultCallbacks = 0;
  bool endCancelled = false;
  bool endHadError = false;
  servicelib::MessageContext pipelineContext;

  using Endpoint = servicelib::datasource::http::BeastEndpoint<
      std::string, std::string, DisconnectHandler>;
  Endpoint endpoint{
      environment, 1,
      DisconnectHandler{&endCalls, &resultCallbacks, &endCancelled,
                        &endHadError, &completed},
      [&](servicelib::MessageContext context,
          servicelib::Payload<std::string>) {
        pipelineContext = std::move(context);
        accepted.Send();
      },
      true};
  endpoint.start(servicelib::Context{});

  auto router = std::make_shared<servicelib::http::Router>();
  router->Add(
      "POST", "/orders",
      [&endpoint](servicelib::http::Request request,
                  servicelib::MessageContext context) {
        return endpoint.handle(std::move(request), std::move(context));
      });
  servicelib::http::Server::Options options;
  options.address = "127.0.0.1";
  options.port = 0;
  servicelib::http::Server server(io.get_executor(), std::move(router),
                                  options);
  server.Start();

  auto client = boost::asio::co_spawn(
      io, SendThenDisconnect(server.port(), accepted), boost::asio::use_future);
  std::jthread ioThread([&] { io.run(); });
  ASSERT_TRUE(completed.WaitUntil(std::chrono::steady_clock::now() +
                                  std::chrono::seconds{2}));
  ASSERT_EQ(client.wait_for(std::chrono::seconds{2}),
            std::future_status::ready);
  EXPECT_NO_THROW(client.get());

  EXPECT_TRUE(pipelineContext.cancelled());
  EXPECT_EQ(endCalls, 1);
  EXPECT_TRUE(endCancelled);
  EXPECT_TRUE(endHadError);
  endpoint.consumeResult(
      pipelineContext,
      servicelib::Payload<std::string>::make(std::string{"late"}));
  EXPECT_EQ(resultCallbacks, 0);
  EXPECT_EQ(endCalls, 1);

  endpoint.stop(servicelib::Context{});
  server.Stop();
  workGuard.reset();
  io.stop();
  servicelib::detail::ParallelExecutorRegistry::Clear();
}

boost::asio::awaitable<std::string> SendRequest(std::uint16_t port) {
  const auto executor = co_await boost::asio::this_coro::executor;
  boost::beast::tcp_stream stream(executor);
  co_await stream.async_connect(
      boost::asio::ip::tcp::endpoint{
          boost::asio::ip::make_address("127.0.0.1"), port},
      boost::asio::use_awaitable);
  boost::beast::http::request<boost::beast::http::string_body> request{
      boost::beast::http::verb::post, "/wait", 11};
  request.set(boost::beast::http::field::host, "localhost");
  request.keep_alive(true);
  request.prepare_payload();
  co_await boost::beast::http::async_write(stream, request,
                                           boost::asio::use_awaitable);
  boost::beast::flat_buffer buffer;
  boost::beast::http::response<boost::beast::http::string_body> response;
  co_await boost::beast::http::async_read(stream, buffer, response,
                                          boost::asio::use_awaitable);
  co_return response.body();
}

boost::asio::awaitable<void> ServeTwoRequestsOnOneConnection(
    boost::asio::ip::tcp::acceptor& acceptor) {
  auto socket = co_await acceptor.async_accept(boost::asio::use_awaitable);
  boost::beast::tcp_stream stream(std::move(socket));
  boost::beast::flat_buffer buffer;
  for (int requestNumber = 0; requestNumber < 2; ++requestNumber) {
    boost::beast::http::request<boost::beast::http::string_body> request;
    co_await boost::beast::http::async_read(stream, buffer, request,
                                            boost::asio::use_awaitable);
    boost::beast::http::response<boost::beast::http::string_body> response{
        boost::beast::http::status::ok, 11};
    response.body() = std::to_string(requestNumber + 1);
    response.keep_alive(requestNumber == 0);
    response.prepare_payload();
    co_await boost::beast::http::async_write(stream, response,
                                             boost::asio::use_awaitable);
  }
}

boost::asio::awaitable<std::size_t> RunHttpClientLoad(
    servicelib::http::Client& client, std::uint16_t port,
    std::chrono::steady_clock::time_point deadline) {
  const std::string body(16 * 1024, 'x');
  std::size_t requests{};
  while (std::chrono::steady_clock::now() < deadline) {
    servicelib::http::Request request;
    request.method = "POST";
    request.target = "/hot";
    request.body = body;
    auto response = co_await client.Send("127.0.0.1", std::to_string(port),
                                         std::move(request));
    if (response.status != 200) {
      throw std::runtime_error("profiling HTTP response is not 200");
    }
    ++requests;
  }
  co_return requests;
}

TEST(HttpServer, GracefulStopDrainsAcceptedRequestAndClosesKeepAlive) {
  boost::asio::io_context io;
  servicelib::detail::SingleUseEvent accepted;
  servicelib::detail::SingleUseEvent release;
  auto router = std::make_shared<servicelib::http::Router>();
  router->Add(
      "POST", "/wait",
      [&](servicelib::http::Request, servicelib::MessageContext)
          -> boost::asio::awaitable<servicelib::http::Response> {
        accepted.Send();
        co_await release.AsyncWait();
        co_return servicelib::http::Response{200, {}, "drained", "text/plain",
                                             true};
      });
  servicelib::http::Server::Options options;
  options.address = "127.0.0.1";
  options.port = 0;
  options.shutdownTimeout = std::chrono::milliseconds{500};
  servicelib::http::Server server(io.get_executor(), std::move(router),
                                  options);
  server.Start();
  auto request = boost::asio::co_spawn(
      io, SendRequest(server.port()), boost::asio::use_future);
  std::jthread ioThread([&] { io.run(); });
  ASSERT_TRUE(accepted.WaitUntil(std::chrono::steady_clock::now() +
                                 std::chrono::seconds{2}));

  auto stopped = std::async(std::launch::async, [&] { server.Stop(); });
  EXPECT_EQ(stopped.wait_for(std::chrono::milliseconds{30}),
            std::future_status::timeout);
  release.Send();

  ASSERT_EQ(request.wait_for(std::chrono::seconds{2}),
            std::future_status::ready);
  EXPECT_EQ(request.get(), "drained");
  ASSERT_EQ(stopped.wait_for(std::chrono::seconds{2}),
            std::future_status::ready);
  stopped.get();
  EXPECT_FALSE(server.running());
  io.stop();
}

TEST(HttpServer, ShutdownDeadlineForcesCancellationOfAcceptedRequest) {
  boost::asio::io_context io;
  servicelib::detail::SingleUseEvent accepted;
  servicelib::detail::SingleUseEvent never;
  servicelib::detail::SingleUseEvent retired;
  std::atomic<bool> cancelled{};
  auto router = std::make_shared<servicelib::http::Router>();
  router->Add(
      "POST", "/wait",
      [&](servicelib::http::Request, servicelib::MessageContext context)
          -> boost::asio::awaitable<servicelib::http::Response> {
        accepted.Send();
        co_await never.AsyncWait(context);
        cancelled.store(context.cancelled(), std::memory_order_release);
        retired.Send();
        co_return servicelib::http::Response{499, {}, "cancelled", "text/plain",
                                             false};
      });
  servicelib::http::Server::Options options;
  options.address = "127.0.0.1";
  options.port = 0;
  options.shutdownTimeout = std::chrono::milliseconds{30};
  servicelib::http::Server server(io.get_executor(), std::move(router),
                                  options);
  server.Start();
  auto request = boost::asio::co_spawn(
      io, SendRequest(server.port()), boost::asio::use_future);
  std::jthread ioThread([&] { io.run(); });
  ASSERT_TRUE(accepted.WaitUntil(std::chrono::steady_clock::now() +
                                 std::chrono::seconds{2}));

  const auto started = std::chrono::steady_clock::now();
  server.Stop();
  const auto elapsed = std::chrono::steady_clock::now() - started;
  EXPECT_GE(elapsed, std::chrono::milliseconds{20});
  EXPECT_LT(elapsed, std::chrono::seconds{2});
  ASSERT_TRUE(retired.WaitUntil(std::chrono::steady_clock::now() +
                                std::chrono::seconds{2}));
  EXPECT_TRUE(cancelled.load(std::memory_order_acquire));
  EXPECT_EQ(request.wait_for(std::chrono::seconds{2}),
            std::future_status::ready);
  EXPECT_THROW(static_cast<void>(request.get()), std::exception);
  io.stop();
}

TEST(HttpServer, RoutesGetPostKeepsConnectionAndEnforcesBodyLimit) {
  boost::asio::io_context io;
  auto router = std::make_shared<servicelib::http::Router>();
  router->Add(
      "GET", "/items",
      [](servicelib::http::Request request, servicelib::MessageContext)
          -> boost::asio::awaitable<servicelib::http::Response> {
        co_return servicelib::http::Response{
            200, {}, request.target, "application/json", true};
      });
  router->Add(
      "POST", "/items",
      [](servicelib::http::Request request, servicelib::MessageContext)
          -> boost::asio::awaitable<servicelib::http::Response> {
        co_return servicelib::http::Response{
            201, {}, request.body, "application/json", true};
      });
  servicelib::http::Server::Options serverOptions;
  serverOptions.address = "127.0.0.1";
  serverOptions.port = 0;
  serverOptions.bodyLimit = 3;
  servicelib::http::Server server(io.get_executor(), std::move(router),
                                  serverOptions);
  server.Start();
  servicelib::http::Client::Options clientOptions;
  clientOptions.connections = 1;
  servicelib::http::Client client(io.get_executor(), clientOptions);
  std::jthread ioThread([&] { io.run(); });
  const auto send = [&](std::string method, std::string target,
                        std::string body = {}) {
    servicelib::http::Request request;
    request.method = std::move(method);
    request.target = std::move(target);
    request.body = std::move(body);
    auto response = boost::asio::co_spawn(
        io,
        client.Send("127.0.0.1", std::to_string(server.port()),
                    std::move(request)),
        boost::asio::use_future);
    EXPECT_EQ(response.wait_for(std::chrono::seconds{2}),
              std::future_status::ready);
    return response.get();
  };

  const auto get = send("GET", "/items?kind=all");
  EXPECT_EQ(get.status, 200);
  EXPECT_EQ(get.body, "/items?kind=all");
  EXPECT_EQ(get.contentType, "application/json");
  const auto post = send("POST", "/items", "abc");
  EXPECT_EQ(post.status, 201);
  EXPECT_EQ(post.body, "abc");
  EXPECT_EQ(send("PUT", "/items").status, 405);
  EXPECT_EQ(send("GET", "/missing").status, 404);
  EXPECT_EQ(server.acceptedConnections(), 1U);
  const auto tooLarge = send("POST", "/items", "abcd");
  EXPECT_EQ(tooLarge.status, 413);
  EXPECT_FALSE(tooLarge.keepAlive);
  client.Stop();
  server.Stop();
  io.stop();
}

TEST(HttpServer, DispatchesBusinessHandlerOnWorkerExecutorOutsideSocketStrand) {
  boost::asio::io_context io;
  const boost::asio::any_io_executor workerExecutor = io.get_executor();
  std::atomic<bool> handlerUsedWorkerExecutor{};
  auto router = std::make_shared<servicelib::http::Router>();
  router->Add(
      "POST", "/wait",
      [&](servicelib::http::Request, servicelib::MessageContext)
          -> boost::asio::awaitable<servicelib::http::Response> {
        const boost::asio::any_io_executor handlerExecutor =
            co_await boost::asio::this_coro::executor;
        handlerUsedWorkerExecutor.store(handlerExecutor == workerExecutor,
                                        std::memory_order_release);
        co_return servicelib::http::Response{200, {}, "worker", "text/plain",
                                             true};
      });
  servicelib::http::Server::Options options;
  options.address = "127.0.0.1";
  options.port = 0;
  servicelib::http::Server server(workerExecutor, std::move(router), options);
  server.Start();
  servicelib::http::Client client(workerExecutor);
  servicelib::http::Request request;
  request.method = "POST";
  request.target = "/wait";
  auto response = boost::asio::co_spawn(
      io,
      client.Send("127.0.0.1", std::to_string(server.port()),
                  std::move(request)),
      boost::asio::use_future);
  std::jthread ioThread([&] { io.run(); });

  ASSERT_EQ(response.wait_for(std::chrono::seconds{2}),
            std::future_status::ready);
  EXPECT_EQ(response.get().body, "worker");
  EXPECT_TRUE(handlerUsedWorkerExecutor.load(std::memory_order_acquire));

  client.Stop();
  server.Stop();
  io.stop();
}

TEST(HttpClient, StopCancelsAndJoinsAcceptedSendBeforeReturning) {
  boost::asio::io_context io;
  servicelib::detail::SingleUseEvent accepted;
  servicelib::detail::SingleUseEvent never;
  servicelib::detail::SingleUseEvent retired;
  std::atomic<bool> serverContextCancelled{};
  auto router = std::make_shared<servicelib::http::Router>();
  router->Add(
      "POST", "/wait",
      [&](servicelib::http::Request, servicelib::MessageContext context)
          -> boost::asio::awaitable<servicelib::http::Response> {
        accepted.Send();
        co_await never.AsyncWait(context);
        serverContextCancelled.store(context.cancelled(),
                                     std::memory_order_release);
        retired.Send();
        co_return servicelib::http::Response{499, {}, "cancelled", "text/plain",
                                             false};
      });
  servicelib::http::Server::Options serverOptions;
  serverOptions.address = "127.0.0.1";
  serverOptions.port = 0;
  servicelib::http::Server server(io.get_executor(), std::move(router),
                                  serverOptions);
  server.Start();
  servicelib::http::Client client(io.get_executor());
  servicelib::http::Request transportRequest;
  transportRequest.method = "POST";
  transportRequest.target = "/wait";
  transportRequest.path = "/wait";
  auto response = boost::asio::co_spawn(
      io,
      CaptureClientError(client.Send("127.0.0.1", std::to_string(server.port()),
                                     std::move(transportRequest))),
      boost::asio::use_future);
  std::jthread ioThread([&] { io.run(); });
  ASSERT_TRUE(accepted.WaitUntil(std::chrono::steady_clock::now() +
                                 std::chrono::seconds{2}));

  auto stopped = std::async(std::launch::async, [&] { client.Stop(); });
  ASSERT_EQ(stopped.wait_for(std::chrono::seconds{2}),
            std::future_status::ready);
  stopped.get();
  ASSERT_EQ(response.wait_for(std::chrono::seconds{2}),
            std::future_status::ready);
  EXPECT_EQ(response.get(), servicelib::http::ClientErrorCode::kStopped);
  ASSERT_TRUE(retired.WaitUntil(std::chrono::steady_clock::now() +
                                std::chrono::seconds{2}));
  EXPECT_TRUE(serverContextCancelled.load(std::memory_order_acquire));

  servicelib::http::Request rejectedRequest;
  rejectedRequest.method = "GET";
  rejectedRequest.target = "/wait";
  auto rejected = boost::asio::co_spawn(
      io,
      CaptureClientError(client.Send("127.0.0.1", std::to_string(server.port()),
                                     std::move(rejectedRequest))),
      boost::asio::use_future);
  ASSERT_EQ(rejected.wait_for(std::chrono::seconds{2}),
            std::future_status::ready);
  EXPECT_EQ(rejected.get(), servicelib::http::ClientErrorCode::kStopped);
  server.Stop();
  io.stop();
}

TEST(HttpClient, RequestDeadlineCancelsAcceptedReadAndMapsTimeout) {
  boost::asio::io_context io;
  servicelib::detail::SingleUseEvent accepted;
  servicelib::detail::SingleUseEvent never;
  auto router = std::make_shared<servicelib::http::Router>();
  router->Add(
      "GET", "/wait",
      [&](servicelib::http::Request, servicelib::MessageContext context)
          -> boost::asio::awaitable<servicelib::http::Response> {
        accepted.Send();
        co_await never.AsyncWait(context);
        co_return servicelib::http::Response{499, {}, "cancelled", "text/plain",
                                             false};
      });
  servicelib::http::Server::Options serverOptions;
  serverOptions.address = "127.0.0.1";
  serverOptions.port = 0;
  servicelib::http::Server server(io.get_executor(), std::move(router),
                                  serverOptions);
  server.Start();
  servicelib::http::Client::Options clientOptions;
  clientOptions.timeout = std::chrono::seconds{2};
  servicelib::http::Client client(io.get_executor(), clientOptions);
  servicelib::http::Request request;
  request.method = "GET";
  request.target = "/wait";
  auto response = boost::asio::co_spawn(
      io,
      CaptureClientError(client.Send(
          "127.0.0.1", std::to_string(server.port()), std::move(request),
          servicelib::MessageContext{}.withDeadline(
              std::chrono::steady_clock::now() +
              std::chrono::milliseconds{40}))),
      boost::asio::use_future);
  std::jthread ioThread([&] { io.run(); });
  ASSERT_TRUE(accepted.WaitUntil(std::chrono::steady_clock::now() +
                                 std::chrono::seconds{2}));
  ASSERT_EQ(response.wait_for(std::chrono::seconds{2}),
            std::future_status::ready);
  EXPECT_EQ(response.get(), servicelib::http::ClientErrorCode::kTimeout);
  client.Stop();
  server.Stop();
  io.stop();
}

TEST(HttpClient, ExternalCancellationInterruptsAcceptedRead) {
  boost::asio::io_context io;
  servicelib::detail::SingleUseEvent accepted;
  servicelib::detail::SingleUseEvent never;
  auto router = std::make_shared<servicelib::http::Router>();
  router->Add(
      "GET", "/wait",
      [&](servicelib::http::Request, servicelib::MessageContext context)
          -> boost::asio::awaitable<servicelib::http::Response> {
        accepted.Send();
        co_await never.AsyncWait(context);
        co_return servicelib::http::Response{499, {}, "cancelled", "text/plain",
                                             false};
      });
  servicelib::http::Server::Options serverOptions;
  serverOptions.address = "127.0.0.1";
  serverOptions.port = 0;
  servicelib::http::Server server(io.get_executor(), std::move(router),
                                  serverOptions);
  server.Start();
  servicelib::http::Client::Options clientOptions;
  clientOptions.timeout = std::chrono::seconds{2};
  servicelib::http::Client client(io.get_executor(), clientOptions);
  std::stop_source cancelled;
  servicelib::http::Request request;
  request.method = "GET";
  request.target = "/wait";
  auto response = boost::asio::co_spawn(
      io,
      CaptureClientError(client.Send(
          "127.0.0.1", std::to_string(server.port()), std::move(request),
          servicelib::MessageContext{}.withStopToken(cancelled.get_token()))),
      boost::asio::use_future);
  std::jthread ioThread([&] { io.run(); });
  ASSERT_TRUE(accepted.WaitUntil(std::chrono::steady_clock::now() +
                                 std::chrono::seconds{2}));
  cancelled.request_stop();
  ASSERT_EQ(response.wait_for(std::chrono::seconds{2}),
            std::future_status::ready);
  EXPECT_EQ(response.get(), servicelib::http::ClientErrorCode::kCancelled);
  client.Stop();
  server.Stop();
  io.stop();
}

TEST(HttpClient, PoolLimitQueuesAndHonorsAcquisitionDeadline) {
  boost::asio::io_context io;
  servicelib::detail::SingleUseEvent accepted;
  servicelib::detail::SingleUseEvent release;
  auto router = std::make_shared<servicelib::http::Router>();
  router->Add(
      "GET", "/hold",
      [&](servicelib::http::Request, servicelib::MessageContext)
          -> boost::asio::awaitable<servicelib::http::Response> {
        accepted.Send();
        co_await release.AsyncWait();
        co_return servicelib::http::Response{200, {}, "ok", "text/plain",
                                             true};
      });
  servicelib::http::Server::Options serverOptions;
  serverOptions.address = "127.0.0.1";
  serverOptions.port = 0;
  servicelib::http::Server server(io.get_executor(), std::move(router),
                                  serverOptions);
  server.Start();
  servicelib::http::Client::Options clientOptions;
  clientOptions.connections = 1;
  clientOptions.timeout = std::chrono::seconds{2};
  servicelib::http::Client client(io.get_executor(), clientOptions);
  servicelib::http::Request firstRequest;
  firstRequest.method = "GET";
  firstRequest.target = "/hold";
  auto first = boost::asio::co_spawn(
      io, client.Send("127.0.0.1", std::to_string(server.port()),
                      std::move(firstRequest)),
      boost::asio::use_future);
  std::jthread ioThread([&] { io.run(); });
  ASSERT_TRUE(accepted.WaitUntil(std::chrono::steady_clock::now() +
                                 std::chrono::seconds{2}));

  servicelib::http::Request secondRequest;
  secondRequest.method = "GET";
  secondRequest.target = "/hold";
  auto second = boost::asio::co_spawn(
      io,
      CaptureClientError(client.Send(
          "127.0.0.1", std::to_string(server.port()),
          std::move(secondRequest),
          servicelib::MessageContext{}.withDeadline(
              std::chrono::steady_clock::now() +
              std::chrono::milliseconds{40}))),
      boost::asio::use_future);
  ASSERT_EQ(second.wait_for(std::chrono::seconds{2}),
            std::future_status::ready);
  EXPECT_EQ(second.get(), servicelib::http::ClientErrorCode::kPoolTimeout);
  EXPECT_EQ(client.connectionCount(), 1U);
  release.Send();
  ASSERT_EQ(first.wait_for(std::chrono::seconds{2}),
            std::future_status::ready);
  EXPECT_EQ(first.get().status, 200);
  client.Stop();
  server.Stop();
  io.stop();
}

TEST(HttpClient, ReusesKeepAliveConnectionWithinConfiguredPool) {
  boost::asio::io_context io;
  boost::asio::ip::tcp::acceptor acceptor(
      io, {boost::asio::ip::make_address("127.0.0.1"), 0});
  const auto port = acceptor.local_endpoint().port();
  auto served = boost::asio::co_spawn(
      io, ServeTwoRequestsOnOneConnection(acceptor), boost::asio::use_future);
  servicelib::http::Client::Options options;
  options.connections = 1;
  options.timeout = std::chrono::seconds{1};
  servicelib::http::Client client(io.get_executor(), options);
  std::jthread ioThread([&] { io.run(); });

  servicelib::http::Request firstRequest;
  firstRequest.method = "GET";
  firstRequest.target = "/first";
  auto first = boost::asio::co_spawn(
      io, client.Send("localhost", std::to_string(port),
                      std::move(firstRequest)),
      boost::asio::use_future);
  ASSERT_EQ(first.wait_for(std::chrono::seconds{2}),
            std::future_status::ready);
  EXPECT_EQ(first.get().body, "1");
  EXPECT_EQ(client.connectionCount(), 1U);

  servicelib::http::Request secondRequest;
  secondRequest.method = "GET";
  secondRequest.target = "/second";
  auto second = boost::asio::co_spawn(
      io, client.Send("localhost", std::to_string(port),
                      std::move(secondRequest)),
      boost::asio::use_future);
  ASSERT_EQ(second.wait_for(std::chrono::seconds{2}),
            std::future_status::ready);
  EXPECT_EQ(second.get().body, "2");
  ASSERT_EQ(served.wait_for(std::chrono::seconds{2}),
            std::future_status::ready);
  EXPECT_NO_THROW(served.get());
  EXPECT_EQ(client.connectionCount(), 1U);
  client.Stop();
  acceptor.close();
  io.stop();
}

TEST(HttpClient, MapsResolveConnectAndBodyLimitErrors) {
  boost::asio::io_context io;
  servicelib::http::Client::Options options;
  options.timeout = std::chrono::seconds{1};
  options.responseBodyLimit = 3;
  servicelib::http::Client client(io.get_executor(), options);

  servicelib::http::Request resolveRequest;
  resolveRequest.method = "GET";
  resolveRequest.target = "/";
  auto resolve = boost::asio::co_spawn(
      io,
      CaptureClientError(
          client.Send("invalid host name", "80", std::move(resolveRequest))),
      boost::asio::use_future);
  io.run();
  ASSERT_EQ(resolve.wait_for(std::chrono::seconds{0}),
            std::future_status::ready);
  EXPECT_EQ(resolve.get(), servicelib::http::ClientErrorCode::kResolve);

  io.restart();
  boost::asio::ip::tcp::acceptor unused(
      io, {boost::asio::ip::make_address("127.0.0.1"), 0});
  const auto unusedPort = unused.local_endpoint().port();
  unused.close();
  servicelib::http::Request connectRequest;
  connectRequest.method = "GET";
  connectRequest.target = "/";
  auto connect = boost::asio::co_spawn(
      io, CaptureClientError(client.Send("127.0.0.1",
                                         std::to_string(unusedPort),
                                         std::move(connectRequest))),
      boost::asio::use_future);
  io.run();
  ASSERT_EQ(connect.wait_for(std::chrono::seconds{0}),
            std::future_status::ready);
  EXPECT_EQ(connect.get(), servicelib::http::ClientErrorCode::kConnect);

  io.restart();
  auto router = std::make_shared<servicelib::http::Router>();
  router->Add(
      "GET", "/large",
      [](servicelib::http::Request, servicelib::MessageContext)
          -> boost::asio::awaitable<servicelib::http::Response> {
        co_return servicelib::http::Response{200, {}, "large", "text/plain",
                                             false};
      });
  servicelib::http::Server::Options serverOptions;
  serverOptions.address = "127.0.0.1";
  serverOptions.port = 0;
  servicelib::http::Server server(io.get_executor(), std::move(router),
                                  serverOptions);
  server.Start();
  servicelib::http::Request bodyRequest;
  bodyRequest.method = "GET";
  bodyRequest.target = "/large";
  auto body = boost::asio::co_spawn(
      io, CaptureClientError(client.Send("127.0.0.1",
                                         std::to_string(server.port()),
                                         std::move(bodyRequest))),
      boost::asio::use_future);
  std::jthread ioThread([&] { io.run(); });
  ASSERT_EQ(body.wait_for(std::chrono::seconds{2}),
            std::future_status::ready);
  EXPECT_EQ(body.get(), servicelib::http::ClientErrorCode::kBodyLimit);
  client.Stop();
  server.Stop();
  io.stop();
}

TEST(HttpContext, PropagatesSupportedFieldsAndKeepsPriorityLocal) {
  servicelib::http::Headers incoming{
      {"x-stream-id", "order-42"},
      {"x-priority", "-50"},
      {"x-timeout-ms", "5000"},
      {"x-trace", "1"},
      {"traceparent",
       "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"},
      {"tracestate", "vendor=value"},
      {"baggage", "tenant=acme"},
      {"authorization", "do-not-forward"},
  };

  auto context = servicelib::http::ContextFromHeaders(incoming);
  EXPECT_EQ(context.streamId(), "order-42");
  EXPECT_FALSE(context.hasPriority());
  ASSERT_TRUE(context.deadline().has_value());
  EXPECT_TRUE(context.samplingEnabled());
  EXPECT_EQ(context.trace().traceId, "4bf92f3577b34da6a3ce929d0e0e4736");
  EXPECT_EQ(context.trace().spanId, "00f067aa0ba902b7");
  EXPECT_EQ(context.trace().traceState, "vendor=value");
  EXPECT_EQ(context.trace().baggage, "tenant=acme");

  context = context.withPriority(17);
  servicelib::http::Headers outgoing;
  servicelib::http::InjectContext(context, outgoing);
  EXPECT_EQ(outgoing["x-stream-id"], "order-42");
  EXPECT_EQ(outgoing["x-trace"], "1");
  EXPECT_EQ(outgoing["traceparent"],
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01");
  EXPECT_EQ(outgoing["tracestate"], "vendor=value");
  EXPECT_EQ(outgoing["baggage"], "tenant=acme");
  EXPECT_TRUE(outgoing.contains("x-timeout-ms"));
  EXPECT_FALSE(outgoing.contains("x-priority"));
  EXPECT_FALSE(outgoing.contains("authorization"));
}

TEST(HttpContext, DisabledTracingKeepsOnlyTransportFields) {
  servicelib::http::Headers incoming{
      {"x-stream-id", "order-42"},
      {"x-timeout-ms", "5000"},
      {"x-trace", "1"},
      {"traceparent",
       "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"},
      {"tracestate", "vendor=value"},
      {"baggage", "tenant=acme"},
  };

  const auto context = servicelib::http::ContextFromHeaders(incoming, false);
  EXPECT_EQ(context.streamId(), "order-42");
  ASSERT_TRUE(context.deadline().has_value());
  EXPECT_FALSE(context.samplingEnabled());
  EXPECT_FALSE(context.trace().isValid());
  EXPECT_TRUE(context.trace().traceState.empty());
  EXPECT_TRUE(context.trace().baggage.empty());
}

TEST(HttpContext, RoundTripsSupportedPropagationOverRealTcp) {
  boost::asio::io_context io;
  std::optional<servicelib::MessageContext> received;
  auto router = std::make_shared<servicelib::http::Router>();
  router->Add(
      "GET", "/context",
      [&](servicelib::http::Request, servicelib::MessageContext context)
          -> boost::asio::awaitable<servicelib::http::Response> {
        received.emplace(std::move(context));
        co_return servicelib::http::Response{200, {}, "ok", "text/plain",
                                             false};
      });
  servicelib::http::Server::Options serverOptions;
  serverOptions.address = "127.0.0.1";
  serverOptions.port = 0;
  servicelib::http::Server server(io.get_executor(), std::move(router),
                                  serverOptions);
  server.Start();
  servicelib::http::Client client(io.get_executor());
  servicelib::http::Request request;
  request.method = "GET";
  request.target = "/context";
  auto context = servicelib::MessageContext{}
                     .withStreamId("stream-transport")
                     .withPriority(91)
                     .withDeadline(std::chrono::steady_clock::now() +
                                   std::chrono::seconds{5})
                     .withSampling(true)
                     .withTrace({"4bf92f3577b34da6a3ce929d0e0e4736",
                                 "00f067aa0ba902b7", true, "vendor=value",
                                 "tenant=acme"});
  auto result = boost::asio::co_spawn(
      io,
      client.Send("127.0.0.1", std::to_string(server.port()),
                  std::move(request), std::move(context)),
      boost::asio::use_future);
  std::jthread ioThread([&] { io.run(); });
  ASSERT_EQ(result.wait_for(std::chrono::seconds{2}),
            std::future_status::ready);
  EXPECT_EQ(result.get().status, 200);
  ASSERT_TRUE(received.has_value());
  EXPECT_EQ(received->streamId(), "stream-transport");
  EXPECT_FALSE(received->hasPriority());
  EXPECT_TRUE(received->samplingEnabled());
  EXPECT_EQ(received->trace().traceId, "4bf92f3577b34da6a3ce929d0e0e4736");
  EXPECT_EQ(received->trace().spanId, "00f067aa0ba902b7");
  EXPECT_EQ(received->trace().traceState, "vendor=value");
  EXPECT_EQ(received->trace().baggage, "tenant=acme");
  ASSERT_TRUE(received->deadline().has_value());
  EXPECT_GT(*received->deadline(), std::chrono::steady_clock::now());
  client.Stop();
  server.Stop();
  io.stop();
}

TEST(HttpClientProfiling, DISABLED_KeepAlivePoolHotPath) {
  const auto* durationValue = std::getenv("SERVICELIB_PROFILE_SECONDS");
  if (!durationValue) GTEST_SKIP() << "profiling workload is opt-in";
  const auto duration = std::chrono::seconds{std::stoll(durationValue)};
  ASSERT_GT(duration, std::chrono::seconds::zero());

  boost::asio::io_context io;
  auto workGuard = boost::asio::make_work_guard(io);
  auto router = std::make_shared<servicelib::http::Router>();
  router->Add(
      "POST", "/hot",
      [](servicelib::http::Request, servicelib::MessageContext)
          -> boost::asio::awaitable<servicelib::http::Response> {
        co_return servicelib::http::Response{
            200, {}, std::string(16 * 1024, 'y'), "application/octet-stream",
            true};
      });
  servicelib::http::Server::Options serverOptions;
  serverOptions.address = "127.0.0.1";
  serverOptions.port = 0;
  servicelib::http::Server server(io.get_executor(), std::move(router),
                                  serverOptions);
  server.Start();
  servicelib::http::Client::Options clientOptions;
  clientOptions.connections = 8;
  clientOptions.timeout = std::chrono::seconds{5};
  servicelib::http::Client client(io.get_executor(), clientOptions);
  const auto deadline = std::chrono::steady_clock::now() + duration;
  constexpr std::size_t kWorkers = 32;
  std::size_t remaining = kWorkers;
  std::size_t requests{};
  std::exception_ptr workerError;
  for (std::size_t index = 0; index < kWorkers; ++index) {
    boost::asio::co_spawn(
        io, RunHttpClientLoad(client, server.port(), deadline),
        [&](std::exception_ptr error, std::size_t completedRequests) {
          if (error && !workerError) workerError = std::move(error);
          requests += completedRequests;
          if (--remaining == 0) io.stop();
        });
  }
  io.run();
  client.Stop();
  server.Stop();
  workGuard.reset();
  if (workerError) std::rethrow_exception(workerError);
  EXPECT_GT(requests, 0U);
  std::cout << "http_client_profile requests=" << requests
            << " rate="
            << static_cast<double>(requests) /
                   static_cast<double>(duration.count())
            << "/s\n";
}

class MockSinkClient final : public servicelib::datasink::http::Client {
 public:
  boost::asio::awaitable<servicelib::datasink::http::Response> perform(
      servicelib::datasink::http::Request request,
      servicelib::MessageContext) override {
    lastRequest = std::move(request);
    ++calls;
    co_return servicelib::datasink::http::Response{200, "response", {}};
  }

  int calls{};
  std::optional<servicelib::datasink::http::Request> lastRequest;
};

struct SinkHandler final {
  using State = int;
  int* endCalls{};
  bool* hadError{};

  servicelib::BeginResult<State> beginRequest(
      servicelib::MessageContext context, auto&) {
    return {std::move(context), 1};
  }
  void consumeMessage(servicelib::MessageContext, auto&, State&,
                      const std::string& value,
                      servicelib::datasink::http::Requester& requester) {
    auto& request =
        requester.newRequest("POST", "http://example.test/data", value);
    request.headers[std::string{"content-type"}] = "text/plain";
  }
  void handleResponse(
      servicelib::MessageContext context, auto& streamContext, State&,
      const servicelib::datasink::http::Response& response) {
    streamContext.collect(std::move(context), response.body);
  }
  void endRequest(servicelib::MessageContext, auto&, std::exception_ptr error,
                  State&) noexcept {
    ++*endCalls;
    *hadError = static_cast<bool>(error);
  }
};

TEST(HttpDataSink, PreservesCanonicalRequestLifecycleAndStreamId) {
  boost::asio::io_context io;
  servicelib::detail::ParallelExecutorRegistry::Set(io.get_executor());
  {
    servicelib::testtracing::TestTracing tracing;
    TestEnvironment environment{&tracing};
    MockSinkClient client;
    int endCalls = 0;
    bool hadError = true;
    std::string result;
    TestSinkEndpointStream<std::string, std::string> stream{
        environment, 1,
        [&](servicelib::MessageContext,
            servicelib::Payload<std::string> value) { result = value.get(); }};
    servicelib::datasink::http::BeastEndpoint<std::string, std::string,
                                              SinkHandler>
        endpoint{stream, client, SinkHandler{&endCalls, &hadError}};
    endpoint.start(servicelib::Context{});
    endpoint.consume(
        servicelib::tracing::EnableSampling(
            servicelib::MessageContext{}.withStreamId("stream-42")),
        servicelib::Payload<std::string>::make("payload"));
    while (endCalls == 0) ASSERT_GT(io.run_one(), 0U);
    io.restart();
    while (io.poll_one() != 0U) {
    }
    endpoint.stop(servicelib::Context{});

    ASSERT_TRUE(client.lastRequest.has_value());
    EXPECT_EQ(client.calls, 1);
    EXPECT_EQ(client.lastRequest->url, "http://example.test/data");
    EXPECT_EQ(client.lastRequest->body, "payload");
    const auto requestStreamId =
        client.lastRequest->headers[std::string{"x-stream-id"}];
    EXPECT_FALSE(requestStreamId.empty());
    EXPECT_NE(requestStreamId, "stream-42");
    EXPECT_EQ(result, "response");
    EXPECT_EQ(endCalls, 1);
    EXPECT_FALSE(hadError);
    const servicelib::metrics::Labels labels{{"connector", "http"},
                                              {"endpoint", "http-source"}};
    EXPECT_EQ(environment.metrics()
                  .counter("datasink_endpoint.messages_total", labels)
                  .count(),
              1);
    EXPECT_EQ(environment.metrics()
                  .gauge("datasink_endpoint.active_requests", labels)
                  .value(),
              0);
    EXPECT_EQ(environment.metrics()
                  .histogram("datasink_endpoint.request_duration_seconds",
                             labels)
                  .count(),
              1);
    const auto spans = tracing.spans();
    ASSERT_EQ(spans.size(), 1);
    EXPECT_EQ(spans.front().name, "http.output");
    EXPECT_TRUE(std::any_of(
        spans.front().events.begin(), spans.front().events.end(),
        [](const auto& event) { return event.name == "handle_response"; }));
  }
  servicelib::detail::ParallelExecutorRegistry::Clear();
}

}  // namespace

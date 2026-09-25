#include <boost/asio/co_spawn.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/write.hpp>
#include <boost/asio/use_future.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>

#include <gtest/gtest.h>

#include <servicelib/datasource/http/beast.hpp>
#include <servicelib/datasink/http/beast.hpp>
#include <servicelib/runtime/detail/asio_dispatch.hpp>
#include <servicelib/runtime/environment/environment.hpp>
#include <servicelib/runtime/testlog/testlog.hpp>
#include <servicelib/runtime/testmetrics/testmetrics.hpp>
#include <servicelib/runtime/testtracing/testtracing.hpp>
#include <servicelib/transformation/streams.hpp>

#include "test_sink_endpoint_stream.hpp"

#include <chrono>
#include <atomic>
#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <exception>
#include <future>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
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
  explicit TestConfig(servicelib::api::HTTPMethodType method =
                          servicelib::api::HTTPMethodType::kPOST,
                      bool withSecondEndpoint = false) {
    connector.id = 10;
    connector.name = "http";
    connector.host = "127.0.0.1";
    connector.port = 8080;
    endpoint.id = 1;
    endpoint.name = "http-source";
    endpoint.idDataConnector = connector.id;
    endpoint.httpMethodType = method;
    endpoint.path = "/orders";
    if (withSecondEndpoint) {
      secondEndpoint = endpoint;
      secondEndpoint->id = 2;
      secondEndpoint->name = "http-second";
      secondEndpoint->path = "/second";
    }
    stream.id = 33;
    stream.name = "Receive Booking";
    stream.pipeline = "booking";
    stream.component = "Prepare Reservation";
  }

  std::vector<const servicelib::config::ServiceConfig*> GetServices()
      const override { return {}; }
  std::vector<servicelib::config::StreamConfigRef> GetStreams() const override {
    return {stream};
  }
  std::vector<servicelib::config::DataConnectorConfigRef> GetDataConnectors()
      const override { return {connector}; }
  std::vector<servicelib::config::EndpointConfigRef> GetEndpoints()
      const override {
    if (secondEndpoint) return {endpoint, *secondEndpoint};
    return {endpoint};
  }
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
  std::optional<servicelib::config::HttpEndpointConfig> secondEndpoint;
  servicelib::config::InputStreamConfig stream;
};

class TestEnvironment final : public servicelib::IRuntimeEnvironment {
 public:
  explicit TestEnvironment(
      servicelib::testtracing::TestTracing* tracing = nullptr,
      servicelib::api::HTTPMethodType method = servicelib::api::HTTPMethodType::kPOST,
      bool withSecondEndpoint = false)
      : config_(method, withSecondEndpoint), runtimeConfig_(config_), tracing_(tracing) {
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
      environment, 1, 33, Handler{},
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
  const auto expectAttribute = [&](const std::string& key, const std::string& value) {
    const auto& attributes = spans.front().attributes;
    const auto found = std::find_if(attributes.begin(), attributes.end(),
        [&](const auto& attribute) { return attribute.key() == key; });
    ASSERT_NE(found, attributes.end());
    EXPECT_EQ(std::get<std::string>(found->value()), value);
  };
  expectAttribute("stream", "Receive Booking");
  expectAttribute("pipeline", "booking");
  expectAttribute("component", "Prepare Reservation");
  EXPECT_EQ(spans.front().statusCode,
            servicelib::tracing::StatusCode::kUnset);
  EXPECT_TRUE(std::any_of(
      spans.front().events.begin(), spans.front().events.end(),
      [](const auto& event) { return event.name == "done_received"; }));
  endpoint.stop(servicelib::Context{});
  servicelib::detail::ParallelExecutorRegistry::Clear();
}

struct CompletionProbeHandler final {
  using State = int;
  using Request = std::string;
  using Response = std::string;
  bool hasResult;
  int* endCalls;

  servicelib::BeginResult<State> beginRequest(servicelib::MessageContext context,
      auto&, servicelib::datasource::http::HandlerData&) {
    return {std::move(context), 0};
  }
  void consumeMessage(servicelib::MessageContext context, auto& stream, State& state,
      servicelib::datasource::http::HandlerData& data, auto result) {
    if (hasResult) {
      Handler{}.consumeMessage(std::move(context), stream, state, data, result);
    } else {
      data.responseBody = "accepted";
      stream.collect(std::move(context), data.request.body);
    }
  }
  std::string getMessageId(servicelib::MessageContext, auto&, State&, const std::string&) {
    return "result";
  }
  void endRequest(servicelib::MessageContext, auto&, std::exception_ptr, State&,
      servicelib::datasource::http::HandlerData&) noexcept { ++*endCalls; }
};

TEST(HttpDataSource, TracingDoesNotCreateOrWaitForGraphCompletion) {
  for (const bool tracingEnabled : {false, true}) {
    for (const bool hasResult : {false, true}) {
      SCOPED_TRACE(::testing::Message() << "tracing=" << tracingEnabled
                                      << " hasResult=" << hasResult);
      boost::asio::io_context io;
      servicelib::detail::ParallelExecutorRegistry::Set(io.get_executor());
      servicelib::testtracing::TestTracing tracing;
      TestEnvironment environment{tracingEnabled ? &tracing : nullptr};
      servicelib::AsyncCompletionToken retained;
      std::optional<servicelib::MessageContext> deferred;
      int endCalls = 0;
      using Endpoint = servicelib::datasource::http::BeastEndpoint<
          std::string, std::string, CompletionProbeHandler>;
      Endpoint endpoint{environment, 1, CompletionProbeHandler{hasResult, &endCalls},
          [&](servicelib::MessageContext context, servicelib::Payload<std::string>) {
            retained = context.retainCompletionToken();
            deferred = std::move(context);
          }, hasResult};
      endpoint.start(servicelib::Context{});
      servicelib::http::Request request;
      request.method = "POST";
      request.path = request.target = "/orders";
      request.body = "one";
      auto response = boost::asio::co_spawn(io, endpoint.handle(std::move(request),
          servicelib::tracing::EnableSampling(servicelib::MessageContext{})),
          boost::asio::use_future);
      io.poll();
      EXPECT_TRUE(deferred.has_value());
      EXPECT_FALSE(static_cast<bool>(retained));
      if (hasResult && deferred) {
        EXPECT_EQ(response.wait_for(std::chrono::milliseconds{0}), std::future_status::timeout);
        endpoint.consumeResult(*deferred, servicelib::Payload<std::string>::make("one"));
        io.restart();
        io.poll();
      }
      EXPECT_EQ(response.wait_for(std::chrono::milliseconds{0}), std::future_status::ready);
      EXPECT_EQ(endCalls, 1);
      // Release the probe even on regression so an unexpected frame cannot hang the test.
      retained.reset();
      io.restart();
      while (response.wait_for(std::chrono::milliseconds{0}) != std::future_status::ready) {
        ASSERT_GT(io.run_one_for(std::chrono::seconds{1}), 0U);
      }
      EXPECT_EQ(response.get().body, hasResult ? "reply:one" : "accepted");
      if (deferred) {
        endpoint.consumeResult(*deferred, servicelib::Payload<std::string>::make("late"));
      }
      EXPECT_EQ(endCalls, 1);
      EXPECT_EQ(tracing.spans().size(), tracingEnabled ? 1U : 0U);
      endpoint.stop(servicelib::Context{});
      servicelib::detail::ParallelExecutorRegistry::Clear();
    }
  }
}

TEST(HttpDataSource, TracingDoesNotDelayCancellationOrDeadline) {
  for (const bool tracingEnabled : {false, true}) {
    for (const bool useDeadline : {false, true}) {
      SCOPED_TRACE(::testing::Message() << "tracing=" << tracingEnabled
                                      << " deadline=" << useDeadline);
      boost::asio::io_context io;
      servicelib::detail::ParallelExecutorRegistry::Set(io.get_executor());
      servicelib::testtracing::TestTracing tracing;
      TestEnvironment environment{tracingEnabled ? &tracing : nullptr};
      servicelib::AsyncCompletionToken retained;
      std::optional<servicelib::MessageContext> deferred;
      int endCalls = 0;
      using Endpoint = servicelib::datasource::http::BeastEndpoint<
          std::string, std::string, CompletionProbeHandler>;
      Endpoint endpoint{environment, 1, CompletionProbeHandler{true, &endCalls},
          [&](servicelib::MessageContext context, servicelib::Payload<std::string>) {
            retained = context.retainCompletionToken();
            deferred = std::move(context);
          }, true};
      endpoint.start(servicelib::Context{});
      std::stop_source stop;
      const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds{20};
      auto context = servicelib::MessageContext{}.withStopToken(stop.get_token());
      if (useDeadline) context = std::move(context).withDeadline(deadline);
      servicelib::http::Request request;
      request.method = "POST";
      request.path = request.target = "/orders";
      auto response = boost::asio::co_spawn(io, endpoint.handle(std::move(request),
          servicelib::tracing::EnableSampling(std::move(context))), boost::asio::use_future);
      io.poll();
      EXPECT_TRUE(deferred.has_value());
      EXPECT_EQ(response.wait_for(std::chrono::milliseconds{0}), std::future_status::timeout);
      if (useDeadline) std::this_thread::sleep_until(deadline);
      else stop.request_stop();
      io.restart();
      io.poll();
      EXPECT_EQ(response.wait_for(std::chrono::milliseconds{0}), std::future_status::ready);
      EXPECT_EQ(endCalls, 1);
      retained.reset();
      io.restart();
      while (response.wait_for(std::chrono::milliseconds{0}) != std::future_status::ready) {
        ASSERT_GT(io.run_one_for(std::chrono::seconds{1}), 0U);
      }
      static_cast<void>(response.get());
      if (deferred) {
        EXPECT_TRUE(deferred->cancelled());
        endpoint.consumeResult(*deferred, servicelib::Payload<std::string>::make("late"));
      }
      EXPECT_EQ(endCalls, 1);
      EXPECT_EQ(tracing.spans().size(), tracingEnabled ? 1U : 0U);
      endpoint.stop(servicelib::Context{});
      servicelib::detail::ParallelExecutorRegistry::Clear();
    }
  }
}

struct RetainedCallbackHandler final {
  using State = int;
  using Request = std::string;
  using Response = std::string;
  servicelib::BeginResult<State> beginRequest(servicelib::MessageContext context,
      auto&, servicelib::datasource::http::HandlerData&) {
    return {std::move(context), 0};
  }
  void consumeMessage(servicelib::MessageContext context, auto& stream, State&,
      servicelib::datasource::http::HandlerData&, auto result) {
    result.setResultCallback("result",
        [result, local = 0](servicelib::MessageContext, auto&, State& count,
            const std::string&, servicelib::datasource::http::HandlerData& data) mutable {
          data.responseBody += std::to_string(++local);
          if (++count == 2) result.done();
          return false;
        });
    stream.collect(context, std::string{"first"});
    stream.collect(std::move(context), std::string{"second"});
  }
  std::string getMessageId(servicelib::MessageContext, auto&, State&, const std::string&) {
    return "result";
  }
  void endRequest(servicelib::MessageContext, auto&, std::exception_ptr, State&,
      servicelib::datasource::http::HandlerData&) noexcept {}
};

TEST(HttpDataSource, RetainedCallbackKeepsStateAndTraceOrder) {
  boost::asio::io_context io;
  servicelib::detail::ParallelExecutorRegistry::Set(io.get_executor());
  servicelib::testtracing::TestTracing tracing;
  TestEnvironment environment{&tracing};
  using Endpoint = servicelib::datasource::http::BeastEndpoint<
      std::string, std::string, RetainedCallbackHandler>;
  Endpoint* pointer{};
  Endpoint endpoint{environment, 1, RetainedCallbackHandler{},
      [&](servicelib::MessageContext context, servicelib::Payload<std::string> value) {
        pointer->consumeResult(std::move(context), std::move(value));
      }, true};
  pointer = &endpoint;
  endpoint.start(servicelib::Context{});
  servicelib::http::Request request;
  request.method = "POST";
  request.path = request.target = "/orders";
  auto response = boost::asio::co_spawn(io,
      endpoint.handle(std::move(request), servicelib::tracing::EnableSampling(
          servicelib::MessageContext{}.withStreamId("retained-callback"))),
      boost::asio::use_future);
  while (response.wait_for(std::chrono::milliseconds{0}) != std::future_status::ready)
    ASSERT_GT(io.run_one(), 0U);
  EXPECT_EQ(response.get().body, "12");
  const auto spans = tracing.spans();
  ASSERT_EQ(spans.size(), 1U);
  std::vector<std::string> events;
  for (const auto& event : spans.front().events) events.push_back(event.name);
  EXPECT_EQ(events, (std::vector<std::string>{"begin_request", "result_consumed",
      "done_called", "result_consumed", "consume_message", "done_received"}));
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

TEST(HttpServer, ShutdownDeadlineDoesNotWaitForUncooperativeHandler) {
  std::weak_ptr<servicelib::http::Router> retainedRouter;
  {
    boost::asio::io_context io;
    servicelib::detail::SingleUseEvent accepted;
    servicelib::detail::SingleUseEvent release;
    servicelib::detail::SingleUseEvent retired;
    std::atomic<bool> handlerReturned{};
    auto router = std::make_shared<servicelib::http::Router>();
    retainedRouter = router;
    router->Add(
        "POST", "/wait",
        [&](servicelib::http::Request, servicelib::MessageContext)
            -> boost::asio::awaitable<servicelib::http::Response> {
          accepted.Send();
          // Deliberately ignore request cancellation without blocking a worker.
          co_await release.AsyncWait();
          handlerReturned.store(true, std::memory_order_release);
          retired.Send();
          co_return servicelib::http::Response{
              200, {}, "released", "text/plain", false};
        });
    servicelib::http::Server::Options options;
    options.address = "127.0.0.1";
    options.port = 0;
    options.shutdownTimeout = std::chrono::milliseconds{30};
    auto server = std::make_unique<servicelib::http::Server>(
        io.get_executor(), std::move(router), options);
    server->Start();
    auto request = boost::asio::co_spawn(
        io, SendRequest(server->port()), boost::asio::use_future);
    std::jthread ioThread([&] { io.run(); });
    EXPECT_TRUE(accepted.WaitUntil(std::chrono::steady_clock::now() +
                                  std::chrono::seconds{2}));

    auto stopped = std::async(std::launch::async, [&] { server->Stop(); });
    const bool returnedBeforeHandler =
        stopped.wait_for(std::chrono::seconds{1}) == std::future_status::ready;
    EXPECT_TRUE(returnedBeforeHandler)
        << "30ms shutdown deadline must not wait for handler completion";
    EXPECT_FALSE(handlerReturned.load(std::memory_order_acquire));
    if (returnedBeforeHandler) {
      stopped.get();
      server.reset();
      EXPECT_FALSE(retainedRouter.expired())
          << "the active session must retain its route after Server destruction";
    }

    // Always release the handler, including when testing the old blocking Stop.
    release.Send();
    EXPECT_TRUE(retired.WaitUntil(std::chrono::steady_clock::now() +
                                 std::chrono::seconds{2}));
    if (!returnedBeforeHandler) {
      EXPECT_EQ(stopped.wait_for(std::chrono::seconds{2}),
                std::future_status::ready);
      stopped.get();
      server.reset();
    }
    const auto requestStatus = request.wait_for(std::chrono::seconds{2});
    EXPECT_EQ(requestStatus, std::future_status::ready);
    if (requestStatus == std::future_status::ready) {
      EXPECT_THROW(static_cast<void>(request.get()), std::exception);
    }
    io.stop();
    ioThread.join();
  }
  EXPECT_TRUE(retainedRouter.expired());
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
      [&](servicelib::http::Request, servicelib::MessageContext)
          -> boost::asio::awaitable<servicelib::http::Response> {
        accepted.Send();
        // Keep the server from racing the client's deadline with a response.
        // This test verifies cancellation of an accepted client-side read.
        co_await never.AsyncWait();
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
              std::chrono::milliseconds{200}))),
      boost::asio::use_future);
  std::jthread ioThread([&] { io.run(); });
  ASSERT_TRUE(accepted.WaitUntil(std::chrono::steady_clock::now() +
                                 std::chrono::seconds{2}));
  ASSERT_EQ(response.wait_for(std::chrono::seconds{2}),
            std::future_status::ready);
  EXPECT_EQ(response.get(), servicelib::http::ClientErrorCode::kTimeout);
  never.Send();
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

TEST(HttpClient, CancelledPoolWaiterDoesNotLeakTheConnectionOrPoll) {
  boost::asio::io_context io;
  servicelib::detail::SingleUseEvent accepted;
  servicelib::detail::SingleUseEvent release;
  auto router = std::make_shared<servicelib::http::Router>();
  router->Add("GET", "/hold",
      [&](servicelib::http::Request, servicelib::MessageContext)
          -> boost::asio::awaitable<servicelib::http::Response> {
        accepted.Send();
        co_await release.AsyncWait();
        co_return servicelib::http::Response{200, {}, "ok", "text/plain", true};
      });
  servicelib::http::Server::Options serverOptions;
  serverOptions.address = "127.0.0.1";
  serverOptions.port = 0;
  servicelib::http::Server server(io.get_executor(), router, serverOptions);
  server.Start();
  servicelib::http::Client::Options options;
  options.connections = 1;
  options.timeout = std::chrono::seconds{3};
  options.acquirePollInterval = std::chrono::seconds{10};
  servicelib::http::Client client(io.get_executor(), options);
  const auto send = [&]() {
    servicelib::http::Request request;
    request.method = "GET";
    request.target = "/hold";
    return client.Send("127.0.0.1", std::to_string(server.port()), std::move(request));
  };
  auto first = boost::asio::co_spawn(io, send(), boost::asio::use_future);
  std::jthread worker([&] { io.run(); });
  EXPECT_TRUE(accepted.WaitUntil(std::chrono::steady_clock::now() + std::chrono::seconds{2}));
  boost::asio::cancellation_signal cancel;
  auto cancelled = boost::asio::co_spawn(io, send(),
      boost::asio::bind_cancellation_slot(cancel.slot(), boost::asio::use_future));
  // Emit on the same executor as the cancellation slot, after admission.
  auto cancelTimer = std::make_shared<boost::asio::steady_timer>(io, std::chrono::milliseconds{20});
  cancelTimer->async_wait([&cancel, cancelTimer](boost::system::error_code) {
    cancel.emit(boost::asio::cancellation_type::all);
  });
  EXPECT_EQ(cancelled.wait_for(std::chrono::seconds{2}), std::future_status::ready);
  EXPECT_THROW(static_cast<void>(cancelled.get()), boost::system::system_error);
  auto next = boost::asio::co_spawn(io, send(), boost::asio::use_future);
  release.Send();
  EXPECT_EQ(first.wait_for(std::chrono::seconds{2}), std::future_status::ready);
  EXPECT_EQ(first.get().status, 200);
  EXPECT_EQ(next.wait_for(std::chrono::seconds{2}), std::future_status::ready);
  EXPECT_EQ(next.get().status, 200);
  EXPECT_EQ(client.connectionCount(), 1U);
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

class StopProbeHttpEndpoint final : public servicelib::datasink::http::IEndpoint {
 public:
  StopProbeHttpEndpoint(int id, std::function<void()> stop)
      : id_(id), stop_(std::move(stop)) {}
  int id() const noexcept override { return id_; }
  void start(servicelib::Context) override {}
  void stop(servicelib::Context) override { stop_(); }
 private:
  int id_;
  std::function<void()> stop_;
};

TEST(HttpDataSink, StopIsConcurrentBetweenIdsButReverseSequentialWithinOneId) {
  using namespace std::chrono_literals;
  TestEnvironment environment{nullptr, servicelib::api::HTTPMethodType::kPOST, true};
  TestSinkEndpointStream<int, int> stream{environment, 1};
  auto sink = servicelib::datasink::http::BeastDataSink::make(stream);
  std::promise<void> lastEntered, otherEntered, releaseLast;
  auto lastReady = lastEntered.get_future();
  auto otherReady = otherEntered.get_future();
  auto release = releaseLast.get_future().share();
  std::atomic<bool> firstStarted{false};
  std::atomic<bool> lastFinished{false};
  std::mutex orderMutex;
  std::vector<int> sameEndpointOrder;
  sink->addEndpoint(std::make_shared<StopProbeHttpEndpoint>(1, [&] {
    firstStarted.store(true);
    EXPECT_TRUE(lastFinished.load());
    std::lock_guard lock(orderMutex);
    sameEndpointOrder.push_back(1);
  }));
  sink->addEndpoint(std::make_shared<StopProbeHttpEndpoint>(1, [&] {
    {
      std::lock_guard lock(orderMutex);
      sameEndpointOrder.push_back(2);
    }
    lastEntered.set_value();
    release.wait();
    lastFinished.store(true);
  }));
  sink->addEndpoint(std::make_shared<StopProbeHttpEndpoint>(2, [&] {
    otherEntered.set_value();
  }));
  sink->start({});
  auto stopped = std::async(std::launch::async, [&] { sink->stop({}); });
  EXPECT_EQ(lastReady.wait_for(1s), std::future_status::ready);
  EXPECT_EQ(otherReady.wait_for(1s), std::future_status::ready);
  EXPECT_FALSE(firstStarted.load());
  EXPECT_EQ(stopped.wait_for(0ms), std::future_status::timeout);
  // Release even after a failed assertion, so a serial implementation cannot
  // leave the test process waiting indefinitely during future destruction.
  releaseLast.set_value();
  EXPECT_NO_THROW(stopped.get());
  EXPECT_TRUE(firstStarted.load());
  EXPECT_EQ(sameEndpointOrder, (std::vector<int>{2, 1}));
}

struct ConfiguredMethodSinkHandler final {
  using State = int;
  std::string method;
  int* ended;
  servicelib::BeginResult<State> beginRequest(
      servicelib::MessageContext context, auto&) {
    return {std::move(context), 0};
  }
  void consumeMessage(servicelib::MessageContext, auto&, State&,
                      const std::string& value,
                      servicelib::datasink::http::Requester& requester) {
    requester.newRequest(method, "http://example.test/resource", value);
  }
  void handleResponse(servicelib::MessageContext context, auto& stream,
                      State&, const servicelib::datasink::http::Response& response) {
    stream.collect(std::move(context), response.body);
  }
  void endRequest(servicelib::MessageContext, auto&, std::exception_ptr error,
                  State&) {
    EXPECT_FALSE(error);
    ++*ended;
  }
};

class ConfiguredMethodSinkClient final : public servicelib::datasink::http::Client {
 public:
  std::string expectedMethod;
  int calls{};
  boost::asio::awaitable<servicelib::datasink::http::Response> perform(
      servicelib::datasink::http::Request request,
      servicelib::MessageContext) override {
    EXPECT_EQ(request.method, expectedMethod);
    ++calls;
    co_return servicelib::datasink::http::Response{200, "result", {}};
  }
};

TEST(HttpDataSink, AcceptsEveryDeclaredMethodAndRejectsInvalidConfiguration) {
  using Method = servicelib::api::HTTPMethodType;
  using Endpoint = servicelib::datasink::http::BeastEndpoint<
      std::string, std::string, ConfiguredMethodSinkHandler>;
  const std::pair<Method, const char*> methods[] = {
      {Method::kGET, "GET"}, {Method::kPOST, "POST"},
      {Method::kPUT, "PUT"}, {Method::kPATCH, "PATCH"},
      {Method::kDELETE, "DELETE"}, {Method::kHEAD, "HEAD"},
      {Method::kOPTIONS, "OPTIONS"}, {Method::kTRACE, "TRACE"},
      {Method::kCONNECT, "CONNECT"}};
  boost::asio::io_context io;
  servicelib::detail::ParallelExecutorRegistry::Set(io.get_executor());
  for (const auto& [method, name] : methods) {
    SCOPED_TRACE(name);
    TestEnvironment environment{nullptr, method};
    ConfiguredMethodSinkClient client;
    client.expectedMethod = name;
    int ended = 0;
    int received = 0;
    TestSinkEndpointStream<std::string, std::string> stream{
        environment, 1,
        [&received](servicelib::MessageContext context,
                     servicelib::Payload<std::string> value) {
          EXPECT_EQ(context.streamId(), "method-parent");
          EXPECT_EQ(value.get(), "result");
          ++received;
        }};
    EXPECT_NO_THROW({
      Endpoint endpoint(stream, client, ConfiguredMethodSinkHandler{name, &ended});
      endpoint.start({});
      io.restart();
      auto call = boost::asio::co_spawn(io,
          servicelib::detail::CooperativeExecution::Run([&] {
            endpoint.consume(servicelib::MessageContext{}.withStreamId("method-parent"),
                             servicelib::Payload<std::string>::make("request"));
            EXPECT_EQ(received, 1);
            EXPECT_EQ(ended, 1);
          }), boost::asio::use_future);
      io.run();
      call.get();
      endpoint.stop({});
    });
    EXPECT_EQ(client.calls, 1);
    EXPECT_EQ(received, 1);
    EXPECT_EQ(ended, 1);
  }
  for (const auto method : {Method::kUndefined, static_cast<Method>(-1),
                            static_cast<Method>(99)}) {
    SCOPED_TRACE(static_cast<int>(method));
    TestEnvironment environment{nullptr, method};
    ConfiguredMethodSinkClient client;
    int ended = 0;
    TestSinkEndpointStream<std::string, std::string> stream{environment, 1};
    EXPECT_THROW((Endpoint{stream, client, ConfiguredMethodSinkHandler{"GET", &ended}}),
                 std::invalid_argument);
    EXPECT_EQ(client.calls, 0);
    EXPECT_EQ(ended, 0);
  }
  servicelib::detail::ParallelExecutorRegistry::Clear();
}

struct HttpSubStreamCall final {
  std::function<void(servicelib::MessageContext, int)> output;
  bool ended{};
  std::exception_ptr error;
};

struct HttpSubStreamHandler final {
  using State = std::shared_ptr<HttpSubStreamCall>;
  servicelib::ContextKey<HttpSubStreamCall>* key;
  std::string url;

  servicelib::BeginResult<State> beginRequest(
      servicelib::MessageContext context, auto&) {
    auto state = context.localValue(*key);
    return {std::move(context), std::move(state)};
  }
  void consumeMessage(servicelib::MessageContext, auto&, State&,
                      const int& value,
                      servicelib::datasink::http::Requester& requester) {
    requester.newRequest("POST", url, std::to_string(value));
  }
  void handleResponse(servicelib::MessageContext context, auto& stream,
                      State&, const servicelib::datasink::http::Response& response) {
    stream.collect(std::move(context), std::stoi(response.body));
  }
  void endRequest(servicelib::MessageContext, auto&, std::exception_ptr error,
                  State& state) {
    state->error = error;
    state->ended = true;
  }
};

struct HttpSubStreamTypes {
  template <typename> struct DataType {};
};

using HttpSubStreamForward = std::function<void(servicelib::MessageContext, int)>;
using HttpSubStreamInvoke = std::function<void(
    servicelib::MessageContext, int, HttpSubStreamForward)>;

struct HttpSubStreamWork final {
  HttpSubStreamInvoke* invoke;
  template <typename Output>
  void operator()(servicelib::MessageContext context, servicelib::StreamBase&,
                  int& value, Output&& output) const {
    (*invoke)(std::move(context), value,
        [&output](servicelib::MessageContext returned, int result) {
          output.out(std::move(returned), result);
        });
  }
};

class HttpSubStreamApp final
    : public servicelib::StreamExecutionEnvironment<HttpSubStreamApp,
                                                     HttpSubStreamTypes> {
 public:
  std::shared_ptr<servicelib::SubStream<int, int, HttpSubStreamApp>> entry;
  HttpSubStreamInvoke invoke;
  void init() {
    servicelib::config::SubStreamConfig config;
    config.id = 101;
    config.name = "remote-lookup";
    entry = servicelib::makeSubStream<int, int, HttpSubStreamApp>(config, *this);
    servicelib::config::MapStreamConfig work;
    work.id = 102;
    work.name = "http-lookup";
    auto& result = entry->map(work, servicelib::StreamType<int>{},
                              servicelib::StreamFunction(HttpSubStreamWork{&invoke}));
    entry->setSource(result);
    static_cast<void>(getExecutionRuntime<>());
  }
  void delay(servicelib::Context, servicelib::pool::IDelayPool::Duration,
             std::function<void()> task) override { task(); }
};

TEST(HttpDataSink, ConcurrentSubStreamsKeepCorrelationOverTcpOnOneWorker) {
  constexpr int callCount = 16;
  HttpSubStreamApp app;
  app.init();
  boost::asio::io_context io;
  servicelib::detail::ParallelExecutorRegistry::Set(io.get_executor());
  servicelib::ContextKey<HttpSubStreamCall> callKey;
  servicelib::ContextKey<int> callerKey;
  std::unordered_set<std::string> wireIds;
  std::vector<std::shared_ptr<boost::asio::steady_timer>> gates;
  auto router = std::make_shared<servicelib::http::Router>();
  router->Add("POST", "/lookup",
      [&](servicelib::http::Request request, servicelib::MessageContext context)
          -> boost::asio::awaitable<servicelib::http::Response> {
        EXPECT_FALSE(context.localValue(callerKey));
        EXPECT_NE(context.streamId(), "shared-parent");
        EXPECT_TRUE(wireIds.emplace(context.streamId()).second);
        auto gate = std::make_shared<boost::asio::steady_timer>(
            io, std::chrono::seconds{5});
        gates.push_back(gate);
        if (gates.size() == callCount) {
          for (const auto& waiting : gates) waiting->cancel();
        } else {
          boost::system::error_code error;
          co_await gate->async_wait(boost::asio::redirect_error(
              boost::asio::use_awaitable, error));
          EXPECT_EQ(error, boost::asio::error::operation_aborted);
        }
        co_return servicelib::http::Response{
            200, {}, std::to_string(std::stoi(request.body) * 2),
            "text/plain", true};
      });
  servicelib::http::Server::Options serverOptions;
  serverOptions.address = "127.0.0.1";
  serverOptions.port = 0;
  servicelib::http::Server server(io.get_executor(), router, serverOptions);
  server.Start();
  servicelib::http::Client::Options clientOptions;
  clientOptions.connections = callCount;
  auto transport = std::make_shared<servicelib::http::Client>(
      io.get_executor(), clientOptions);
  servicelib::datasink::http::BeastClient client(transport);
  TestEnvironment environment;
  TestSinkEndpointStream<int, int> stream{
      environment, 1,
      [&callKey](servicelib::MessageContext context, servicelib::Payload<int> value) {
        const auto call = context.localValue(callKey);
        ASSERT_TRUE(call);
        call->output(std::move(context), value.get());
      }};
  servicelib::datasink::http::BeastEndpoint<int, int, HttpSubStreamHandler> endpoint{
      stream, client, HttpSubStreamHandler{
          &callKey, "http://127.0.0.1:" + std::to_string(server.port()) + "/lookup"}};
  endpoint.start({});
  app.invoke = [&](servicelib::MessageContext context, int value,
                    HttpSubStreamForward output) {
    auto call = std::make_shared<HttpSubStreamCall>();
    call->output = std::move(output);
    endpoint.consume(context.withLocalValue(callKey, call),
                     servicelib::Payload<int>::make(value));
    EXPECT_TRUE(call->ended);
    call->output = {};
    if (call->error) std::rethrow_exception(call->error);
  };
  std::vector<std::future<void>> calls;
  for (int value = 0; value < callCount; ++value) {
    calls.push_back(boost::asio::co_spawn(io,
        servicelib::detail::CooperativeExecution::Run([&, value] {
          int received = 0;
          const auto context = servicelib::MessageContext{}
              .withStreamId("shared-parent")
              .withLocalValue(callerKey, std::make_shared<int>(value))
              .withDeadline(std::chrono::steady_clock::now() + std::chrono::seconds{10});
          app.entry->consume(context, servicelib::Payload<int>::make(value),
              std::make_shared<servicelib::SubStreamCollectorFunc<int>>(
                  [&](servicelib::MessageContext returned, const int& result) {
                    EXPECT_EQ(returned.streamId(), "shared-parent");
                    EXPECT_EQ(returned.localValue(callerKey), context.localValue(callerKey));
                    EXPECT_EQ(result, value * 2);
                    ++received;
                    return true;
                  }));
          EXPECT_EQ(received, 1);
        }), boost::asio::use_future));
  }
  std::jthread worker([&io] { io.run(); });
  for (auto& call : calls) EXPECT_NO_THROW(call.get());
  endpoint.stop({});
  client.Stop();
  server.Stop();
  io.stop();
  worker.join();
  EXPECT_EQ(wireIds.size(), callCount);
  servicelib::detail::ParallelExecutorRegistry::Clear();
}

TEST(HttpDataSink, CancelledNestedSubStreamReleasesStateWithoutCancellingSibling) {
  using namespace std::chrono_literals;
  HttpSubStreamApp app;
  app.init();
  boost::asio::io_context io;
  servicelib::detail::ParallelExecutorRegistry::Set(io.get_executor());
  servicelib::ContextKey<HttpSubStreamCall> callKey;
  std::promise<void> firstArrived;
  auto firstReady = firstArrived.get_future();
  boost::asio::steady_timer lateResponse(io, 30s);
  std::unordered_set<std::string> wireIds;
  auto router = std::make_shared<servicelib::http::Router>();
  router->Add("POST", "/lookup",
      [&](servicelib::http::Request request, servicelib::MessageContext context)
          -> boost::asio::awaitable<servicelib::http::Response> {
        EXPECT_NE(context.streamId(), "nested-parent");
        EXPECT_TRUE(wireIds.emplace(context.streamId()).second);
        const int value = std::stoi(request.body);
        if (value == 1) {
          firstArrived.set_value();
          boost::system::error_code error;
          co_await lateResponse.async_wait(boost::asio::redirect_error(
              boost::asio::use_awaitable, error));
          EXPECT_EQ(error, boost::asio::error::operation_aborted);
        }
        co_return servicelib::http::Response{
            200, {}, std::to_string(value * 2), "text/plain", true};
      });
  servicelib::http::Server::Options serverOptions;
  serverOptions.address = "127.0.0.1";
  serverOptions.port = 0;
  servicelib::http::Server server(io.get_executor(), router, serverOptions);
  server.Start();
  servicelib::http::Client::Options clientOptions;
  clientOptions.connections = 2;
  auto transport = std::make_shared<servicelib::http::Client>(
      io.get_executor(), clientOptions);
  servicelib::datasink::http::BeastClient client(transport);
  TestEnvironment environment;
  TestSinkEndpointStream<int, int> stream{
      environment, 1,
      [&callKey](servicelib::MessageContext context, servicelib::Payload<int> value) {
        const auto call = context.localValue(callKey);
        ASSERT_TRUE(call);
        call->output(std::move(context), value.get());
      }};
  servicelib::datasink::http::BeastEndpoint<int, int, HttpSubStreamHandler> endpoint{
      stream, client, HttpSubStreamHandler{
          &callKey, "http://127.0.0.1:" + std::to_string(server.port()) + "/lookup"}};
  endpoint.start({});
  std::weak_ptr<HttpSubStreamCall> cancelledState;
  std::atomic<int> ended{0}, failed{0}, cancelledDeliveries{0}, siblingDeliveries{0};
  app.invoke = [&](servicelib::MessageContext context, int value,
                    HttpSubStreamForward output) {
    if (value >= 100) {
      int nested = -1;
      int received = 0;
      app.entry->consume(context, servicelib::Payload<int>::make(value - 100),
          std::make_shared<servicelib::SubStreamCollectorFunc<int>>(
              [&](servicelib::MessageContext returned, const int& result) {
                EXPECT_EQ(returned.streamId(), "nested-parent");
                nested = result;
                ++received;
                return true;
              }));
      EXPECT_EQ(received, 1);
      output(std::move(context), nested + 1000);
      return;
    }
    auto call = std::make_shared<HttpSubStreamCall>();
    call->output = std::move(output);
    if (value == 1) cancelledState = call;
    endpoint.consume(context.withLocalValue(callKey, call),
                     servicelib::Payload<int>::make(value));
    EXPECT_TRUE(call->ended);
    ++ended;
    if (call->error) ++failed;
    call->output = {};
    // No fabricated result on HTTP failure: SubStream must observe its own
    // cancellation, while a successful sibling completes through its collector.
  };
  std::stop_source cancellation;
  const auto parent = servicelib::MessageContext{}.withStreamId("nested-parent");
  auto cancelled = boost::asio::co_spawn(io,
      servicelib::detail::CooperativeExecution::Run([&] {
        EXPECT_THROW(app.entry->consume(parent.withStopToken(cancellation.get_token()),
            servicelib::Payload<int>::make(101),
            std::make_shared<servicelib::SubStreamCollectorFunc<int>>(
                [&](servicelib::MessageContext, const int&) {
                  ++cancelledDeliveries;
                  return true;
                })), std::runtime_error);
      }), boost::asio::use_future);
  auto sibling = boost::asio::co_spawn(io,
      servicelib::detail::CooperativeExecution::Run([&] {
        app.entry->consume(parent, servicelib::Payload<int>::make(102),
            std::make_shared<servicelib::SubStreamCollectorFunc<int>>(
                [&](servicelib::MessageContext context, const int& result) {
                  EXPECT_EQ(context.streamId(), "nested-parent");
                  EXPECT_EQ(result, 1004);
                  ++siblingDeliveries;
                  return true;
                }));
      }), boost::asio::use_future);
  std::jthread worker([&io] { io.run(); });
  EXPECT_EQ(firstReady.wait_for(2s), std::future_status::ready);
  cancellation.request_stop();
  const auto cancelledStatus = cancelled.wait_for(2s);
  EXPECT_EQ(cancelledStatus, std::future_status::ready);
  EXPECT_EQ(sibling.wait_for(2s), std::future_status::ready);
  if (cancelledStatus == std::future_status::ready) {
    EXPECT_NO_THROW(cancelled.get());
    EXPECT_TRUE(cancelledState.expired());
  }
  // Also releases the server if cancellation assertions failed.
  boost::asio::post(io, [&lateResponse] { lateResponse.cancel(); });
  if (cancelledStatus != std::future_status::ready) {
    EXPECT_NO_THROW(cancelled.get());
  }
  EXPECT_NO_THROW(sibling.get());
  endpoint.stop({});
  client.Stop();
  server.Stop();
  io.stop();
  worker.join();
  EXPECT_EQ(ended.load(), 2);
  EXPECT_EQ(failed.load(), 1);
  EXPECT_EQ(cancelledDeliveries.load(), 0);
  EXPECT_EQ(siblingDeliveries.load(), 1);
  EXPECT_EQ(wireIds.size(), 2U);
  servicelib::detail::ParallelExecutorRegistry::Clear();
}

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
    auto consumed = boost::asio::co_spawn(io,
        servicelib::detail::CooperativeExecution::Run([&] {
          endpoint.consume(
              servicelib::tracing::EnableSampling(
                  servicelib::MessageContext{}.withStreamId("stream-42")),
              servicelib::Payload<std::string>::make("payload"));
          EXPECT_EQ(endCalls, 1);
          EXPECT_EQ(result, "response");
        }), boost::asio::use_future);
    while (consumed.wait_for(std::chrono::seconds{0}) != std::future_status::ready)
      ASSERT_GT(io.run_one(), 0U);
    consumed.get();
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

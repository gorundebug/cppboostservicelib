/*
 * Boost.Beast HTTP datasink.
 *
 * This is the canonical ServiceLib HTTP sink contract with only the
 * userver coroutine-aware client boundary replaced by an Asio awaitable.
 */
#pragma once

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>

#include <servicelib/datasink/http/client.hpp>
#include <servicelib/runtime/common.hpp>
#include <servicelib/runtime/config/dataconnector_types.hpp>
#include <servicelib/runtime/config/endpoint_types.hpp>
#include <servicelib/runtime/datasink.hpp>
#include <servicelib/runtime/detail/asio_dispatch.hpp>
#include <servicelib/runtime/detail/async_operations.hpp>
#include <servicelib/runtime/environment/environment.hpp>
#include <servicelib/runtime/environment/tracing/tracing.hpp>

#include <chrono>
#include <exception>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace servicelib::datasink::http {

class HttpEndpointError final : public std::runtime_error {
 public:
  using std::runtime_error::runtime_error;
};

struct Request final {
  std::string method{"GET"};
  std::string url;
  std::string body;
  servicelib::http::Headers headers;
  std::optional<std::chrono::milliseconds> timeout;
};

class Requester final {
 public:
  Request& newRequest(std::string_view method, std::string url,
                      std::string body = {}) {
    request_.emplace();
    request_->method = std::string(method);
    request_->url = std::move(url);
    request_->body = std::move(body);
    return *request_;
  }

  [[nodiscard]] bool hasRequest() const noexcept { return request_.has_value(); }

  Request takeRequest() {
    if (!request_) {
      throw HttpEndpointError("HTTP sink handler did not create a request");
    }
    return std::move(*request_);
  }

 private:
  std::optional<Request> request_;
};

struct Response final {
  int status{};
  std::string body;
  servicelib::http::Headers headers;

  [[nodiscard]] bool isError() const noexcept { return status >= 400; }
};

class Client {
 public:
  virtual ~Client() = default;
  virtual boost::asio::awaitable<Response> perform(
      Request request, MessageContext context) = 0;
};

class BeastClient final : public Client {
 public:
  explicit BeastClient(std::shared_ptr<servicelib::http::Client> client)
      : client_(std::move(client)) {
    if (!client_) throw std::invalid_argument("HTTP client is null");
  }

  void Stop() noexcept { client_->Stop(); }

  boost::asio::awaitable<Response> perform(
      Request request, MessageContext context) override {
    auto target = parseUrl(request.url);
    if (request.timeout) {
      const auto requestDeadline =
          std::chrono::steady_clock::now() + *request.timeout;
      if (!context.deadline() || requestDeadline < *context.deadline()) {
        context = std::move(context).withDeadline(requestDeadline);
      }
    }
    servicelib::http::Request transportRequest;
    transportRequest.method = std::move(request.method);
    transportRequest.target = std::move(target.target);
    transportRequest.path = transportRequest.target.substr(
        0, transportRequest.target.find('?'));
    transportRequest.headers = std::move(request.headers);
    transportRequest.body = std::move(request.body);
    auto response = co_await client_->Send(
        std::move(target.host), std::move(target.port),
        std::move(transportRequest), std::move(context));
    co_return Response{response.status, std::move(response.body),
                       std::move(response.headers)};
  }

 private:
  struct Target final {
    std::string host;
    std::string port;
    std::string target;
  };

  static Target parseUrl(std::string_view url) {
    constexpr std::string_view scheme{"http://"};
    if (!url.starts_with(scheme)) {
      throw HttpEndpointError("only http:// URLs are supported");
    }
    url.remove_prefix(scheme.size());
    const auto pathOffset = url.find('/');
    const auto authority = url.substr(0, pathOffset);
    if (authority.empty()) throw HttpEndpointError("HTTP URL host is empty");
    const auto target = pathOffset == std::string_view::npos
                            ? std::string{"/"}
                            : std::string{url.substr(pathOffset)};
    std::string host;
    std::string port{"80"};
    if (authority.front() == '[') {
      const auto closing = authority.find(']');
      if (closing == std::string_view::npos) {
        throw HttpEndpointError("invalid IPv6 HTTP URL");
      }
      host = std::string{authority.substr(1, closing - 1)};
      if (closing + 1 < authority.size()) {
        if (authority[closing + 1] != ':') {
          throw HttpEndpointError("invalid HTTP URL authority");
        }
        port = std::string{authority.substr(closing + 2)};
      }
    } else if (const auto separator = authority.rfind(':');
               separator != std::string_view::npos) {
      host = std::string{authority.substr(0, separator)};
      port = std::string{authority.substr(separator + 1)};
    } else {
      host = std::string{authority};
    }
    if (host.empty() || port.empty()) {
      throw HttpEndpointError("invalid HTTP URL authority");
    }
    return {std::move(host), std::move(port), std::move(target)};
  }

  std::shared_ptr<servicelib::http::Client> client_;
};

class IEndpoint {
 public:
  virtual ~IEndpoint() = default;
  [[nodiscard]] virtual int id() const noexcept = 0;
  virtual void start(Context context) = 0;
  virtual void stop(Context context) = 0;
};

template <typename T, typename R, typename E = std::exception_ptr>
class StreamContext final : public servicelib::SinkStreamContext<T, R, E> {
 public:
  using Base = servicelib::SinkStreamContext<T, R, E>;
  using ResultOutput = typename Base::ResultOutput;
  using ErrorOutput = typename Base::ErrorOutput;

  StreamContext(ResultOutput resultOutput, ErrorOutput errorOutput)
      : Base(std::move(resultOutput), std::move(errorOutput)) {}
};

template <typename T, typename R, typename Handler,
          typename E = std::exception_ptr>
class BeastEndpoint final : public IEndpoint {
 public:
  using State = typename Handler::State;
  using StreamContext = http::StreamContext<T, R, E>;

  BeastEndpoint(SinkEndpointStream<T, R, E>& stream, Client& client,
                Handler handler)
      : environment_(stream.environment()),
        endpointId_(stream.endpointId()),
        streamName_(resolveStreamName(environment_, stream.streamConfigId())),
        endpointName_(endpointConfig(environment_, endpointId_).name),
        serviceName_(resolveServiceName(environment_)),
        executor_(detail::ParallelExecutorRegistry::Get()),
        client_(client),
        handler_(std::move(handler)),
        streamContext_(stream.resultOutput(), stream.errorOutput()),
        metrics_(environment_.getMetrics(), environment_.getLogger(),
                 connectorConfig(environment_, endpointId_).name,
                 endpointName_) {
    const auto endpoint = endpointConfig();
    if (endpoint.httpMethodType != api::HTTPMethodType::kGET &&
        endpoint.httpMethodType != api::HTTPMethodType::kPOST) {
      throw std::invalid_argument("HTTP sink endpoint method is undefined");
    }
  }

  [[nodiscard]] int id() const noexcept override { return endpointId_; }
  void start([[maybe_unused]] Context context) override { operations_.start(); }
  void stop([[maybe_unused]] Context context) override {
    operations_.stopAndWait();
  }

  void consume(MessageContext context, Payload<T> payload) {
    auto operation = operations_.acquire();
    if (!operation) return;
    auto completion = context.retainCompletion();
    boost::asio::co_spawn(
        executor_, run(std::move(context), std::move(payload)),
        [operation = std::move(operation),
         completion = std::move(completion)](std::exception_ptr) mutable {
          completion.reset();
          operation.reset();
        });
  }

  [[nodiscard]] config::HttpEndpointConfig endpointConfig() const {
    return endpointConfig(environment_, endpointId_);
  }
  [[nodiscard]] config::HttpDataConnectorConfig connectorConfig() const {
    return connectorConfig(environment_, endpointId_);
  }

 private:
  boost::asio::awaitable<void> run(MessageContext context, Payload<T> payload) {
    auto startedSpan = startTrace(context);
    std::stop_source externalCancellation;
    context = std::move(context).withExternalCancellation(
        externalCancellation.get_token());
    std::optional<servicelib::BeginResult<State>> beginResult;
    try {
      beginResult.emplace(handler_.beginRequest(context, streamContext_));
    } catch (...) {
      const auto error = std::current_exception();
      traceError(startedSpan.span(), error, "begin_request.error");
      metrics_.beginRequestFailed(tracing::ExceptionMessage(error));
      co_return;
    }
    tracing::SpanEvent(startedSpan.span(), "begin_request");
    auto begin = std::move(*beginResult);
    context = std::move(begin.context);
    const auto requestContext =
        context.withStreamId(servicelib::http::NewStreamId());
    const auto startedAt = metrics_.requestStart();
    std::exception_ptr error;
    std::optional<Request> request;
    bool messageConsumed = false;
    try {
      Requester requester;
      handler_.consumeMessage(context, streamContext_, begin.state,
                              payload.get(), requester);
      tracing::SpanEvent(startedSpan.span(), "consume_message");
      messageConsumed = true;
      request.emplace(requester.takeRequest());
    } catch (...) {
      error = std::current_exception();
      traceError(startedSpan.span(), error,
                 messageConsumed ? "no_request.error"
                                 : "consume_message.error");
    }
    if (!error) {
      request->headers[std::string{servicelib::kStreamIdHeader}] =
          requestContext.streamId();
      if (tracing::SamplingEnabled(context)) {
        request->headers[std::string{"X-Trace"}] = "1";
      }
      std::optional<Response> response;
      try {
        response.emplace(
            co_await client_.perform(std::move(*request), requestContext));
        tracing::SpanEvent(
            startedSpan.span(), "http_call",
            {tracing::Attribute::Int64("status_code", response->status)});
      } catch (...) {
        error = std::current_exception();
        traceError(startedSpan.span(), error, "http_call.error");
      }
      if (!error) {
        try {
          handler_.handleResponse(context, streamContext_, begin.state,
                                  *response);
          tracing::SpanEvent(startedSpan.span(), "handle_response");
        } catch (...) {
          error = std::current_exception();
          traceError(startedSpan.span(), error, "handle_response.error");
        }
      }
    }
    if (context.cancelled()) externalCancellation.request_stop();
    try {
      handler_.endRequest(context, streamContext_, error, begin.state);
    } catch (...) {
    }
    metrics_.requestEnd(startedAt, error);
  }

  [[nodiscard]] tracing::ActiveSpan startTrace(MessageContext& context) {
    if (!tracing::SamplingEnabled(context)) return {};
    auto* engine = environment_.getTracing();
    if (!engine) return {};
    auto tracer = engine->tracer(serviceName_);
    if (!tracer) return {};
    return tracing::StartSpanInPlace(
        context, tracer.get(), "http.output",
        {tracing::Attribute::String("stream", streamName_),
         tracing::Attribute::String("endpoint", endpointName_)});
  }

  static void traceError(tracing::Span* span, std::exception_ptr error,
                         std::string_view event) {
    const auto message = tracing::ExceptionMessage(error);
    tracing::SpanError(span, message);
    tracing::SpanEvent(span, event,
                       {tracing::Attribute::String("error", message)});
  }

  [[nodiscard]] static std::string resolveStreamName(
      const IServiceEnvironment& environment, std::size_t streamConfigId) {
    const auto runtime = environment.getRuntimeConfigSnapshot();
    if (!runtime || streamConfigId == 0) return {};
    const auto stream = runtime->GetStreamConfigByID(
        static_cast<int>(streamConfigId));
    return stream ? stream->GetName() : std::string{};
  }

  [[nodiscard]] static std::string resolveServiceName(
      const IServiceEnvironment& environment) {
    const auto service = environment.getServiceConfigSnapshot();
    return service ? service->name : std::string{};
  }

  static config::HttpEndpointConfig endpointConfig(
      const IServiceEnvironment& environment, int endpointId) {
    const auto runtime = environment.getRuntimeConfigSnapshot();
    if (!runtime) throw std::invalid_argument("runtime config is null");
    const auto endpoint = runtime->GetEndpointConfigByID(endpointId);
    const auto* http =
        endpoint ? endpoint->As<config::HttpEndpointConfig>() : nullptr;
    if (!http) {
      throw std::invalid_argument("HTTP endpoint config not found for id=" +
                                  std::to_string(endpointId));
    }
    return *http;
  }

  static config::HttpDataConnectorConfig connectorConfig(
      const IServiceEnvironment& environment, int endpointId) {
    const auto runtime = environment.getRuntimeConfigSnapshot();
    if (!runtime) throw std::invalid_argument("runtime config is null");
    const auto endpoint = endpointConfig(environment, endpointId);
    const auto connector =
        runtime->GetDataConnectorByID(endpoint.idDataConnector);
    const auto* http = connector
                           ? connector->template As<
                                 config::HttpDataConnectorConfig>()
                           : nullptr;
    if (!http) {
      throw std::invalid_argument(
          "HTTP data connector config not found for endpoint id=" +
          std::to_string(endpointId));
    }
    return *http;
  }

  IServiceEnvironment& environment_;
  int endpointId_;
  std::string streamName_;
  std::string endpointName_;
  std::string serviceName_;
  boost::asio::any_io_executor executor_;
  Client& client_;
  Handler handler_;
  StreamContext streamContext_;
  servicelib::DataSinkEndpointMetrics metrics_;
  detail::AsyncOperations operations_;
};

class BeastDataSink final {
 public:
  template <typename T, typename R, typename E>
  [[nodiscard]] static std::shared_ptr<BeastDataSink> make(
      SinkEndpointStream<T, R, E>& stream) {
    auto& environment = stream.environment();
    return std::shared_ptr<BeastDataSink>(new BeastDataSink(
        environment, connectorIdForEndpoint(environment, stream.endpointId())));
  }

  [[nodiscard]] int id() const noexcept { return connectorId_; }

  [[nodiscard]] config::HttpDataConnectorConfig config() const {
    const auto runtime = environment_.getRuntimeConfigSnapshot();
    const auto connector =
        runtime ? runtime->GetDataConnectorByID(connectorId_) : std::nullopt;
    const auto* http = connector
                           ? connector->template As<
                                 config::HttpDataConnectorConfig>()
                           : nullptr;
    if (!http) {
      throw std::invalid_argument("HTTP datasink config not found for id=" +
                                  std::to_string(connectorId_));
    }
    return *http;
  }

  void addEndpoint(std::shared_ptr<IEndpoint> endpoint) {
    if (!endpoint) {
      throw std::invalid_argument("HTTP datasink endpoint is null");
    }
    const auto configured =
        environment_.getRuntimeConfigSnapshot()->GetEndpointConfigByID(
            endpoint->id());
    if (!configured || configured->GetIdDataConnector() != connectorId_) {
      throw std::invalid_argument(
          "HTTP datasink endpoint belongs to another connector");
    }
    if (!endpoints_.emplace(endpoint->id(), std::move(endpoint)).second) {
      throw std::invalid_argument("duplicate HTTP datasink endpoint id");
    }
  }

  [[nodiscard]] std::shared_ptr<IEndpoint> endpoint(int endpointId) const {
    const auto it = endpoints_.find(endpointId);
    return it == endpoints_.end() ? nullptr : it->second;
  }

  void start(Context context) {
    std::vector<IEndpoint*> started;
    try {
      for (const auto& [_, endpoint] : endpoints_) {
        endpoint->start(context);
        started.push_back(endpoint.get());
      }
    } catch (...) {
      for (auto it = started.rbegin(); it != started.rend(); ++it) {
        (*it)->stop(context);
      }
      throw;
    }
  }

  void stop(Context context) {
    for (const auto& [_, endpoint] : endpoints_) endpoint->stop(context);
  }

 private:
  BeastDataSink(IServiceEnvironment& environment, int connectorId)
      : environment_(environment), connectorId_(connectorId) {
    static_cast<void>(config());
  }

  [[nodiscard]] static int connectorIdForEndpoint(
      IServiceEnvironment& environment, int endpointId) {
    const auto runtime = environment.getRuntimeConfigSnapshot();
    const auto endpoint =
        runtime ? runtime->GetEndpointConfigByID(endpointId) : std::nullopt;
    if (!endpoint) {
      throw std::invalid_argument("HTTP datasink endpoint config not found");
    }
    return endpoint->GetIdDataConnector();
  }

  IServiceEnvironment& environment_;
  int connectorId_;
  std::unordered_map<int, std::shared_ptr<IEndpoint>> endpoints_;
};

}  // namespace servicelib::datasink::http

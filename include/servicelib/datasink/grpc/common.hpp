#pragma once

#include <atomic>
#include <chrono>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>


#include <servicelib/runtime/common.hpp>
#include <servicelib/runtime/config/dataconnector_types.hpp>
#include <servicelib/runtime/config/endpoint_types.hpp>
#include <servicelib/runtime/datasink.hpp>
#include <servicelib/runtime/detail/async_operations.hpp>
#include <servicelib/runtime/environment/environment.hpp>
#include <servicelib/runtime/environment/tracing/tracing.hpp>
#include <servicelib/runtime/detail/http_types.hpp>

namespace servicelib::datasink::grpc {

template <typename Req>
class Sender final {
 public:
  using Send = std::function<void(Req)>;
  explicit Sender(Send send, std::shared_ptr<tracing::Span> requestSpan = {})
      : send_(std::move(send)), span_(std::move(requestSpan)) {
    if (!send_) throw std::invalid_argument("gRPC sender is empty");
  }
  void send(Req request) {
    try {
      send_(std::move(request));
      tracing::SpanEvent(span_.get(), "send");
    } catch (...) {
      const auto message = tracing::ExceptionMessage(std::current_exception());
      tracing::SpanError(span_.get(), message);
      tracing::SpanEvent(span_.get(), "send.error",
                         {tracing::Attribute::String("error", message)});
      throw;
    }
  }

 private:
  Send send_;
  std::shared_ptr<tracing::Span> span_;
};

class ResultContext final {
 public:
  ResultContext() = default;
  explicit ResultContext(std::function<void()> done) : done_(std::move(done)) {}
  void done() {
    if (done_) done_();
  }

 private:
  std::function<void()> done_;
};

template <typename T, typename R, typename E = std::exception_ptr>
class StreamContext final : public servicelib::SinkStreamContext<T, R, E> {
 public:
  using Base = servicelib::SinkStreamContext<T, R, E>;
  using ResultOutput = typename Base::ResultOutput;
  using ErrorOutput = typename Base::ErrorOutput;

  StreamContext(ResultOutput result = {}, ErrorOutput error = {})
      : Base(std::move(result), std::move(error)) {}
};

class IEndpoint {
 public:
  virtual ~IEndpoint() = default;
  [[nodiscard]] virtual int id() const noexcept = 0;
  virtual void start(Context) = 0;
  virtual void stop(Context) = 0;
};

class DataSink final {
 public:
  template <typename T, typename R, typename E>
  [[nodiscard]] static std::shared_ptr<DataSink> make(
      SinkEndpointStream<T, R, E>& stream) {
    auto& environment = stream.environment();
    return std::shared_ptr<DataSink>(new DataSink(
        environment, connectorIdForEndpoint(environment, stream.endpointId())));
  }
  [[nodiscard]] int id() const noexcept { return connectorId_; }
  [[nodiscard]] config::GrpcDataConnectorConfig config() const {
    const auto runtime = environment_.getRuntimeConfigSnapshot();
    const auto connector =
        runtime ? runtime->GetDataConnectorByID(connectorId_) : std::nullopt;
    const auto* grpc =
        connector ? connector->template As<config::GrpcDataConnectorConfig>()
                  : nullptr;
    if (!grpc) throw std::invalid_argument("gRPC datasink config not found");
    return *grpc;
  }
  void addEndpoint(std::shared_ptr<IEndpoint> endpoint) {
    if (!endpoint) throw std::invalid_argument("gRPC endpoint is null");
    const auto runtime = environment_.getRuntimeConfigSnapshot();
    const auto endpointConfig =
        runtime ? runtime->GetEndpointConfigByID(endpoint->id()) : std::nullopt;
    if (!endpointConfig ||
        endpointConfig->GetIdDataConnector() != connectorId_) {
      throw std::invalid_argument("gRPC endpoint belongs to another connector");
    }
    if (!endpoints_.emplace(endpoint->id(), std::move(endpoint)).second) {
      throw std::invalid_argument("duplicate gRPC endpoint id");
    }
  }
  [[nodiscard]] std::shared_ptr<IEndpoint> endpoint(int id) const {
    const auto it = endpoints_.find(id);
    return it == endpoints_.end() ? nullptr : it->second;
  }
  void start(Context context) {
    for (const auto& [_, endpoint] : endpoints_) endpoint->start(context);
  }
  void stop(Context context) {
    for (const auto& [_, endpoint] : endpoints_) endpoint->stop(context);
  }

 private:
  DataSink(IServiceEnvironment& environment, int connectorId)
      : environment_(environment), connectorId_(connectorId) {
    static_cast<void>(config());
  }

  [[nodiscard]] static int connectorIdForEndpoint(
      IServiceEnvironment& environment, int endpointId) {
    const auto runtime = environment.getRuntimeConfigSnapshot();
    const auto endpoint =
        runtime ? runtime->GetEndpointConfigByID(endpointId) : std::nullopt;
    if (!endpoint) {
      throw std::invalid_argument("gRPC datasink endpoint config not found");
    }
    return endpoint->GetIdDataConnector();
  }

  IServiceEnvironment& environment_;
  int connectorId_;
  std::unordered_map<int, std::shared_ptr<IEndpoint>> endpoints_;
};

struct CallOptions final {
  MessageContext context;
};

inline CallOptions callOptions(const MessageContext& context) {
  return CallOptions{context};
}

template <typename T, typename R, typename Handler,
          typename E = std::exception_ptr>
class Endpoint : public IEndpoint {
 public:
  using State = typename Handler::State;
  using ContextType = StreamContext<T, R, E>;

 protected:
  Endpoint(SinkEndpointStream<T, R, E>& stream,
           api::GrpcMethodType expectedMethod, Handler handler)
      : environment_(stream.environment()),
        endpointId_(stream.endpointId()),
        streamName_(resolveStreamName(environment_, stream.streamConfigId())),
        endpointName_(endpointConfig().name),
        serviceName_(resolveServiceName(environment_)),
        handler_(std::move(handler)),
        streamContext_(stream.resultOutput(), stream.errorOutput()),
        metrics_(environment_.getMetrics(), environment_.getLogger(),
                 connectorConfig().name, endpointName_, "grpc") {
    if (endpointConfig().grpcMethodType != expectedMethod) {
      throw std::invalid_argument("unexpected gRPC method type for endpoint");
    }
  }

 public:
  [[nodiscard]] int id() const noexcept override { return endpointId_; }
  void start([[maybe_unused]] Context context) override {
    asyncOperations_.start();
  }
  void stop([[maybe_unused]] Context context) override {
    asyncOperations_.stopAndWait();
  }

  [[nodiscard]] config::GrpcEndpointConfig endpointConfig() const {
    const auto config = environment_.getRuntimeConfigSnapshot();
    const auto endpoint =
        config ? config->GetEndpointConfigByID(endpointId_) : std::nullopt;
    const auto* grpc = endpoint
                           ? endpoint->template As<config::GrpcEndpointConfig>()
                           : nullptr;
    if (!grpc) throw std::invalid_argument("gRPC endpoint config not found");
    return *grpc;
  }

  [[nodiscard]] config::GrpcDataConnectorConfig connectorConfig() const {
    const auto config = environment_.getRuntimeConfigSnapshot();
    const auto endpoint = endpointConfig();
    const auto connector =
        config ? config->GetDataConnectorByID(endpoint.idDataConnector)
               : std::nullopt;
    const auto* grpc =
        connector ? connector->template As<config::GrpcDataConnectorConfig>()
                  : nullptr;
    if (!grpc) throw std::invalid_argument("gRPC connector config not found");
    return *grpc;
  }

 private:
  IServiceEnvironment& environment_;
  int endpointId_;
  std::string streamName_;
  std::string endpointName_;
  std::string serviceName_;

 protected:
  [[nodiscard]] tracing::ActiveSpan startTrace(MessageContext& context) {
    if (!tracing::SamplingEnabled(context)) {
      return {};
    }
    std::shared_ptr<tracing::Tracer> tracer;
    if (auto* tracingEngine = environment_.getTracing()) {
      tracer = tracingEngine->tracer(serviceName_);
    }
    if (!tracer) {
      return {};
    }
    return tracing::StartSpanInPlace(
        context, tracer.get(), "grpc.output",
        {
            tracing::Attribute::String("stream", streamName_),
            tracing::Attribute::String("endpoint", endpointName_),
        });
  }

  struct DetachedTrace final {
    MessageContext context;
    std::shared_ptr<tracing::Span> span;
  };

  [[nodiscard]] DetachedTrace startDetachedTrace(MessageContext context) {
    if (!tracing::SamplingEnabled(context)) {
      return {std::move(context), {}};
    }
    auto* tracingEngine = environment_.getTracing();
    auto tracer = tracingEngine ? tracingEngine->tracer(serviceName_) : nullptr;
    if (!tracer) {
      return {std::move(context), {}};
    }
    auto parent = context.trace();
    if (!parent.isValid()) parent = tracer->currentSpanContext();
    auto span = tracer->startDetachedChildOf(
        "grpc.output", parent,
        {
            tracing::Attribute::String("stream", streamName_),
            tracing::Attribute::String("endpoint", endpointName_),
        });
    if (span) context = std::move(context).withTrace(span->spanContext());
    return {std::move(context), std::move(span)};
  }

  static void traceError(tracing::Span* span, const std::exception_ptr& error,
                         std::string_view event = {}) {
    const auto message = tracing::ExceptionMessage(error);
    tracing::SpanError(span, message);
    if (!event.empty()) {
      tracing::SpanEvent(span, event,
                         {tracing::Attribute::String("error", message)});
    }
  }

  MessageContext ensureStreamId(MessageContext context) const {
    if (context.streamId().empty()) {
      context = std::move(context).withStreamId(servicelib::http::NewStreamId());
    }
    return context;
  }

  MessageContext newRequestStreamId(MessageContext context) const {
    return std::move(context).withStreamId(servicelib::http::NewStreamId());
  }

  void callEnd(MessageContext context, std::exception_ptr error,
               State& state) noexcept {
    try {
      handler_.endRequest(std::move(context), streamContext_, error, state);
    } catch (...) {
      // EndRequest is noexcept by the Go-compatible handler contract.
    }
  }

  Handler handler_;
  ContextType streamContext_;
  DataSinkEndpointMetrics metrics_;
  detail::AsyncOperations asyncOperations_;

 private:
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
};

}  // namespace servicelib::datasink::grpc

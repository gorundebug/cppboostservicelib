#pragma once

#include <atomic>
#include <chrono>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>


#include <servicelib/runtime/common.hpp>
#include <servicelib/runtime/config/dataconnector_types.hpp>
#include <servicelib/runtime/config/endpoint_types.hpp>
#include <servicelib/runtime/datasource.hpp>
#include <servicelib/runtime/environment/environment.hpp>
#include <servicelib/runtime/environment/tracing/tracing.hpp>
#include <servicelib/runtime/store/rotatingmap.hpp>
#include <servicelib/runtime/detail/grpc_context.hpp>
#include <servicelib/runtime/detail/http_types.hpp>
#include <servicelib/runtime/detail/sync.hpp>
#include <grpcpp/server_context.h>

namespace servicelib::datasource::grpc {

inline constexpr auto kPendingRotationInterval = std::chrono::seconds{30};

class RpcCancelledError final : public std::runtime_error {
 public:
  RpcCancelledError() : std::runtime_error("gRPC request cancelled") {}
};

inline MessageContext messageContext(::grpc::ServerContext& call,
                                     bool tracingEnabled = true) {
  return servicelib::grpc_transport::ExtractContext(call, tracingEnabled);
}

template <typename Res>
class Sender final {
 public:
  using Send = std::function<void(Res)>;

  explicit Sender(Send send, std::shared_ptr<tracing::Span> requestSpan = {})
      : send_(std::move(send)), span_(std::move(requestSpan)) {
    if (!send_) throw std::invalid_argument("gRPC sender is empty");
  }

  void send(Res value) {
    std::lock_guard lock(mu_);
    if (!active_) throw std::runtime_error("gRPC stream is closed");
    try {
      send_(std::move(value));
      tracing::SpanEvent(span_.get(), "send");
    } catch (...) {
      const auto message = tracing::ExceptionMessage(std::current_exception());
      tracing::SpanError(span_.get(), message);
      tracing::SpanEvent(span_.get(), "send.error",
                         {tracing::Attribute::String("error", message)});
      throw;
    }
  }

  void close() noexcept {
    std::lock_guard lock(mu_);
    active_ = false;
  }

 private:
  std::mutex mu_;
  Send send_;
  std::shared_ptr<tracing::Span> span_;
  bool active_{true};
};

template <typename State, typename T, typename Res, typename R,
          typename E = std::exception_ptr>
struct RequestState final {
  using StreamContext = servicelib::SourceStreamContext<T, R, E>;
  using Callback = std::function<bool(MessageContext, StreamContext&, State&,
                                      const R&, Sender<Res>&)>;

  RequestState(MessageContext contextValue, State stateValue,
               std::shared_ptr<Sender<Res>> senderValue,
               std::shared_ptr<tracing::Span> requestSpan)
      : context(std::move(contextValue)),
        state(std::move(stateValue)),
        sender(std::move(senderValue)),
        span(std::move(requestSpan)) {}

  MessageContext context;
  State state;
  std::shared_ptr<Sender<Res>> sender;
  std::shared_ptr<tracing::Span> span;
  servicelib::detail::SingleUseEvent done;
  std::atomic<bool> doneSent{false};
  std::atomic<bool> pendingInserted{false};
  std::shared_mutex lifetimeMutex;
  std::mutex callbacksMutex;
  std::unordered_map<std::string, Callback> callbacks;
};

template <typename State, typename T, typename Res, typename R,
          typename E = std::exception_ptr>
class ResultContext final {
 public:
  using Request = RequestState<State, T, Res, R, E>;
  using Callback = typename Request::Callback;

  ResultContext() = default;
  explicit ResultContext(std::shared_ptr<Request> request)
      : request_(std::move(request)) {}

  void setResultCallback(std::string messageId, Callback callback) {
    if (!request_) return;
    std::lock_guard lock(request_->callbacksMutex);
    request_->callbacks[std::move(messageId)] = std::move(callback);
  }

  void done() noexcept {
    if (!request_) return;
    bool expected = false;
    if (request_->doneSent.compare_exchange_strong(expected, true,
                                                   std::memory_order_acq_rel)) {
      tracing::SpanEvent(request_->span.get(), "done_called");
      request_->done.Send();
    }
  }

 private:
  std::shared_ptr<Request> request_;
};

class IEndpoint {
 public:
  virtual ~IEndpoint() = default;
  [[nodiscard]] virtual int id() const noexcept = 0;
  virtual void start(Context context) = 0;
  virtual void stop(Context context) = 0;
};

// Connector-level lifecycle owner. The generated userver service component
// registers its generated service with ugrpc and keeps the typed endpoints;
// this object owns only config-driven endpoint lifecycle, as in Go.
class DataSource final {
 public:
  template <typename Input>
  [[nodiscard]] static std::shared_ptr<DataSource> make(
      IServiceEnvironment& environment, const Input& input) {
    return std::shared_ptr<DataSource>(new DataSource(
        environment, connectorIdForEndpoint(environment, input.getEndpointId())));
  }

  [[nodiscard]] int id() const noexcept { return connectorId_; }
  [[nodiscard]] config::GrpcDataConnectorConfig config() const {
    const auto runtime = environment_.getRuntimeConfigSnapshot();
    const auto connector =
        runtime ? runtime->GetDataConnectorByID(connectorId_) : std::nullopt;
    const auto* grpc =
        connector ? connector->template As<config::GrpcDataConnectorConfig>()
                  : nullptr;
    if (!grpc) throw std::invalid_argument("gRPC datasource config not found");
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
  DataSource(IServiceEnvironment& environment, int connectorId)
      : environment_(environment), connectorId_(connectorId) {
    static_cast<void>(config());
  }

  [[nodiscard]] static int connectorIdForEndpoint(
      IServiceEnvironment& environment, int endpointId) {
    const auto runtime = environment.getRuntimeConfigSnapshot();
    const auto endpoint =
        runtime ? runtime->GetEndpointConfigByID(endpointId) : std::nullopt;
    if (!endpoint) {
      throw std::invalid_argument("gRPC datasource endpoint config not found");
    }
    return endpoint->GetIdDataConnector();
  }

  IServiceEnvironment& environment_;
  int connectorId_;
  std::unordered_map<int, std::shared_ptr<IEndpoint>> endpoints_;
};

// Common typed endpoint implementation. The four mode-specific endpoint
// classes only define how requests are read and responses are written. This is
// the same split as datasource/grpc in Go.
template <typename Req, typename Res, typename T, typename R, typename Handler,
          typename E = std::exception_ptr>
class Endpoint : public IEndpoint {
 public:
  using State = typename Handler::State;
  using StreamContext = servicelib::SourceStreamContext<T, R, E>;
  using Request = RequestState<State, T, Res, R, E>;
  using ResultCtx = ResultContext<State, T, Res, R, E>;
  using Output = typename StreamContext::Output;
  using ErrorOutput = typename StreamContext::ErrorOutput;

 protected:
  Endpoint(IServiceEnvironment& environment, int endpointId,
           api::GrpcMethodType expectedMethod, Handler handler, Output output,
           bool hasResult, ErrorOutput errorOutput = {})
      : Endpoint(environment, endpointId, 0, expectedMethod, std::move(handler),
                 std::move(output), hasResult, std::move(errorOutput)) {}

  Endpoint(IServiceEnvironment& environment, int endpointId, int streamConfigId,
           api::GrpcMethodType expectedMethod, Handler handler, Output output,
           bool hasResult, ErrorOutput errorOutput = {})
      : environment_(environment),
        endpointId_(endpointId),
        streamName_(resolveStreamName(environment, streamConfigId)),
        endpointName_(endpointConfig().name),
        handler_(std::move(handler)),
        streamContext_(std::move(output), std::move(errorOutput)),
        hasResult_(hasResult),
        pending_(kPendingRotationInterval),
        metrics_(environment.getMetrics(), environment.getLogger(),
                 connectorConfig().name, endpointName_, "grpc") {
    const auto endpoint = endpointConfig();
    if (endpoint.grpcMethodType != expectedMethod) {
      throw std::invalid_argument("unexpected gRPC method type for endpoint " +
                                  endpoint.name);
    }
  }

 public:
  [[nodiscard]] int id() const noexcept override { return endpointId_; }
  void start(Context context) override {
    if (hasResult_) pending_.start(std::move(context));
  }
  void stop(Context context) override {
    if (hasResult_) pending_.stop(std::move(context));
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

  [[nodiscard]] tracing::ActiveSpan startTrace(MessageContext& context) {
    context = applyEndpointTracing(std::move(context));
    if (!tracing::SamplingEnabled(context)) {
      return {};
    }
    std::shared_ptr<tracing::Tracer> tracer;
    if (auto* tracingEngine = environment_.getTracing()) {
      tracer = tracingEngine->tracer(environment_.getServiceName());
    }
    if (!tracer) {
      return {};
    }
    return tracing::StartSpanInPlace(
        context, tracer.get(), "grpc.input",
        {
            tracing::Attribute::String("stream", streamName_),
            tracing::Attribute::String("endpoint", endpointName_),
        });
  }

  std::shared_ptr<Request> begin(MessageContext context,
                                 std::shared_ptr<Sender<Res>> sender,
                                 std::shared_ptr<tracing::Span> span) {
    try {
      auto begin = handler_.beginRequest(std::move(context), streamContext_);
      if (begin.context.streamId().empty()) {
        begin.context = std::move(begin.context).withStreamId(
            servicelib::http::NewStreamId());
      }
      auto request = std::make_shared<Request>(
          std::move(begin.context), std::move(begin.state), std::move(sender),
          std::move(span));
      tracing::SpanEvent(request->span.get(), "begin_request");
      if (request->span) {
        tracing::SpanAttrs(
            request->span.get(),
            {
                tracing::Attribute::String(
                    "stream_id", std::string{request->context.streamId()}),
                tracing::Attribute::Bool("has_result", hasResult_),
            });
      }
      return request;
    } catch (...) {
      const auto message = tracing::ExceptionMessage(std::current_exception());
      tracing::SpanError(span.get(), message);
      tracing::SpanEvent(span.get(), "begin_request.error",
                         {tracing::Attribute::String("error", message)});
      metrics_.beginRequestFailed(message);
      throw;
    }
  }

  void activate(const std::shared_ptr<Request>& request) {
    if (!hasResult_) return;
    pending_.set(std::string{request->context.streamId()}, request);
    request->pendingInserted.store(true, std::memory_order_release);
    metrics_.pendingAdd(std::string{request->context.streamId()});
  }

  void consume(const std::shared_ptr<Request>& request, const Req& value) {
    try {
      handler_.consumeMessage(
          request->context, streamContext_, request->state, value,
          hasResult_ ? ResultCtx{request} : ResultCtx{}, *request->sender);
      tracing::SpanEvent(request->span.get(), "consume_message");
    } catch (...) {
      const auto message = tracing::ExceptionMessage(std::current_exception());
      tracing::SpanError(request->span.get(), message);
      tracing::SpanEvent(request->span.get(), "consume_message.error",
                         {tracing::Attribute::String("error", message)});
      throw;
    }
  }

  void eof(const std::shared_ptr<Request>& request,
           std::optional<std::int64_t> messagesReceived = std::nullopt) {
    handler_.eof(request->context, streamContext_, request->state);
    if (messagesReceived) {
      tracing::SpanEvent(
          request->span.get(), "eof",
          {tracing::Attribute::Int64("messages_received", *messagesReceived)});
    } else {
      tracing::SpanEvent(request->span.get(), "eof");
    }
  }

  void waitDone(const std::shared_ptr<Request>& request) {
    if (hasResult_) {
      request->done.Wait();
      tracing::SpanEvent(request->span.get(), "done_received");
    }
  }

  void recordFailure(const std::shared_ptr<Request>& request,
                     const std::exception_ptr& error) {
    const auto message = tracing::ExceptionMessage(error);
    tracing::SpanError(request->span.get(), message);
    if (request->context.cancelled()) {
      tracing::SpanEvent(request->span.get(), "context_cancelled",
                         {tracing::Attribute::String("error", message)});
    }
  }

  template <typename CompletionReady>
  void finish(const std::shared_ptr<Request>& request,
              std::exception_ptr& error, CompletionReady&& completionReady) {
    std::unique_lock lifetimeLock(request->lifetimeMutex);
    if (error && std::forward<CompletionReady>(completionReady)()) {
      error = nullptr;
      tracing::SpanEvent(request->span.get(), "done_received");
    }
    if (request->pendingInserted.exchange(false, std::memory_order_acq_rel)) {
      static_cast<void>(pending_.pop(std::string{request->context.streamId()}));
      metrics_.pendingRemove(std::string{request->context.streamId()});
    }
    request->sender->close();
    try {
      handler_.endRequest(request->context, streamContext_, error,
                          request->state);
    } catch (...) {
      const auto endError = std::current_exception();
      tracing::SpanError(request->span.get(),
                         tracing::ExceptionMessage(endError));
      if (!error) std::rethrow_exception(endError);
    }
  }

  void finish(const std::shared_ptr<Request>& request,
              std::exception_ptr& error) {
    finish(request, error, [] { return false; });
  }

  void consumeResult(MessageContext context, Payload<R> payload) {
    if (context.streamId().empty()) {
      metrics_.missingStreamId();
      return;
    }
    const std::string streamId{context.streamId()};
    auto found = pending_.get(streamId);
    if (!found) {
      metrics_.lateResult(streamId);
      return;
    }
    auto request = *found;
    std::shared_lock lifetimeLock(request->lifetimeMutex);
    const auto current = pending_.get(streamId);
    if (!current || *current != request) {
      metrics_.lateResult(streamId);
      tracing::SpanEvent(request->span.get(), "late_result");
      return;
    }
    const auto messageId = handler_.getMessageId(context, streamContext_,
                                                 request->state, payload.get());
    typename Request::Callback callback;
    {
      std::lock_guard lock(request->callbacksMutex);
      const auto it = request->callbacks.find(messageId);
      if (it != request->callbacks.end()) callback = it->second;
    }
    if (!callback) {
      metrics_.unknownMessageId(streamId, messageId);
      tracing::SpanEvent(request->span.get(), "unknown_message_id",
                         {tracing::Attribute::String("message_id", messageId)});
      return;
    }
    if (callback(std::move(context), streamContext_, request->state,
                 payload.get(), *request->sender)) {
      bool duplicate = false;
      {
        std::lock_guard lock(request->callbacksMutex);
        duplicate = request->callbacks.erase(messageId) == 0;
      }
      if (duplicate) {
        metrics_.duplicateMessageId(streamId, messageId);
        tracing::SpanEvent(
            request->span.get(), "duplicate_message_id",
            {tracing::Attribute::String("message_id", messageId)});
      }
    }
    if (request->span) {
      tracing::SpanEvent(request->span.get(), "result_consumed",
                         {tracing::Attribute::String("message_id", messageId)});
    }
  }

  [[nodiscard]] bool hasResult() const noexcept { return hasResult_; }
  DataSourceEndpointMetrics& metrics() noexcept { return metrics_; }
  [[nodiscard]] bool tracingEnabled() const noexcept {
    return environment_.getTracing() != nullptr;
  }

  [[nodiscard]] MessageContext applyEndpointTracing(
      MessageContext context) const {
    return ApplyDataSourceEndpointTracing(std::move(context), environment_,
                                          endpointId_);
  }

 private:
  static std::string resolveStreamName(
      const IServiceEnvironment& environment, int streamConfigId) {
    const auto runtime = environment.getRuntimeConfigSnapshot();
    if (!runtime || streamConfigId == 0) return {};
    const auto stream = runtime->GetStreamConfigByID(streamConfigId);
    return stream ? stream->GetName() : std::string{};
  }

  IServiceEnvironment& environment_;
  int endpointId_;
  std::string streamName_;
  std::string endpointName_;

 protected:
  Handler handler_;
  StreamContext streamContext_;

 private:
  bool hasResult_;
  store::RotatingMap<std::string, std::shared_ptr<Request>> pending_;
  DataSourceEndpointMetrics metrics_;
};

template <typename Input, typename Endpoint, typename Handler>
class EndpointConsumer final {
 public:
  static std::shared_ptr<EndpointConsumer> make(
      IServiceEnvironment& environment, Input& input,
      Handler handler) {
    auto result = std::shared_ptr<EndpointConsumer>(new EndpointConsumer(
        environment, input, std::move(handler)));
    if (result->input_.getResultStream()) result->bindResult();
    return result;
  }

  [[nodiscard]] const std::shared_ptr<Endpoint>& endpoint() const noexcept {
    return endpoint_;
  }

 private:
  EndpointConsumer(IServiceEnvironment& environment,
                   Input& input, Handler handler)
      : input_(input),
        endpoint_(std::make_shared<Endpoint>(
            environment, input_.getEndpointId(), input_.getConfigId(),
            std::move(handler),
            [input = &input_](MessageContext context, auto payload) {
              input->consume(std::move(context), std::move(payload));
            },
            input_.getResultStream() != nullptr,
            [input = &input_](MessageContext context, auto error) {
              input->consumeError(std::move(context), std::move(error));
            })) {}

  void bindResult() {
    auto* endpointObserver = endpoint_.get();
    input_.setResultConsumer(
        [endpointObserver](MessageContext context, auto result) {
          endpointObserver->consumeResult(std::move(context),
                                          std::move(result));
        });
  }

  Input& input_;
  std::shared_ptr<Endpoint> endpoint_;
};

}  // namespace servicelib::datasource::grpc

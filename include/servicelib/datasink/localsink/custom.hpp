#pragma once

#include <exception>
#include <functional>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>

#include <servicelib/runtime/common.hpp>
#include <servicelib/runtime/config/dataconnector_types.hpp>
#include <servicelib/runtime/config/endpoint_types.hpp>
#include <servicelib/runtime/datasink.hpp>
#include <servicelib/runtime/environment/environment.hpp>
#include <servicelib/runtime/environment/tracing/tracing.hpp>

namespace servicelib::datasink::localsink {

class IEndpoint {
 public:
  virtual ~IEndpoint() = default;
  [[nodiscard]] virtual int id() const noexcept = 0;
  virtual void start(Context) = 0;
  virtual void stop(Context) = 0;
};

// Go lifecycle: GetStreamID -> BeginRequest -> ConsumeMessage -> EndRequest.
template <typename T, typename R, typename Handler,
          typename E = std::exception_ptr>
class Endpoint final : public IEndpoint {
 public:
  using State = typename Handler::State;
  using StreamContext = SinkStreamContext<T, R, E>;
  using SinkCallback =
      std::function<void(MessageContext, const T&, std::exception_ptr)>;

  Endpoint(SinkEndpointStream<T, R, E>& stream, Handler handler)
      : environment_(stream.environment()),
        endpointId_(stream.endpointId()),
        streamName_(resolveStreamName(
            environment_, static_cast<int>(stream.streamConfigId()))),
        endpointName_(endpointConfig().name),
        handler_(std::move(handler)),
        streamContext_(stream.resultOutput(), stream.errorOutput()),
        metrics_(environment_.getMetrics(), environment_.getLogger(),
                 connectorConfig().name, endpointName_) {}

  [[nodiscard]] int id() const noexcept override { return endpointId_; }
  void start(Context) override {}
  void stop(Context) override {}

  // Mirrors Go's SetSinkCallback. The callback observes completion of the
  // custom sink lifecycle and receives the original message context/value.
  void setSinkCallback(SinkCallback callback) {
    sinkCallback_ = std::move(callback);
  }

  void consume(MessageContext context, Payload<T> payload) {
    auto startedSpan = startTrace(context);
    const auto originalContext = context;
    auto streamId = handler_.getStreamId(context, payload.get());
    context = std::move(context).withStreamId(std::move(streamId));
    if (startedSpan.span()) {
      tracing::SpanAttrs(startedSpan.span(),
                         {tracing::Attribute::String(
                             "stream_id", std::string{context.streamId()})});
    }
    std::optional<BeginResult<State>> begin;
    try {
      begin.emplace(handler_.beginRequest(context, streamContext_));
    } catch (...) {
      const auto message = tracing::ExceptionMessage(std::current_exception());
      tracing::SpanError(startedSpan.span(), message);
      metrics_.beginRequestFailed(message);
      return;
    }
    tracing::SpanEvent(startedSpan.span(), "begin_request");
    context = std::move(begin->context);
    const auto startedAt = metrics_.requestStart();
    std::exception_ptr error;
    try {
      handler_.consumeMessage(context, streamContext_, begin->state,
                              payload.get());
    } catch (...) {
      error = std::current_exception();
      const auto message = tracing::ExceptionMessage(error);
      tracing::SpanError(startedSpan.span(), message);
      tracing::SpanEvent(startedSpan.span(), "consume_message.error",
                         {tracing::Attribute::String("error", message)});
      streamContext_.collectError(context, error);
    }
    if (!error) tracing::SpanEvent(startedSpan.span(), "consume_message");
    try {
      handler_.endRequest(context, streamContext_, error, begin->state);
    } catch (...) {
      // endRequest is noexcept by contract.
    }
    metrics_.requestEnd(startedAt, error);
    if (sinkCallback_) sinkCallback_(originalContext, payload.get(), error);
  }

 private:
  static std::string resolveStreamName(const IServiceEnvironment& environment,
                                       int streamConfigId) {
    const auto runtime = environment.getRuntimeConfigSnapshot();
    if (!runtime || streamConfigId == 0) return {};
    const auto stream = runtime->GetStreamConfigByID(streamConfigId);
    return stream ? stream->GetName() : std::string{};
  }

  [[nodiscard]] tracing::ActiveSpan startTrace(MessageContext& context) {
    if (!tracing::SamplingEnabled(context)) return {};
    auto* engine = environment_.getTracing();
    if (!engine) return {};
    auto tracer = engine->tracer(environment_.getServiceName());
    if (!tracer) return {};
    return tracing::StartSpanInPlace(
        context, tracer.get(), "local.output",
        {
            tracing::Attribute::String("stream", streamName_),
            tracing::Attribute::String("endpoint", endpointName_),
        });
  }

  config::CustomEndpointConfig endpointConfig() const {
    const auto runtime = environment_.getRuntimeConfigSnapshot();
    const auto value =
        runtime ? runtime->GetEndpointConfigByID(endpointId_) : std::nullopt;
    const auto* config =
        value ? value->As<config::CustomEndpointConfig>() : nullptr;
    if (!config)
      throw std::invalid_argument("custom endpoint config not found");
    return *config;
  }

  config::CustomDataConnectorConfig connectorConfig() const {
    const auto runtime = environment_.getRuntimeConfigSnapshot();
    const auto endpoint = endpointConfig();
    const auto value = runtime->GetDataConnectorByID(endpoint.idDataConnector);
    const auto* config =
        value ? value->template As<config::CustomDataConnectorConfig>()
              : nullptr;
    if (!config)
      throw std::invalid_argument("custom connector config not found");
    return *config;
  }

  IServiceEnvironment& environment_;
  int endpointId_;
  std::string streamName_;
  std::string endpointName_;
  Handler handler_;
  StreamContext streamContext_;
  DataSinkEndpointMetrics metrics_;
  SinkCallback sinkCallback_;
};

}  // namespace servicelib::datasink::localsink

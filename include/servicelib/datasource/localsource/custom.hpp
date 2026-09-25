#pragma once

#include <servicelib/runtime/stream_tracing.hpp>
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdlib>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>

#include <servicelib/runtime/common.hpp>
#include <servicelib/runtime/config/dataconnector_types.hpp>
#include <servicelib/runtime/config/endpoint_types.hpp>
#include <servicelib/runtime/datasource.hpp>
#include <servicelib/runtime/detail/http_types.hpp>
#include <servicelib/runtime/detail/sync.hpp>
#include <servicelib/runtime/environment/environment.hpp>
#include <servicelib/runtime/environment/tracing/tracing.hpp>
#include <servicelib/runtime/store/rotatingmap.hpp>

#include <servicelib/datasource/detail/result_context.hpp>

namespace servicelib::datasource::localsource {

inline constexpr auto kPendingRotationInterval = std::chrono::seconds{30};

template <typename T>
class DataProducer {
 public:
  using Consumer = std::function<void(MessageContext, Payload<T>)>;
  virtual ~DataProducer() = default;
  // start may block for the complete producer lifetime. The endpoint runs it
  // in managed runtime work, matching the Go producer goroutine.
  virtual void start(Context context, Consumer consumer) = 0;
  virtual void stop(Context context) = 0;
};

class IEndpoint {
 public:
  virtual ~IEndpoint() = default;
  [[nodiscard]] virtual int id() const noexcept = 0;
  virtual void start(Context context) = 0;
  virtual void stop(Context context) = 0;
};

// Handler lifecycle and callback contract are the C++ spelling of
// datasource/localsource in Go:
//   concurrency -> beginRequest -> consumeMessage -> [done] -> endRequest.
template <typename T, typename R, typename Handler,
          typename E = std::exception_ptr, typename Input = T>
class Endpoint final : public IEndpoint {
 public:
  using State = typename Handler::State;
  using StreamContext = SourceStreamContext<T, R, E>;
  using Result = PendingResult<State, T, R, E>;
  using Output = typename StreamContext::Output;
  using ErrorOutput = typename StreamContext::ErrorOutput;

  template <typename InputStreamType>
  static std::shared_ptr<Endpoint> make(
      IServiceEnvironment& environment,
      InputStreamType& input, DataProducer<Input>& producer,
      Handler& handler) {
    auto endpoint = std::shared_ptr<Endpoint>(new Endpoint(
        environment, input.getEndpointId(),
        static_cast<int>(input.getConfigId()), producer, handler,
        [input = &input](MessageContext context, Payload<T> value) {
          input->consume(std::move(context), std::move(value));
        },
        input.getResultStream() != nullptr,
        [input = &input](MessageContext context, Payload<E> error) {
          input->consumeError(std::move(context), std::move(error));
        }, nullptr));
    if (input.getResultStream() != nullptr) {
      auto* endpointObserver = endpoint.get();
      input.setResultConsumer(
          [endpointObserver](MessageContext context, Payload<R> result) {
            endpointObserver->consumeResult(std::move(context),
                                            std::move(result));
          });
    }
    return endpoint;
  }

  Endpoint(IServiceEnvironment& environment, int endpointId,
           DataProducer<Input>& producer, Handler handler, Output output,
           bool hasResult, ErrorOutput errorOutput = {})
      : Endpoint(environment, endpointId, 0, producer, std::move(handler),
                 std::move(output), hasResult,
                 connectorConfig(environment, endpointId).name,
                 endpointConfig(environment, endpointId).name,
                 std::move(errorOutput)) {}

  Endpoint(IServiceEnvironment& environment, int endpointId, int streamConfigId,
           DataProducer<Input>& producer, Handler handler, Output output,
           bool hasResult, ErrorOutput errorOutput = {})
      : Endpoint(environment, endpointId, streamConfigId, producer,
                 std::move(handler), std::move(output), hasResult,
                 connectorConfig(environment, endpointId).name,
                 endpointConfig(environment, endpointId).name,
                 std::move(errorOutput)) {}

  Endpoint(IServiceEnvironment& environment, int endpointId,
           DataProducer<Input>& producer, Handler handler, Output output,
           bool hasResult, std::string connectorName, std::string endpointName,
           ErrorOutput errorOutput = {}, bool processInline = true)
      : Endpoint(environment, endpointId, 0, producer, std::move(handler),
                 std::move(output), hasResult, std::move(connectorName),
                 std::move(endpointName), std::move(errorOutput), processInline,
                 "local.input") {}

  Endpoint(IServiceEnvironment& environment, int endpointId, int streamConfigId,
           DataProducer<Input>& producer, Handler handler, Output output,
           bool hasResult, std::string connectorName, std::string endpointName,
           ErrorOutput errorOutput = {}, bool /*processInline*/ = true,
           std::string traceOperation = "local.input")
      : environment_(environment),
        endpointId_(endpointId),
        tracingEngineAvailable_(environment.getTracing() != nullptr),
        streamIdentity_(resolveStreamIdentity(environment, streamConfigId)),
        endpointName_(endpointName),
        producer_(producer),
        ownedHandler_(std::move(handler)),
        handler_(&*ownedHandler_),
        streamContext_(std::move(output), std::move(errorOutput)),
        hasResult_(hasResult),
        traceOperation_(std::move(traceOperation)),
        pending_(kPendingRotationInterval),
        metrics_(environment.getMetrics(), environment.getLogger(),
                 std::move(connectorName), endpointName_) {}

 private:
  Endpoint(IServiceEnvironment& environment, int endpointId, int streamConfigId,
           DataProducer<Input>& producer, Handler& handler, Output output,
           bool hasResult, ErrorOutput errorOutput, std::nullptr_t)
      : environment_(environment),
        endpointId_(endpointId),
        tracingEngineAvailable_(environment.getTracing() != nullptr),
        streamIdentity_(resolveStreamIdentity(environment, streamConfigId)),
        endpointName_(endpointConfig(environment, endpointId).name),
        producer_(producer),
        handler_(&handler),
        streamContext_(std::move(output), std::move(errorOutput)),
        hasResult_(hasResult),
        traceOperation_("local.input"),
        pending_(kPendingRotationInterval),
        metrics_(environment.getMetrics(), environment.getLogger(),
                 connectorConfig(environment, endpointId).name,
                 endpointName_) {}

 public:

  ~Endpoint() override {
    if (started_.load(std::memory_order_acquire)) std::abort();
  }

  [[nodiscard]] int id() const noexcept override { return endpointId_; }

  void start(Context context) override {
    bool expected = false;
    if (!started_.compare_exchange_strong(expected, true,
                                          std::memory_order_acq_rel)) {
      throw std::logic_error("custom datasource endpoint already started");
    }
    requestStopSource_ = std::stop_source{};
    {
      std::lock_guard lock(concurrencyMutex_);
      stopped_ = false;
    }
    if (hasResult_) pending_.start(context);
    try {
      // DataProducer::start is explicitly allowed to block for the producer
      // lifetime. Give that boundary its own endpoint-owned thread so neither
      // a reactor worker nor the bounded shared blocking executor is occupied
      // permanently.
      producerThread_ = std::thread([this, context] {
        try {
          producer_.start(context, [this](MessageContext messageContext,
                                          Payload<Input> payload) {
            submit(std::move(messageContext), std::move(payload));
          });
        } catch (const std::exception& error) {
          try {
            environment_.getLogger().error(
                "datasource producer stopped",
                {log::Field::Str("endpoint", endpointName_),
                 log::Field::Err(error)});
          } catch (...) {
          }
        } catch (...) {
          try {
            environment_.getLogger().error(
                "datasource producer stopped with an unknown error",
                {log::Field::Str("endpoint", endpointName_)});
          } catch (...) {
          }
        }
      });
    } catch (...) {
      if (hasResult_) pending_.stop(context);
      started_.store(false, std::memory_order_release);
      throw;
    }
  }

  void stop(Context context) override {
    if (!started_.exchange(false, std::memory_order_acq_rel)) return;
    {
      std::lock_guard lock(concurrencyMutex_);
      stopped_ = true;
      concurrencyCv_.notify_all();
    }
    // Wake result waits before stopping a producer whose stop() may wait for
    // its currently executing consumer callback (notably librdkafka polling).
    requestStopSource_.request_stop();
    try {
      producer_.stop(context);
    } catch (...) {
      // Continue joining endpoint work; shutdown safety must not depend on a
      // user producer honoring its noexcept-style stop contract.
    }
    if (producerThread_.joinable()) producerThread_.join();
    {
      std::unique_lock lock(concurrencyMutex_);
      concurrencyCv_.wait(lock, [this] { return active_ == 0; });
    }
    if (hasResult_) pending_.stop(std::move(context));
  }

  // Result stream entry point. getMessageId and callbacks may execute
  // concurrently, as in Go; handler State must synchronize shared access.
  void consumeResult(MessageContext context, Payload<R> payload) {
    if (!hasResult_) return;
    if (context.streamId().empty()) {
      metrics_.missingStreamId();
      return;
    }
    const auto found = pending_.get(std::string{context.streamId()});
    if (!found) {
      metrics_.lateResult(context.streamId());
      return;
    }
    const auto& result = *found;
    std::shared_lock lifetimeLock(result->lifetimeMutex);
    const auto current = pending_.get(std::string{context.streamId()});
    if (!current || *current != result) {
      metrics_.lateResult(context.streamId());
      if (auto* traceSpan = result->span.get()) traceSpan->addEvent("late_result");
      return;
    }
    const auto messageId = handler_->getMessageId(context, streamContext_,
                                                 result->state, payload.get());
    std::shared_ptr<typename Result::Callback> callback;
    {
      std::lock_guard lock(result->callbacksMutex);
      const auto it = result->callbacks.find(messageId);
      if (it != result->callbacks.end()) callback = it->second;
    }
    if (!callback || !*callback) {
      metrics_.unknownMessageId(context.streamId(), messageId);
      if (auto* traceSpan = result->span.get()) traceSpan->addEvent("unknown_message_id",
                         {tracing::Attribute::String("message_id", messageId)});
      return;
    }
    if ((*callback)(context, streamContext_, result->state, payload.get())) {
      bool duplicate = false;
      {
        std::lock_guard lock(result->callbacksMutex);
        duplicate = result->callbacks.erase(messageId) == 0;
      }
      if (duplicate) {
        metrics_.duplicateMessageId(context.streamId(), messageId);
        if (auto* traceSpan = 
            result->span.get()) traceSpan->addEvent("duplicate_message_id",
            {tracing::Attribute::String("message_id", messageId)});
      }
    }
    if (auto* traceSpan = result->span.get()) traceSpan->addEvent("result_consumed",
                       {tracing::Attribute::String("message_id", messageId)});
  }

 private:
  void submit(MessageContext context, Payload<Input> payload) {
    if (!acquire()) return;
    // Like Go DataProducer.Consume, return only after this value's lifecycle.
    // The legacy constructor flag is accepted for source compatibility; it
    // must not detach a producer call or change its backpressure semantics.
    struct Release final {
      Endpoint* endpoint;
      ~Release() { endpoint->release(); }
    } release{this};
    process(std::move(context), std::move(payload));
  }

  bool acquire() {
    std::unique_lock lock(concurrencyMutex_);
    for (;;) {
      if (stopped_) return false;
      const auto limit = handler_->concurrency(streamContext_);
      if (limit <= 0 || active_ < static_cast<std::size_t>(limit)) {
        ++active_;
        return true;
      }
      concurrencyCv_.wait(lock);
    }
  }

  void release() noexcept {
    std::lock_guard lock(concurrencyMutex_);
    --active_;
    concurrencyCv_.notify_all();
  }

  void process(MessageContext context, Payload<Input> payload) {
    if (tracingEngineAvailable_) {
      context = ApplyDataSourceEndpointTracing(
          std::move(context), environment_, endpointId_);
    }
    std::shared_ptr<tracing::Tracer> tracer;
    if (auto* tracingEngine =
            tracingEngineAvailable_ ? environment_.getTracing() : nullptr;
        tracingEngine && tracing::SamplingEnabled(context)) {
      tracer = tracingEngine->tracer(environment_.getServiceName());
    }
    tracing::ActiveSpan startedSpan;
    if (tracer) {
      startedSpan = tracing::StartSpanInPlace(
          context, tracer.get(), traceOperation_,
          {tracing::Attribute::String("stream", streamIdentity_.name),
            tracing::Attribute::String("pipeline", streamIdentity_.pipeline),
            tracing::Attribute::String("component", streamIdentity_.component),
           tracing::Attribute::String("endpoint", endpointName_)});
    }
    std::optional<BeginResult<State>> begin;
    try {
      begin.emplace(handler_->beginRequest(context, streamContext_));
    } catch (...) {
      const auto message = tracing::ExceptionMessage(std::current_exception());
      if (auto* traceSpan = startedSpan.span()) tracing::SpanError(traceSpan, message);
      if (auto* traceSpan = startedSpan.span()) traceSpan->addEvent("begin_request.error",
                         {tracing::Attribute::String("error", message)});
      metrics_.beginRequestFailed(message);
      return;
    }
    if (auto* traceSpan = startedSpan.span()) traceSpan->addEvent("begin_request");
    context = std::move(begin->context);
    if (context.streamId().empty()) {
      context = std::move(context).withStreamId(servicelib::http::NewStreamId());
    }
    const std::string streamId{context.streamId()};
    if (startedSpan.span()) {
      tracing::SpanAttrs(
          startedSpan.span(),
          {tracing::Attribute::String("stream_id", streamId),
           tracing::Attribute::Bool("has_result", hasResult_)});
    }
    auto result = std::make_shared<Result>(std::move(begin->state),
                                           startedSpan.sharedSpan());
    const auto startedAt = metrics_.requestStart();
    std::exception_ptr error;
    bool resultWaitFailed = false;
    bool pendingInserted = false;
    try {
      if (hasResult_) {
        pending_.set(streamId, result);
        pendingInserted = true;
        metrics_.pendingAdd(streamId);
      }
      try {
        handler_->consumeMessage(context, streamContext_, result->state,
                                payload.get(),
                                ResultContext<State, T, R, E>{result});
      } catch (...) {
        if (auto* traceSpan = startedSpan.span()) {
          traceSpan->addEvent(
              "consume_message.error",
              {tracing::Attribute::String(
                  "error", tracing::ExceptionMessage(std::current_exception()))});
        }
        throw;
      }
      if (auto* traceSpan = startedSpan.span()) traceSpan->addEvent("consume_message");
      if (hasResult_) {
        try {
          std::stop_callback cancellation{
              context.stopToken(), [result] {
                bool expected = false;
                if (result->wakeSent.compare_exchange_strong(
                        expected, true, std::memory_order_acq_rel)) {
                  result->done.Send();
                }
              }};
          std::stop_callback endpointCancellation{
              requestStopSource_.get_token(), [result] {
                bool expected = false;
                if (result->wakeSent.compare_exchange_strong(
                        expected, true, std::memory_order_acq_rel)) {
                  result->done.Send();
                }
              }};
          if (context.deadline()) {
            if (!result->done.WaitUntil(*context.deadline())) {
              bool expected = false;
              if (!result->wakeSent.compare_exchange_strong(
                      expected, true, std::memory_order_acq_rel)) {
                // A concurrent sender won the wake-up race. Complete its Send
                // before allowing SingleUseEvent to be destroyed.
                result->done.Wait();
              }
              throw std::runtime_error(
                  "custom datasource result wait timeout");
            }
          } else {
            result->done.Wait();
          }
          if (auto* traceSpan = startedSpan.span()) traceSpan->addEvent("done_received");
          if (context.cancelled() || requestStopSource_.stop_requested()) {
            throw std::runtime_error("custom datasource request cancelled");
          }
        } catch (...) {
          resultWaitFailed = true;
          throw;
        }
      }
    } catch (...) {
      error = std::current_exception();
      if (!resultWaitFailed) {
        if (auto* traceSpan = startedSpan.span()) {
          tracing::SpanError(traceSpan, tracing::ExceptionMessage(error));
        }
      }
    }
    std::unique_lock lifetimeLock(result->lifetimeMutex);
    if (resultWaitFailed &&
        result->completed.load(std::memory_order_acquire)) {
      error = nullptr;
      if (auto* traceSpan = startedSpan.span()) traceSpan->addEvent("done_received");
    } else if (resultWaitFailed) {
      if (auto* traceSpan = startedSpan.span()) {
        const auto message = tracing::ExceptionMessage(error);
        tracing::SpanError(traceSpan, message);
        traceSpan->addEvent(
            "context_cancelled",
            {tracing::Attribute::String("error", message)});
      }
    }
    if (pendingInserted) {
      static_cast<void>(pending_.pop(streamId));
      metrics_.pendingRemove(streamId);
    }
    try {
      handler_->endRequest(context, streamContext_, error, result->state);
    } catch (...) {
      // endRequest is noexcept by contract.
    }
    metrics_.requestEnd(startedAt, error);
  }

  static config::CustomEndpointConfig endpointConfig(
      const IServiceEnvironment& environment, int endpointId) {
    const auto runtime = environment.getRuntimeConfigSnapshot();
    const auto value =
        runtime ? runtime->GetEndpointConfigByID(endpointId) : std::nullopt;
    const auto* config =
        value ? value->As<config::CustomEndpointConfig>() : nullptr;
    if (!config)
      throw std::invalid_argument("custom endpoint config not found");
    return *config;
  }

  static config::CustomDataConnectorConfig connectorConfig(
      const IServiceEnvironment& environment, int endpointId) {
    const auto runtime = environment.getRuntimeConfigSnapshot();
    const auto endpoint = endpointConfig(environment, endpointId);
    const auto value = runtime->GetDataConnectorByID(endpoint.idDataConnector);
    const auto* config =
        value ? value->As<config::CustomDataConnectorConfig>() : nullptr;
    if (!config)
      throw std::invalid_argument("custom connector config not found");
    return *config;
  }

  static StreamTraceIdentity resolveStreamIdentity(
      const IServiceEnvironment& environment, int streamConfigId) {
    const auto runtime = environment.getRuntimeConfigSnapshot();
    if (!runtime || streamConfigId == 0) return {};
    const auto stream = runtime->GetStreamConfigByID(streamConfigId);
    return stream ? StreamTraceIdentity{stream->GetName(), stream->GetPipeline(), stream->GetComponent()}
                  : StreamTraceIdentity{};
  }

  IServiceEnvironment& environment_;
  int endpointId_;
  bool tracingEngineAvailable_;
  StreamTraceIdentity streamIdentity_;
  std::string endpointName_;
  DataProducer<Input>& producer_;
  std::optional<Handler> ownedHandler_;
  Handler* handler_;
  StreamContext streamContext_;
  bool hasResult_;
  std::string traceOperation_;
  store::RotatingMap<std::string, std::shared_ptr<Result>> pending_;
  DataSourceEndpointMetrics metrics_;
  std::mutex concurrencyMutex_;
  std::condition_variable concurrencyCv_;
  std::size_t active_{0};
  bool stopped_{true};
  std::atomic<bool> started_{false};
  std::stop_source requestStopSource_;
  std::thread producerThread_;
};

}  // namespace servicelib::datasource::localsource

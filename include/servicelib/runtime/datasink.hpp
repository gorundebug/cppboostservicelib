#pragma once

#include <chrono>
#include <cstddef>
#include <concepts>
#include <exception>
#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

#include <servicelib/runtime/common.hpp>
#include <servicelib/runtime/environment/log/log.hpp>
#include <servicelib/runtime/environment/metrics/metrics.hpp>
#include <servicelib/runtime/payload.hpp>

namespace servicelib {

class IServiceEnvironment;

// Typed, non-owning transport boundary for a sink graph node.  The graph owns
// the concrete sink; data-sink endpoints receive this interface instead of a
// separately repeated environment, endpoint id, stream id and output
// callbacks.
template <typename T, typename R, typename E = std::exception_ptr>
class SinkEndpointStream {
 public:
  using ValueType = T;
  using ResultType = R;
  using ErrorType = E;
  using ResultOutput = std::function<void(MessageContext, Payload<R>)>;
  using ErrorOutput = std::function<void(MessageContext, Payload<E>)>;

  virtual ~SinkEndpointStream() = default;

  [[nodiscard]] virtual IServiceEnvironment& environment() const = 0;
  [[nodiscard]] virtual int endpointId() const noexcept = 0;
  [[nodiscard]] virtual std::size_t streamConfigId() const noexcept = 0;
  virtual void collectResult(MessageContext, Payload<R>) = 0;
  virtual void collectError(MessageContext, Payload<E>) = 0;

  [[nodiscard]] ResultOutput resultOutput() {
    return [this](MessageContext context, Payload<R> result) {
      collectResult(std::move(context), std::move(result));
    };
  }

  [[nodiscard]] ErrorOutput errorOutput() {
    return [this](MessageContext context, Payload<E> error) {
      collectError(std::move(context), std::move(error));
    };
  }
};

// A sink node is owned by the execution graph and becomes available while the
// graph is assembled.  This checked reference models that late binding without
// exposing a nullable raw pointer in generated service code.
template <typename T, typename R, typename E = std::exception_ptr>
class SinkEndpointStreamRef {
 public:
  SinkEndpointStreamRef() noexcept = default;

  SinkEndpointStreamRef& operator=(SinkEndpointStream<T, R, E>& stream) noexcept {
    stream_ = std::ref(stream);
    return *this;
  }

  [[nodiscard]] SinkEndpointStream<T, R, E>& get() const {
    if (!stream_) {
      throw std::logic_error("sink endpoint stream is not initialized");
    }
    return stream_->get();
  }

 private:
  std::optional<std::reference_wrapper<SinkEndpointStream<T, R, E>>> stream_;
};

// Common result/error collector context used by datasink implementations.
// Transport-specific request and response state belongs to
// datasink/<transport>. This is the C++ counterpart of runtime.StreamContext
// in Go: E defaults to std::exception_ptr (the raw transport failure), but a
// sink's own handler may collect a differently-typed error downstream by
// calling collectError with a value of E — mirroring Go's sc.ErrorCollect.
template <typename T, typename R, typename E = std::exception_ptr>
class SinkStreamContext {
 public:
  using ResultOutput = std::function<void(MessageContext, Payload<R>)>;
  using ErrorOutput = std::function<void(MessageContext, Payload<E>)>;

  SinkStreamContext(ResultOutput resultOutput = {},
                    ErrorOutput errorOutput = {})
      : resultOutput_(std::move(resultOutput)),
        errorOutput_(std::move(errorOutput)) {}

  void collect(MessageContext context, Payload<R> payload) const {
    if (resultOutput_) {
      resultOutput_(std::move(context), std::move(payload));
    }
  }

  void collect(MessageContext context, const R& value) const
    requires std::copy_constructible<R>
  {
    collect(std::move(context), Payload<R>::make(value));
  }

  void collect(MessageContext context, R&& value) const
    requires std::move_constructible<R>
  {
    collect(std::move(context), Payload<R>::make(std::move(value)));
  }

  void collectError(MessageContext context, Payload<E> error) const {
    if (errorOutput_) {
      errorOutput_(std::move(context), std::move(error));
    }
  }

  void collectError(MessageContext context, const E& error) const
    requires std::copy_constructible<E>
  {
    collectError(std::move(context), Payload<E>::make(error));
  }

  void collectError(MessageContext context, E&& error) const
    requires std::move_constructible<E>
  {
    collectError(std::move(context), Payload<E>::make(std::move(error)));
  }

 private:
  ResultOutput resultOutput_;
  ErrorOutput errorOutput_;
};

class DataSinkEndpointMetrics final {
 public:
  using Clock = std::chrono::steady_clock;

  DataSinkEndpointMetrics(metrics::Metrics& metrics, log::Logger& logger,
                          std::string connector, std::string endpoint,
                          std::string protocol = {})
      : logger_(logger),
        endpoint_(std::move(endpoint)),
        enabled_(metrics.enabled()),
        scope_(metrics.scope(
            "datasink_endpoint",
            makeLabels(std::move(connector), endpoint_, std::move(protocol)))),
        beginRequestFailed_(scope_->counter(
            "events_total", "Total number of events in data sink endpoint",
            {{"event", "begin_request_failed"}})),
        lateResult_(scope_->counter(
            "events_total", "Total number of events in data sink endpoint",
            {{"event", "late_result"}})),
        requestErrors_(scope_->counter(
            "events_total", "Total number of events in data sink endpoint",
            {{"event", "request_error"}})),
        messagesTotal_(scope_->counter("messages_total",
                                       "Total number of successfully processed "
                                       "messages in data sink endpoint")),
        requestDuration_(scope_->histogram(
            "request_duration_seconds",
            "Request duration in seconds for data sink endpoint")),
        activeRequests_(
            scope_->gauge("active_requests",
                          "Number of active requests in data sink endpoint")) {}

  [[nodiscard]] Clock::time_point requestStart() noexcept {
    if (!enabled_) return {};
    bestEffortTelemetry([this] { activeRequests_->inc(); });
    return Clock::now();
  }

  void requestEnd(Clock::time_point startedAt,
                  const std::exception_ptr& error) noexcept {
    if (!enabled_) return;
    const auto elapsed =
        std::chrono::duration<double>(Clock::now() - startedAt).count();
    bestEffortTelemetry([this] { activeRequests_->dec(); });
    bestEffortTelemetry(
        [this, elapsed] { requestDuration_->observe(elapsed); });
    if (error) {
      bestEffortTelemetry([this] { requestErrors_->inc(); });
    } else {
      bestEffortTelemetry([this] { messagesTotal_->inc(); });
    }
  }

  void beginRequestFailed(std::string_view error = {}) noexcept {
    bestEffortTelemetry([this, error = std::string(error)] {
      logger_.error("BeginRequest failed",
                    {log::Field::Str("endpoint", endpoint_),
                     log::Field::Err(error.empty() ? "<unknown>" : error)});
    });
    bestEffortTelemetry([this] { beginRequestFailed_->inc(); });
  }

  void lateResult(std::string_view streamId = {}) noexcept {
    bestEffortTelemetry([this, streamId = std::string(streamId)] {
      logger_.warn("late result for sink endpoint",
                   {log::Field::Str("endpoint", endpoint_),
                    log::Field::Str("stream_id", streamId)});
    });
    bestEffortTelemetry([this] { lateResult_->inc(); });
  }

  metrics::MetricsScope& scope() noexcept { return *scope_; }

 private:
  static metrics::Labels makeLabels(std::string connector,
                                    const std::string& endpoint,
                                    std::string protocol) {
    metrics::Labels labels{{"connector", std::move(connector)},
                           {"endpoint", endpoint}};
    if (!protocol.empty()) {
      labels.emplace("protocol", std::move(protocol));
    }
    return labels;
  }

  log::Logger& logger_;
  std::string endpoint_;
  bool enabled_{};
  std::unique_ptr<metrics::MetricsScope> scope_;
  std::unique_ptr<metrics::Int64Counter> beginRequestFailed_;
  std::unique_ptr<metrics::Int64Counter> lateResult_;
  std::unique_ptr<metrics::Int64Counter> requestErrors_;
  std::unique_ptr<metrics::Int64Counter> messagesTotal_;
  std::unique_ptr<metrics::Float64Histogram> requestDuration_;
  std::unique_ptr<metrics::Int64Gauge> activeRequests_;
};

}  // namespace servicelib

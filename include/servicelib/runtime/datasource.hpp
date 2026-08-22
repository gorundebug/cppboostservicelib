#pragma once

#include <array>
#include <chrono>
#include <concepts>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>

#include <servicelib/runtime/common.hpp>
#include <servicelib/runtime/environment/log/log.hpp>
#include <servicelib/runtime/environment/metrics/metrics.hpp>
#include <servicelib/runtime/payload.hpp>

namespace servicelib {

inline constexpr std::size_t kPendingRequestShardCount = 64;

// Common typed collector context used by datasource implementations.  This is
// the C++ counterpart of runtime.StreamContext in Go; transport-specific
// state remains in datasource/<transport>.
template <typename T, typename R, typename E = std::exception_ptr>
class SourceStreamContext final {
 public:
  using Output = std::function<void(MessageContext, Payload<T>)>;
  using ErrorOutput = std::function<void(MessageContext, Payload<E>)>;

  SourceStreamContext(Output output, ErrorOutput errorOutput = {})
      : output_(std::move(output)), errorOutput_(std::move(errorOutput)) {
    if (!output_) {
      throw std::invalid_argument("datasource output is empty");
    }
  }

  void collect(MessageContext context, Payload<T> payload) const {
    output_(std::move(context), std::move(payload));
  }

  void collect(MessageContext context, const T& value) const
    requires std::copy_constructible<T>
  {
    collect(std::move(context), Payload<T>::make(value));
  }

  void collect(MessageContext context, T&& value) const
    requires std::move_constructible<T>
  {
    collect(std::move(context), Payload<T>::make(std::move(value)));
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
  Output output_;
  ErrorOutput errorOutput_;
};

class DataSourceEndpointMetrics final {
 public:
  using Clock = std::chrono::steady_clock;

  DataSourceEndpointMetrics(metrics::Metrics& metrics, log::Logger& logger,
                            std::string connector, std::string endpoint,
                            std::string protocol = {})
      : logger_(logger),
        endpoint_(std::move(endpoint)),
        enabled_(metrics.enabled()),
        scope_(metrics.scope(
            "datasource_endpoint",
            makeLabels(std::move(connector), endpoint_, std::move(protocol)))),
        beginRequestFailed_(scope_->counter(
            "events_total", "Total number of events in data source endpoint",
            {{"event", "begin_request_failed"}})),
        missingStreamId_(scope_->counter(
            "events_total", "Total number of events in data source endpoint",
            {{"event", "missing_stream_id"}})),
        lateResult_(scope_->counter(
            "events_total", "Total number of events in data source endpoint",
            {{"event", "late_result"}})),
        unknownMessageId_(scope_->counter(
            "events_total", "Total number of events in data source endpoint",
            {{"event", "unknown_message_id"}})),
        duplicateMessageId_(scope_->counter(
            "events_total", "Total number of events in data source endpoint",
            {{"event", "duplicate_message_id"}})),
        invalidHttpMethod_(scope_->counter(
            "events_total", "Total number of events in data source endpoint",
            {{"event", "invalid_http_method"}})),
        requestErrors_(scope_->counter(
            "events_total", "Total number of events in data source endpoint",
            {{"event", "request_error"}})),
        messagesTotal_(scope_->counter("messages_total",
                                       "Total number of successfully processed "
                                       "messages in data source endpoint")),
        requestDuration_(scope_->histogram(
            "request_duration_seconds",
            "Request duration in seconds for data source endpoint")),
        activeRequests_(
            scope_->gauge("active_requests",
                          "Number of active requests in data source endpoint")),
        pendingRequests_(
            scope_->gauge("pending_requests",
                          "Number of requests awaiting a pipeline result")),
        pendingOldestAge_(scope_->observableFloat64Gauge(
            "pending_oldest_age_seconds",
            "Age in seconds of the oldest pending request awaiting a pipeline "
            "result",
            [this] { return oldestPendingAge(); })) {}

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

  void missingStreamId() noexcept {
    bestEffortTelemetry([this] {
      logger_.error("consumeResult called without streamID",
                    {log::Field::Str("endpoint", endpoint_)});
    });
    bestEffortTelemetry([this] { missingStreamId_->inc(); });
  }

  void lateResult(std::string_view sessionId = {}) noexcept {
    bestEffortTelemetry([this, sessionId = std::string(sessionId)] {
      logger_.warn("consumeResult: session not found in pending",
                   {log::Field::Str("endpoint", endpoint_),
                    log::Field::Str("session_id", sessionId)});
    });
    bestEffortTelemetry([this] { lateResult_->inc(); });
  }

  void unknownMessageId(std::string_view sessionId = {},
                        std::string_view messageId = {}) noexcept {
    bestEffortTelemetry([this, sessionId = std::string(sessionId),
                         messageId = std::string(messageId)] {
      logger_.warn("consumeResult: unknown message ID",
                   {log::Field::Str("endpoint", endpoint_),
                    log::Field::Str("message_id", messageId),
                    log::Field::Str("session_id", sessionId)});
    });
    bestEffortTelemetry([this] { unknownMessageId_->inc(); });
  }

  void duplicateMessageId(std::string_view sessionId = {},
                          std::string_view messageId = {}) noexcept {
    bestEffortTelemetry([this, sessionId = std::string(sessionId),
                         messageId = std::string(messageId)] {
      logger_.warn("consumeResult: duplicate message ID",
                   {log::Field::Str("endpoint", endpoint_),
                    log::Field::Str("message_id", messageId),
                    log::Field::Str("session_id", sessionId)});
    });
    bestEffortTelemetry([this] { duplicateMessageId_->inc(); });
  }

  void invalidHttpMethod(std::string_view method = {},
                         std::string_view path = {}) noexcept {
    bestEffortTelemetry(
        [this, method = std::string(method), path = std::string(path)] {
          logger_.warn("invalid HTTP method",
                       {log::Field::Str("method", method),
                        log::Field::Str("endpoint", endpoint_),
                        log::Field::Str("path", path)});
        });
    bestEffortTelemetry([this] { invalidHttpMethod_->inc(); });
  }

  void pendingAdd(std::string streamId) noexcept {
    if (!enabled_) return;
    {
      auto& shard = pendingShard(streamId);
      std::lock_guard lock(shard.mutex);
      shard.started[std::move(streamId)] = Clock::now();
    }
    bestEffortTelemetry([this] { pendingRequests_->inc(); });
  }

  void pendingRemove(const std::string& streamId) noexcept {
    if (!enabled_) return;
    {
      auto& shard = pendingShard(streamId);
      std::lock_guard lock(shard.mutex);
      shard.started.erase(streamId);
    }
    bestEffortTelemetry([this] { pendingRequests_->dec(); });
  }

  metrics::MetricsScope& scope() noexcept { return *scope_; }

 private:
  struct PendingRequestShard {
    mutable std::mutex mutex;
    std::unordered_map<std::string, Clock::time_point> started;
  };

  [[nodiscard]] PendingRequestShard& pendingShard(
      std::string_view streamId) noexcept {
    return pendingShards_[std::hash<std::string_view>{}(streamId) %
                          pendingShards_.size()];
  }

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

  [[nodiscard]] double oldestPendingAge() const noexcept {
    std::optional<Clock::time_point> oldest;
    for (const auto& shard : pendingShards_) {
      std::lock_guard lock(shard.mutex);
      for (const auto& [_, startedAt] : shard.started) {
        if (!oldest || startedAt < *oldest) {
          oldest = startedAt;
        }
      }
    }
    return oldest
               ? std::chrono::duration<double>(Clock::now() - *oldest).count()
               : 0.0;
  }

  log::Logger& logger_;
  std::string endpoint_;
  bool enabled_{};
  std::unique_ptr<metrics::MetricsScope> scope_;
  std::array<PendingRequestShard, kPendingRequestShardCount> pendingShards_;
  std::unique_ptr<metrics::Int64Counter> beginRequestFailed_;
  std::unique_ptr<metrics::Int64Counter> missingStreamId_;
  std::unique_ptr<metrics::Int64Counter> lateResult_;
  std::unique_ptr<metrics::Int64Counter> unknownMessageId_;
  std::unique_ptr<metrics::Int64Counter> duplicateMessageId_;
  std::unique_ptr<metrics::Int64Counter> invalidHttpMethod_;
  std::unique_ptr<metrics::Int64Counter> requestErrors_;
  std::unique_ptr<metrics::Int64Counter> messagesTotal_;
  std::unique_ptr<metrics::Float64Histogram> requestDuration_;
  std::unique_ptr<metrics::Int64Gauge> activeRequests_;
  std::unique_ptr<metrics::Int64Gauge> pendingRequests_;
  std::unique_ptr<metrics::ObservableFloat64Gauge> pendingOldestAge_;
};

}  // namespace servicelib

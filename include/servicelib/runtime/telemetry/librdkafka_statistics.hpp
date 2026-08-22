#pragma once

#include <algorithm>
#include <cstdint>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>

#include <boost/json.hpp>

#if __has_include(<librdkafka/rdkafka.h>)
#include <librdkafka/rdkafka.h>
#else
#include <rdkafka.h>
#endif

#include <servicelib/runtime/environment/metrics/metrics.hpp>

namespace servicelib::telemetry {

// Adapts librdkafka's documented statistics callback to the framework metrics
// registry. librdkafka statistics remain disabled when application metrics are
// disabled, so this class adds no periodic work to telemetry-free services.
class LibrdkafkaStatistics final {
 public:
  LibrdkafkaStatistics(metrics::Metrics& metrics, std::string role)
      : enabled_(metrics.enabled()) {
    if (!enabled_) return;
    auto scope = metrics.scope("kafka_client", {{"role", std::move(role)}});
    brokers_ = scope->gauge("brokers", "Brokers known to this librdkafka client");
    brokersUp_ = scope->gauge("brokers_up", "Brokers currently connected");
    replyQueueMessages_ = scope->gauge(
        "reply_queue_messages", "Operations waiting in the librdkafka reply queue");
    messagesQueued_ = scope->gauge(
        "messages_queued", "Messages currently queued in librdkafka");
    messageBytesQueued_ = scope->gauge(
        "message_bytes_queued", "Message bytes currently queued in librdkafka");
    requestsSent_ = scope->gauge(
        "requests_sent", "Requests sent since this librdkafka client was created");
    responsesReceived_ = scope->gauge(
        "responses_received",
        "Responses received since this librdkafka client was created");
    bytesSent_ = scope->gauge(
        "bytes_sent", "Bytes sent since this librdkafka client was created");
    bytesReceived_ = scope->gauge(
        "bytes_received", "Bytes received since this librdkafka client was created");
    messagesSent_ = scope->gauge(
        "messages_sent", "Messages sent since this librdkafka client was created");
    messagesReceived_ = scope->gauge(
        "messages_received",
        "Messages received since this librdkafka client was created");
    consumerLag_ = scope->gauge(
        "consumer_lag", "Sum of non-negative lag for assigned partitions");
  }

  void configure(rd_kafka_conf_t* config) {
    if (!enabled_) return;
    char error[512]{};
    if (rd_kafka_conf_set(config, "statistics.interval.ms", "1000", error,
                          sizeof(error)) != RD_KAFKA_CONF_OK) {
      throw std::invalid_argument(std::string{"Kafka statistics config: "} +
                                  error);
    }
    rd_kafka_conf_set_stats_cb(config, &StatisticsCallback);
    rd_kafka_conf_set_opaque(config, this);
  }

 private:
  static std::int64_t integer(const boost::json::object& object,
                              std::string_view name) noexcept {
    const auto* value = object.if_contains(name);
    if (!value) return 0;
    if (value->is_int64()) return value->as_int64();
    if (value->is_uint64()) {
      return static_cast<std::int64_t>(std::min<std::uint64_t>(
          value->as_uint64(),
          static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max())));
    }
    return 0;
  }

  static int StatisticsCallback(rd_kafka_t*, char* json, std::size_t size,
                                void* opaque) noexcept {
    auto* statistics = static_cast<LibrdkafkaStatistics*>(opaque);
    if (!statistics || !json) return 0;
    try {
      statistics->update(boost::json::parse(std::string_view{json, size}).as_object());
    } catch (...) {
      // Metrics callbacks cannot affect transport behavior.
    }
    return 0;
  }

  void update(const boost::json::object& snapshot) noexcept {
    const auto* brokers = snapshot.if_contains("brokers");
    std::int64_t brokersCount{};
    std::int64_t brokersUp{};
    if (brokers && brokers->is_object()) {
      brokersCount = static_cast<std::int64_t>(brokers->as_object().size());
      for (const auto& entry : brokers->as_object()) {
        const auto& value = entry.value();
        if (!value.is_object()) continue;
        const auto* state = value.as_object().if_contains("state");
        if (state && state->is_string() && state->as_string() == "UP") {
          ++brokersUp;
        }
      }
    }

    std::int64_t lag{};
    const auto* topics = snapshot.if_contains("topics");
    if (topics && topics->is_object()) {
      for (const auto& topic : topics->as_object()) {
        const auto& topicValue = topic.value();
        if (!topicValue.is_object()) continue;
        const auto* partitions = topicValue.as_object().if_contains("partitions");
        if (!partitions || !partitions->is_object()) continue;
        for (const auto& partition : partitions->as_object()) {
          const auto& partitionValue = partition.value();
          if (!partitionValue.is_object()) continue;
          const auto value = integer(partitionValue.as_object(), "consumer_lag");
          if (value >= 0 && lag <= std::numeric_limits<std::int64_t>::max() - value) {
            lag += value;
          }
        }
      }
    }

    brokers_->set(brokersCount);
    brokersUp_->set(brokersUp);
    replyQueueMessages_->set(integer(snapshot, "replyq"));
    messagesQueued_->set(integer(snapshot, "msg_cnt"));
    messageBytesQueued_->set(integer(snapshot, "msg_size"));
    requestsSent_->set(integer(snapshot, "tx"));
    responsesReceived_->set(integer(snapshot, "rx"));
    bytesSent_->set(integer(snapshot, "tx_bytes"));
    bytesReceived_->set(integer(snapshot, "rx_bytes"));
    messagesSent_->set(integer(snapshot, "txmsgs"));
    messagesReceived_->set(integer(snapshot, "rxmsgs"));
    consumerLag_->set(lag);
  }

  bool enabled_{};
  std::unique_ptr<metrics::Int64Gauge> brokers_;
  std::unique_ptr<metrics::Int64Gauge> brokersUp_;
  std::unique_ptr<metrics::Int64Gauge> replyQueueMessages_;
  std::unique_ptr<metrics::Int64Gauge> messagesQueued_;
  std::unique_ptr<metrics::Int64Gauge> messageBytesQueued_;
  std::unique_ptr<metrics::Int64Gauge> requestsSent_;
  std::unique_ptr<metrics::Int64Gauge> responsesReceived_;
  std::unique_ptr<metrics::Int64Gauge> bytesSent_;
  std::unique_ptr<metrics::Int64Gauge> bytesReceived_;
  std::unique_ptr<metrics::Int64Gauge> messagesSent_;
  std::unique_ptr<metrics::Int64Gauge> messagesReceived_;
  std::unique_ptr<metrics::Int64Gauge> consumerLag_;
};

}  // namespace servicelib::telemetry

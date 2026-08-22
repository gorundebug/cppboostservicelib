#pragma once

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <exception>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>

#if __has_include(<librdkafka/rdkafka.h>)
#include <librdkafka/rdkafka.h>
#else
#include <rdkafka.h>
#endif

#include <servicelib/datasource/localsource/custom.hpp>
#include <servicelib/runtime/detail/kafka_admin.hpp>
#include <servicelib/runtime/telemetry/librdkafka_statistics.hpp>

namespace servicelib::datasource::kafka {

class ConsumerMessage final {
 public:
  ConsumerMessage(std::string key, std::string value, std::string topic,
                  std::uint32_t partition, std::int64_t offset,
                  std::function<void()> commit = {},
                  std::function<void(std::string)> markMessage = {})
      : key_(std::move(key)),
        value_(std::move(value)),
        topic_(std::move(topic)),
        partition_(partition),
        offset_(offset),
        commit_(std::move(commit)),
        markMessage_(std::move(markMessage)) {}

  [[nodiscard]] const std::string& key() const noexcept { return key_; }
  [[nodiscard]] const std::string& value() const noexcept { return value_; }
  [[nodiscard]] const std::string& topic() const noexcept { return topic_; }
  [[nodiscard]] std::uint32_t partition() const noexcept { return partition_; }
  [[nodiscard]] std::int64_t offset() const noexcept { return offset_; }

  void commit() const {
    if (commit_) commit_();
  }

  void markMessage(std::string metadata = {}) const {
    if (markMessage_) markMessage_(std::move(metadata));
  }

 private:
  std::string key_;
  std::string value_;
  std::string topic_;
  std::uint32_t partition_{};
  std::int64_t offset_{};
  std::function<void()> commit_;
  std::function<void(std::string)> markMessage_;
};

class ConsumerClient {
 public:
  using Callback = std::function<void(ConsumerMessage)>;
  virtual ~ConsumerClient() = default;
  virtual void start(Callback callback) = 0;
  virtual void stop() noexcept = 0;
};

class LibrdkafkaConsumerClient final : public ConsumerClient {
 public:
  LibrdkafkaConsumerClient(IServiceEnvironment& environment, int endpointId)
      : LibrdkafkaConsumerClient(environment, endpointId,
                                resolveIdentity(environment, endpointId)) {}

  ~LibrdkafkaConsumerClient() override {
    stop();
    if (consumer_) rd_kafka_consumer_close(consumer_.get());
  }

  void start(Callback callback) override {
    initialize();
    bool expected = false;
    if (!started_.compare_exchange_strong(expected, true,
                                          std::memory_order_acq_rel)) {
      throw std::logic_error("Kafka consumer already started");
    }
    running_.store(true, std::memory_order_release);
    try {
      while (running_.load(std::memory_order_acquire)) {
        std::unique_ptr<rd_kafka_message_t, MessageDeleter> message{
            rd_kafka_consumer_poll(consumer_.get(), 100)};
        if (!message) continue;
        if (message->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) continue;
        if (message->err != RD_KAFKA_RESP_ERR_NO_ERROR) {
          if (IsTransientError(message->err)) continue;
          throw std::runtime_error(std::string{"Kafka consume: "} +
                                   rd_kafka_err2str(message->err));
        }
        enqueue(
            callback,
            QueuedMessage{CopyBytes(message->key, message->key_len),
                          CopyBytes(message->payload, message->len),
                          rd_kafka_topic_name(message->rkt),
                          static_cast<std::uint32_t>(message->partition),
                          message->offset});
      }
    } catch (...) {
      rememberError(std::current_exception());
    }
    stopLanes();
    if (const auto error = takeError()) std::rethrow_exception(error);
  }

  void stop() noexcept override {
    running_.store(false, std::memory_order_release);
  }

 private:
  struct QueuedMessage final {
    std::string key;
    std::string value;
    std::string topic;
    std::uint32_t partition{};
    std::int64_t offset{};
  };

  struct PartitionLane final {
    std::mutex mutex;
    std::condition_variable changed;
    std::deque<QueuedMessage> messages;
    bool stopped{};
    std::thread worker;
  };

  struct EndpointIdentity final {
    int connectorId{};
    std::string topic;
  };

  LibrdkafkaConsumerClient(IServiceEnvironment& environment, int endpointId,
                           EndpointIdentity identity)
      : environment_(environment),
        endpointId_(endpointId),
        connectorId_(identity.connectorId),
        topic_(std::move(identity.topic)),
        statistics_(environment.getMetrics(), "consumer") {}

  [[nodiscard]] static EndpointIdentity resolveIdentity(
      const IServiceEnvironment& environment, int endpointId) {
    const auto runtime = environment.getRuntimeConfigSnapshot();
    const auto endpointRef =
        runtime ? runtime->GetEndpointConfigByID(endpointId) : std::nullopt;
    const auto* endpoint = endpointRef
                               ? endpointRef->As<config::KafkaEndpointConfig>()
                               : nullptr;
    if (!endpoint || endpoint->topic.empty()) {
      throw std::invalid_argument("Kafka consumer topic is empty");
    }
    return {endpoint->idDataConnector, endpoint->topic};
  }

  void initialize() {
    if (consumer_) return;
    const auto runtime = environment_.getRuntimeConfigSnapshot();
    const auto endpointRef =
        runtime ? runtime->GetEndpointConfigByID(endpointId_) : std::nullopt;
    const auto* endpoint = endpointRef
                               ? endpointRef->As<config::KafkaEndpointConfig>()
                               : nullptr;
    if (!endpoint || endpoint->topic.empty() ||
        endpoint->consumerGroup.empty()) {
      throw std::invalid_argument(
          "Kafka consumer endpoint config is missing topic or group");
    }
    const auto connectorRef =
        runtime ? runtime->GetDataConnectorByID(connectorId_) : std::nullopt;
    const auto* connector =
        connectorRef
            ? connectorRef->As<config::KafkaDataConnectorConfig>()
            : nullptr;
    if (!connector || connector->brokers.empty()) {
      throw std::invalid_argument("Kafka consumer brokers are empty");
    }
    auto* kafkaConfig = rd_kafka_conf_new();
    try {
      SetConfig(kafkaConfig, "bootstrap.servers", connector->brokers);
      detail::ApplyKafkaSecurity(kafkaConfig, *connector);
      SetConfig(kafkaConfig, "group.id", endpoint->consumerGroup);
      SetConfig(kafkaConfig, "enable.auto.commit", "true");
      SetConfig(kafkaConfig, "enable.auto.offset.store", "false");
      SetConfig(kafkaConfig, "auto.offset.reset", "earliest");
      if (connector->dialTimeout > 0) {
        SetConfig(kafkaConfig, "socket.timeout.ms",
                  std::to_string(static_cast<std::int64_t>(
                      connector->dialTimeout)));
      }
      if (!connector->version.empty()) {
        SetConfig(kafkaConfig, "broker.version.fallback", connector->version);
        SetConfig(kafkaConfig, "api.version.request", "false");
      }
      statistics_.configure(kafkaConfig);
      char error[512]{};
      consumer_.reset(rd_kafka_new(RD_KAFKA_CONSUMER, kafkaConfig, error,
                                   sizeof(error)));
      if (!consumer_) {
        kafkaConfig = nullptr;
        throw std::runtime_error(std::string{"Kafka consumer: "} + error);
      }
      kafkaConfig = nullptr;
      const auto status = rd_kafka_poll_set_consumer(consumer_.get());
      if (status != RD_KAFKA_RESP_ERR_NO_ERROR) {
        throw std::runtime_error(std::string{"Kafka consumer poll: "} +
                                 rd_kafka_err2str(status));
      }
      auto* topics = rd_kafka_topic_partition_list_new(1);
      rd_kafka_topic_partition_list_add(topics, topic_.c_str(),
                                        RD_KAFKA_PARTITION_UA);
      const auto subscribe = rd_kafka_subscribe(consumer_.get(), topics);
      rd_kafka_topic_partition_list_destroy(topics);
      if (subscribe != RD_KAFKA_RESP_ERR_NO_ERROR) {
        throw std::runtime_error(std::string{"Kafka subscribe: "} +
                                 rd_kafka_err2str(subscribe));
      }
    } catch (...) {
      if (kafkaConfig) rd_kafka_conf_destroy(kafkaConfig);
      consumer_.reset();
      throw;
    }
  }

  void enqueue(const Callback& callback, QueuedMessage message) {
    const auto partition = message.partition;
    auto [it, inserted] = lanes_.try_emplace(
        partition, std::make_unique<PartitionLane>());
    auto& lane = *it->second;
    if (inserted) {
      lane.worker = std::thread([this, &lane, callback] {
        runLane(lane, callback);
      });
    }
    {
      std::lock_guard lock(lane.mutex);
      lane.messages.push_back(std::move(message));
    }
    lane.changed.notify_one();
  }

  void runLane(PartitionLane& lane, const Callback& callback) noexcept {
    try {
      for (;;) {
        std::optional<QueuedMessage> message;
        {
          std::unique_lock lock(lane.mutex);
          lane.changed.wait(lock,
                            [&lane] {
                              return lane.stopped || !lane.messages.empty();
                            });
          if (lane.stopped) return;
          message.emplace(std::move(lane.messages.front()));
          lane.messages.pop_front();
        }
        dispatch(callback, std::move(*message));
        if (!running_.load(std::memory_order_acquire)) return;
      }
    } catch (...) {
      rememberError(std::current_exception());
      running_.store(false, std::memory_order_release);
    }
  }

  void dispatch(const Callback& callback, QueuedMessage message) {
    const auto topic = message.topic;
    const auto partition = message.partition;
    const auto offset = message.offset;
    callback(ConsumerMessage{
        std::move(message.key), std::move(message.value),
        std::move(message.topic), partition, offset,
        [this, topic, partition, offset] {
          commitOffset(topic, partition, offset);
        },
        [this, topic, partition, offset](std::string) {
          storeOffset(topic, partition, offset);
        }});
  }

  void stopLanes() noexcept {
    for (auto& [partition, lane] : lanes_) {
      static_cast<void>(partition);
      {
        std::lock_guard lock(lane->mutex);
        lane->stopped = true;
        lane->messages.clear();
      }
      lane->changed.notify_all();
    }
    for (auto& [partition, lane] : lanes_) {
      static_cast<void>(partition);
      if (lane->worker.joinable()) lane->worker.join();
    }
    lanes_.clear();
  }

  void storeOffset(const std::string& topic, std::uint32_t partition,
                   std::int64_t offset) {
    auto* offsets = rd_kafka_topic_partition_list_new(1);
    auto* entry = rd_kafka_topic_partition_list_add(
        offsets, topic.c_str(), static_cast<std::int32_t>(partition));
    entry->offset = offset + 1;
    const auto status = rd_kafka_offsets_store(consumer_.get(), offsets);
    rd_kafka_topic_partition_list_destroy(offsets);
    if (status != RD_KAFKA_RESP_ERR_NO_ERROR) {
      throw std::runtime_error(std::string{"Kafka mark message: "} +
                               rd_kafka_err2str(status));
    }
  }

  void commitOffset(const std::string& topic, std::uint32_t partition,
                    std::int64_t offset) {
    auto* offsets = rd_kafka_topic_partition_list_new(1);
    auto* entry = rd_kafka_topic_partition_list_add(
        offsets, topic.c_str(), static_cast<std::int32_t>(partition));
    entry->offset = offset + 1;
    const auto status = rd_kafka_commit(consumer_.get(), offsets, 0);
    rd_kafka_topic_partition_list_destroy(offsets);
    if (status != RD_KAFKA_RESP_ERR_NO_ERROR) {
      throw std::runtime_error(std::string{"Kafka commit: "} +
                               rd_kafka_err2str(status));
    }
  }

  void rememberError(std::exception_ptr error) noexcept {
    std::lock_guard lock(errorMutex_);
    if (!error_) error_ = std::move(error);
  }

  std::exception_ptr takeError() noexcept {
    std::lock_guard lock(errorMutex_);
    return std::exchange(error_, {});
  }

  struct KafkaDeleter final {
    void operator()(rd_kafka_t* value) const noexcept {
      if (value) rd_kafka_destroy(value);
    }
  };

  struct MessageDeleter final {
    void operator()(rd_kafka_message_t* value) const noexcept {
      if (value) rd_kafka_message_destroy(value);
    }
  };

  static void SetConfig(rd_kafka_conf_t* config, const char* name,
                        const std::string& value) {
    char error[512]{};
    if (rd_kafka_conf_set(config, name, value.c_str(), error, sizeof(error)) !=
        RD_KAFKA_CONF_OK) {
      throw std::invalid_argument(std::string{"Kafka config "} + name +
                                  ": " + error);
    }
  }

  static std::string CopyBytes(const void* data, std::size_t size) {
    if (!data || size == 0) return {};
    return {static_cast<const char*>(data), size};
  }

  static bool IsTransientError(rd_kafka_resp_err_t error) noexcept {
    switch (error) {
      case RD_KAFKA_RESP_ERR__TRANSPORT:
      case RD_KAFKA_RESP_ERR__RESOLVE:
      case RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC:
      case RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN:
      case RD_KAFKA_RESP_ERR__TIMED_OUT:
      case RD_KAFKA_RESP_ERR__WAIT_COORD:
      case RD_KAFKA_RESP_ERR__IN_PROGRESS:
      case RD_KAFKA_RESP_ERR__TIMED_OUT_QUEUE:
      case RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT:
      case RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART:
      case RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE:
      case RD_KAFKA_RESP_ERR_NETWORK_EXCEPTION:
      case RD_KAFKA_RESP_ERR_COORDINATOR_LOAD_IN_PROGRESS:
      case RD_KAFKA_RESP_ERR_NOT_COORDINATOR:
        return true;
      default:
        return false;
    }
  }

  IServiceEnvironment& environment_;
  int endpointId_;
  int connectorId_;
  std::string topic_;
  telemetry::LibrdkafkaStatistics statistics_;
  std::unique_ptr<rd_kafka_t, KafkaDeleter> consumer_;
  std::map<std::uint32_t, std::unique_ptr<PartitionLane>> lanes_;
  std::mutex errorMutex_;
  std::exception_ptr error_;
  std::atomic<bool> started_{false};
  std::atomic<bool> running_{false};
};

namespace detail {

class ProducerAdapter final
    : public datasource::localsource::DataProducer<ConsumerMessage> {
 public:
  explicit ProducerAdapter(ConsumerClient& client) : client_(client) {}

  void start(Context, Consumer consumer) override {
    client_.start([consumer = std::move(consumer)](ConsumerMessage message) {
      consumer(MessageContext{},
               Payload<ConsumerMessage>::make(std::move(message)));
    });
  }
  void stop(Context) override { client_.stop(); }

 private:
  ConsumerClient& client_;
};

template <typename Handler, typename T, typename R, typename E>
class HandlerAdapter final {
 public:
  using State = typename Handler::State;
  explicit HandlerAdapter(Handler handler) : handler_(std::move(handler)) {}

  int concurrency(SourceStreamContext<T, R, E>& context) {
    return handler_.concurrency(context);
  }
  BeginResult<State> beginRequest(MessageContext context,
                                  SourceStreamContext<T, R, E>& stream) {
    return handler_.beginRequest(std::move(context), stream);
  }
  void consumeMessage(
      MessageContext context, SourceStreamContext<T, R, E>& stream,
      State& state, const ConsumerMessage& message,
      datasource::localsource::ResultContext<State, T, R, E> result) {
    handler_.consumeMessage(std::move(context), stream, state, message,
                            std::move(result));
  }
  std::string getMessageId(MessageContext context,
                           SourceStreamContext<T, R, E>& stream, State& state,
                           const R& value) {
    return handler_.getMessageId(std::move(context), stream, state, value);
  }
  void endRequest(MessageContext context, SourceStreamContext<T, R, E>& stream,
                  std::exception_ptr error, State& state) noexcept {
    handler_.endRequest(std::move(context), stream, error, state);
  }

 private:
  Handler handler_;
};

}  // namespace detail

template <typename T, typename R, typename Handler,
          typename E = std::exception_ptr>
class Endpoint final {
 public:
  using Adapter = detail::HandlerAdapter<Handler, T, R, E>;
  using Implementation =
      datasource::localsource::Endpoint<T, R, Adapter, E, ConsumerMessage>;
  using Output = typename SourceStreamContext<T, R, E>::Output;
  using ErrorOutput = typename SourceStreamContext<T, R, E>::ErrorOutput;

  template <typename InputStreamType>
  static std::shared_ptr<Endpoint> make(
      IServiceEnvironment& environment,
      InputStreamType& input, ConsumerClient& consumer,
      Handler handler) {
    auto endpoint = std::shared_ptr<Endpoint>(new Endpoint(
        environment, input.getEndpointId(),
        static_cast<int>(input.getConfigId()), consumer, std::move(handler),
        [input = &input](MessageContext context, Payload<T> value) {
          input->consume(std::move(context), std::move(value));
        },
        input.getResultStream() != nullptr,
        [input = &input](MessageContext context, Payload<E> error) {
          input->consumeError(std::move(context), std::move(error));
        }));
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

 private:
  Endpoint(IServiceEnvironment& environment, int endpointId,
           int streamConfigId, ConsumerClient& consumer, Handler handler,
           Output output, bool hasResult, ErrorOutput errorOutput = {})
      : producer_(consumer),
        environment_(environment),
        endpointId_(endpointId),
        implementation_(
            environment, endpointId, streamConfigId, producer_,
            Adapter{std::move(handler)}, std::move(output), hasResult,
            connectorConfig(environment, endpointId).name,
            endpointConfig(environment, endpointId).name,
            std::move(errorOutput), true, "kafka.input") {}

 public:
  void start(Context context) {
    const auto endpoint = endpointConfig(environment_, endpointId_);
    if (!endpoint.enabled) return;
    servicelib::detail::EnsureKafkaTopic(
        connectorConfig(environment_, endpointId_), endpoint);
    implementation_.start(std::move(context));
    started_ = true;
  }
  void stop(Context context) {
    if (!started_) return;
    started_ = false;
    implementation_.stop(std::move(context));
  }
  void consumeResult(MessageContext context, Payload<R> payload) {
    implementation_.consumeResult(std::move(context), std::move(payload));
  }

 private:
  static config::KafkaEndpointConfig endpointConfig(
      const IServiceEnvironment& environment, int endpointId) {
    const auto runtime = environment.getRuntimeConfigSnapshot();
    const auto value =
        runtime ? runtime->GetEndpointConfigByID(endpointId) : std::nullopt;
    const auto* result =
        value ? value->As<config::KafkaEndpointConfig>() : nullptr;
    if (!result || result->topic.empty()) {
      throw std::invalid_argument(
          "Kafka endpoint config not found or topic empty");
    }
    return *result;
  }

  static config::KafkaDataConnectorConfig connectorConfig(
      const IServiceEnvironment& environment, int endpointId) {
    const auto runtime = environment.getRuntimeConfigSnapshot();
    const auto endpoint = endpointConfig(environment, endpointId);
    const auto value = runtime->GetDataConnectorByID(endpoint.idDataConnector);
    const auto* result =
        value ? value->As<config::KafkaDataConnectorConfig>() : nullptr;
    if (!result)
      throw std::invalid_argument("Kafka connector config not found");
    return *result;
  }

  detail::ProducerAdapter producer_;
  IServiceEnvironment& environment_;
  int endpointId_{};
  bool started_{};
  Implementation implementation_;
};

}  // namespace servicelib::datasource::kafka

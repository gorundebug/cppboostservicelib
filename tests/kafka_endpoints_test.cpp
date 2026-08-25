#include <gtest/gtest.h>

#include <servicelib/datasink/kafka/librdkafka.hpp>
#include <servicelib/datasource/kafka/librdkafka.hpp>
#include <servicelib/runtime/environment/environment.hpp>
#include <servicelib/runtime/testlog/testlog.hpp>
#include <servicelib/runtime/testmetrics/testmetrics.hpp>

#include "test_sink_endpoint_stream.hpp"

#include "test_async.hpp"

#if __has_include(<librdkafka/rdkafka_mock.h>)
#include <librdkafka/rdkafka_mock.h>
#else
#include <rdkafka_mock.h>
#endif

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <exception>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace {

class MockKafkaCluster final {
 public:
  MockKafkaCluster() {
    char error[512]{};
    handle_ = rd_kafka_new(RD_KAFKA_PRODUCER, rd_kafka_conf_new(), error,
                           sizeof(error));
    if (!handle_) throw std::runtime_error(error);
    cluster_ = rd_kafka_mock_cluster_new(handle_, 1);
    if (!cluster_) throw std::runtime_error("failed to create mock cluster");
  }

  ~MockKafkaCluster() {
    if (cluster_) rd_kafka_mock_cluster_destroy(cluster_);
    if (handle_) rd_kafka_destroy(handle_);
  }

  [[nodiscard]] std::string brokers() const {
    return rd_kafka_mock_cluster_bootstraps(cluster_);
  }

  void createTopic(const std::string& topic, int partitions = 1) {
    const auto status = rd_kafka_mock_topic_create(
        cluster_, topic.c_str(), partitions, 1);
    if (status != RD_KAFKA_RESP_ERR_NO_ERROR) {
      throw std::runtime_error(rd_kafka_err2str(status));
    }
  }

  void setGroupCoordinator(const std::string& group) {
    const auto status = rd_kafka_mock_coordinator_set(
        cluster_, "group", group.c_str(), 1);
    if (status != RD_KAFKA_RESP_ERR_NO_ERROR) {
      throw std::runtime_error(rd_kafka_err2str(status));
    }
  }

  void setBrokerDown() {
    const auto status = rd_kafka_mock_broker_set_down(cluster_, 1);
    if (status != RD_KAFKA_RESP_ERR_NO_ERROR) {
      throw std::runtime_error(rd_kafka_err2str(status));
    }
  }

  void setBrokerUp() {
    const auto status = rd_kafka_mock_broker_set_up(cluster_, 1);
    if (status != RD_KAFKA_RESP_ERR_NO_ERROR) {
      throw std::runtime_error(rd_kafka_err2str(status));
    }
  }

 private:
  rd_kafka_t* handle_{};
  rd_kafka_mock_cluster_t* cluster_{};
};

class TestConfig final : public servicelib::config::IConfig {
 public:
  TestConfig() {
    kafkaEndpoint.id = 3;
    kafkaEndpoint.name = "kafka-messages";
    kafkaEndpoint.idDataConnector = 4;
    kafkaEndpoint.enabled = true;
    kafkaEndpoint.topic = "events";
    kafkaEndpoint.partitions = 4;
    kafkaEndpoint.consumerGroup = "tests";
    kafkaConnector.id = 4;
    kafkaConnector.name = "kafka";
    kafkaConnector.brokers = "localhost:9092";
  }

  std::vector<const servicelib::config::ServiceConfig*> GetServices()
      const override {
    return {};
  }
  std::vector<servicelib::config::StreamConfigRef> GetStreams() const override {
    return {};
  }
  std::vector<servicelib::config::DataConnectorConfigRef> GetDataConnectors()
      const override {
    return {kafkaConnector};
  }
  std::vector<servicelib::config::EndpointConfigRef> GetEndpoints()
      const override {
    return {kafkaEndpoint};
  }
  std::vector<const servicelib::config::PoolConfig*> GetPools() const override {
    return {};
  }
  std::vector<const servicelib::config::LinkConfig*> GetLinks() const override {
    return {};
  }
  std::vector<const servicelib::config::ModuleConfig*> GetModules()
      const override {
    return {};
  }
  std::vector<const servicelib::config::TypeConfig*> GetTypes() const override {
    return {};
  }

  servicelib::config::KafkaEndpointConfig kafkaEndpoint;
  servicelib::config::KafkaDataConnectorConfig kafkaConnector;
};

class TestEnvironment final : public servicelib::IRuntimeEnvironment {
 public:
  TestEnvironment() : runtimeConfig_(config_) {
    service_.name = "endpoint-test";
  }
  servicelib::pool::ITaskPool* getTaskPool(const std::string&) override {
    return nullptr;
  }
  servicelib::pool::IPriorityTaskPool* getPriorityTaskPool(
      const std::string&) override {
    return nullptr;
  }
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
  servicelib::tracing::Tracing* getTracing() override { return nullptr; }
  TestConfig& config() noexcept { return config_; }

 private:
  TestConfig config_;
  servicelib::config::RuntimeConfig runtimeConfig_;
  servicelib::config::ServiceConfig service_;
  servicelib::testlog::TestLog log_;
  servicelib::testmetrics::TestMetrics metrics_;
};

class FakeKafkaProducer final
    : public servicelib::datasink::kafka::ProducerClient {
 public:
  void start(const servicelib::config::KafkaDataConnectorConfig&) override {
    ++startCount;
  }
  void stop() noexcept override { ++stopCount; }
  [[nodiscard]] std::optional<std::uint32_t> partitionCount(
      const std::string&) const override {
    return actualPartitionCount;
  }
  servicelib::datasink::kafka::DeliveryResult send(
      std::string topic, std::string key, std::string value,
      std::optional<std::uint32_t> partition) override {
    observed = topic + ":" + key + ":" + value;
    return {partition, 17, {}};
  }
  servicelib::datasink::kafka::DeliveryResult sendWithHeaders(
      std::string topic, std::string key, std::string value,
      std::optional<std::uint32_t> partition,
      const servicelib::detail::KafkaHeaders& headers) override {
    observedHeaders = headers;
    return send(std::move(topic), std::move(key), std::move(value), partition);
  }
  std::optional<std::uint32_t> actualPartitionCount;
  std::string observed;
  servicelib::detail::KafkaHeaders observedHeaders;
  int startCount{};
  int stopCount{};
};

struct KafkaSinkHandler final {
  using State = int;
  std::uint32_t expectedPartitionCount{4};
  std::string getStreamId(servicelib::MessageContext, const std::string&) {
    return "kafka-sid";
  }
  servicelib::BeginResult<State> beginRequest(
      servicelib::MessageContext context, auto&) {
    return {std::move(context), 0};
  }
  std::optional<std::uint32_t> partition(const std::string&,
                                         std::uint32_t partitionCount) {
    EXPECT_EQ(partitionCount, expectedPartitionCount);
    return partitionCount - 1;
  }
  void consumeMessage(servicelib::MessageContext, auto&, State&,
                      const std::string& value, auto& message) {
    message.key = "key";
    message.value = value;
    const auto delivery = message.sendSync();
    if (delivery.error) std::rethrow_exception(delivery.error);
    message.out(static_cast<int>(*delivery.offset));
  }
  void endRequest(servicelib::MessageContext, auto&, std::exception_ptr error,
                  State&) noexcept {
    EXPECT_FALSE(error);
  }
};

TEST(KafkaDataSink, SendsThroughAdapterAndCollectsDeliveryResult) {
  TestEnvironment environment;
  FakeKafkaProducer producer;
  producer.actualPartitionCount = 6;
  int result = 0;
  TestSinkEndpointStream<std::string, int> stream{
      environment, 3,
      [&](servicelib::MessageContext, servicelib::Payload<int> value) {
        result = value.get();
      }};
  servicelib::datasink::kafka::Endpoint<std::string, int, KafkaSinkHandler>
      endpoint{stream, producer, KafkaSinkHandler{6}};
  endpoint.start(servicelib::Context{});
  servicelib::tracing::SpanContext trace{
      "0123456789abcdef0123456789abcdef", "0123456789abcdef", true,
      "vendor=value", "tenant=test"};
  endpoint.consume(servicelib::MessageContext{}
                       .withStreamId("incoming-stream")
                       .withSampling(true)
                       .withTrace(std::move(trace)),
                   servicelib::Payload<std::string>::make("payload"));
  EXPECT_EQ(producer.observed, "events:key:payload");
  EXPECT_EQ(producer.observedHeaders.at("x-stream-id"), "kafka-sid");
  EXPECT_EQ(producer.observedHeaders.at("traceparent"),
            "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01");
  EXPECT_EQ(producer.observedHeaders.at("tracestate"), "vendor=value");
  EXPECT_EQ(producer.observedHeaders.at("baggage"), "tenant=test");
  EXPECT_EQ(producer.observedHeaders.at("x-trace"), "1");
  EXPECT_EQ(result, 17);
  endpoint.stop(servicelib::Context{});
  EXPECT_EQ(producer.startCount, 1);
  EXPECT_EQ(producer.stopCount, 1);
}

TEST(KafkaContext, RestoresCanonicalPropagationHeaders) {
  const servicelib::detail::KafkaHeaders headers{
      {"x-stream-id", "stream-42"},
      {"traceparent",
       "00-0123456789abcdef0123456789abcdef-0123456789abcdef-00"},
      {"tracestate", "vendor=value"},
      {"baggage", "tenant=test"}};
  const auto context = servicelib::detail::ContextFromKafkaHeaders(headers);
  EXPECT_EQ(context.streamId(), "stream-42");
  EXPECT_FALSE(context.samplingEnabled());
  ASSERT_TRUE(context.trace().isValid());
  EXPECT_EQ(context.trace().traceId, "0123456789abcdef0123456789abcdef");
  EXPECT_EQ(context.trace().spanId, "0123456789abcdef");
  EXPECT_EQ(context.trace().traceState, "vendor=value");
  EXPECT_EQ(context.trace().baggage, "tenant=test");
}

TEST(KafkaDataSink, DisabledEndpointDoesNotStartProducer) {
  TestEnvironment environment;
  environment.config().kafkaEndpoint.enabled = false;
  FakeKafkaProducer producer;
  TestSinkEndpointStream<std::string, int> stream{
      environment, 3,
      [](servicelib::MessageContext, servicelib::Payload<int>) {}};
  servicelib::datasink::kafka::Endpoint<std::string, int, KafkaSinkHandler>
      endpoint{stream, producer, KafkaSinkHandler{4}};

  endpoint.start(servicelib::Context{});
  endpoint.consume(servicelib::MessageContext{},
                   servicelib::Payload<std::string>::make("payload"));
  endpoint.stop(servicelib::Context{});

  EXPECT_EQ(producer.startCount, 0);
  EXPECT_EQ(producer.stopCount, 0);
  EXPECT_TRUE(producer.observed.empty());
}

struct SkippingKafkaSinkHandler final {
  using State = int;
  bool* partitionCalled;
  std::string getStreamId(servicelib::MessageContext, const std::string&) {
    return "skip-kafka-sid";
  }
  servicelib::BeginResult<State> beginRequest(
      servicelib::MessageContext context, auto&) {
    return {std::move(context), 0};
  }
  std::optional<std::uint32_t> partition(const std::string&,
                                         std::uint32_t) {
    *partitionCalled = true;
    return 0;
  }
  void consumeMessage(servicelib::MessageContext, auto&, State&,
                      const std::string&, auto& message) {
    message.skip(42);
  }
  void endRequest(servicelib::MessageContext, auto&, std::exception_ptr error,
                  State&) noexcept {
    EXPECT_FALSE(error);
  }
};

TEST(KafkaDataSink, DoesNotSelectPartitionForSkippedMessage) {
  TestEnvironment environment;
  FakeKafkaProducer producer;
  bool partitionCalled = false;
  int result = 0;
  TestSinkEndpointStream<std::string, int> stream{
      environment, 3,
      [&](servicelib::MessageContext, servicelib::Payload<int> value) {
        result = value.get();
      }};
  servicelib::datasink::kafka::Endpoint<std::string, int,
                                        SkippingKafkaSinkHandler>
      endpoint{stream, producer, SkippingKafkaSinkHandler{&partitionCalled}};
  endpoint.start(servicelib::Context{});
  endpoint.consume(servicelib::MessageContext{},
                   servicelib::Payload<std::string>::make("payload"));
  EXPECT_FALSE(partitionCalled);
  EXPECT_EQ(result, 42);
  EXPECT_TRUE(producer.observed.empty());
  endpoint.stop(servicelib::Context{});
}

struct ThrowingPartitionKafkaSinkHandler final {
  using State = int;
  std::string getStreamId(servicelib::MessageContext, const std::string&) {
    return "partition-error-kafka-sid";
  }
  servicelib::BeginResult<State> beginRequest(
      servicelib::MessageContext context, auto&) {
    return {std::move(context), 0};
  }
  std::optional<std::uint32_t> partition(const std::string&,
                                         std::uint32_t) {
    throw std::runtime_error("partition failed");
  }
  void consumeMessage(servicelib::MessageContext, auto&, State&,
                      const std::string& value, auto& message) {
    message.key = "key";
    message.value = value;
    const auto delivery = message.sendSync();
    message.out(delivery.error ? 43 : -1);
  }
  void endRequest(servicelib::MessageContext, auto&, std::exception_ptr error,
                  State&) noexcept {
    EXPECT_FALSE(error);
  }
};

TEST(KafkaDataSink, PassesPartitionFailureAsDeliveryError) {
  TestEnvironment environment;
  FakeKafkaProducer producer;
  int result = 0;
  TestSinkEndpointStream<std::string, int> stream{
      environment, 3,
      [&](servicelib::MessageContext, servicelib::Payload<int> value) {
        result = value.get();
      }};
  servicelib::datasink::kafka::Endpoint<std::string, int,
                                        ThrowingPartitionKafkaSinkHandler>
      endpoint{stream, producer, ThrowingPartitionKafkaSinkHandler{}};
  endpoint.start(servicelib::Context{});
  endpoint.consume(servicelib::MessageContext{},
                   servicelib::Payload<std::string>::make("payload"));
  EXPECT_EQ(result, 43);
  EXPECT_TRUE(producer.observed.empty());
  endpoint.stop(servicelib::Context{});
}

struct AsyncKafkaSinkHandler final {
  using State = int;
  std::thread::id callerThread;
  std::atomic<bool>* callbackOnAsio;
  std::string getStreamId(servicelib::MessageContext, const std::string&) {
    return "async-kafka-sid";
  }
  servicelib::BeginResult<State> beginRequest(
      servicelib::MessageContext context, auto&) {
    return {std::move(context), 0};
  }
  void consumeMessage(servicelib::MessageContext, auto&, State&,
                      const std::string& value, auto& message) {
    message.key = "async-key";
    message.value = value;
    message.send([this](const servicelib::datasink::kafka::DeliveryResult& result) {
      callbackOnAsio->store(std::this_thread::get_id() != callerThread,
                            std::memory_order_release);
      return static_cast<int>(result.offset.value_or(-1));
    });
  }
  void endRequest(servicelib::MessageContext, auto&, std::exception_ptr error,
                  State&) noexcept {
    EXPECT_FALSE(error);
  }
};

TEST(KafkaDataSink, MarshalsAsyncDeliveryThroughAsioExecutor) {
  test_async::AsioRuntime runtime;
  TestEnvironment environment;
  FakeKafkaProducer producer;
  test_async::Event delivered;
  std::atomic<bool> callbackOnAsio{false};
  const auto callerThread = std::this_thread::get_id();
  TestSinkEndpointStream<std::string, int> stream{
      environment, 3,
      [&](servicelib::MessageContext, servicelib::Payload<int> value) {
        EXPECT_EQ(value.get(), 17);
        delivered.Send();
      }};
  servicelib::datasink::kafka::Endpoint<std::string, int,
                                        AsyncKafkaSinkHandler>
      endpoint{stream, producer,
               AsyncKafkaSinkHandler{callerThread, &callbackOnAsio}};
  endpoint.start(servicelib::Context{});
  endpoint.consume(servicelib::MessageContext{},
                   servicelib::Payload<std::string>::make("payload"));
  ASSERT_TRUE(delivered.WaitForEvent());
  endpoint.stop(servicelib::Context{});
  EXPECT_TRUE(callbackOnAsio.load(std::memory_order_acquire));
}

class FailingKafkaProducer final
    : public servicelib::datasink::kafka::ProducerClient {
 public:
  servicelib::datasink::kafka::DeliveryResult send(
      std::string, std::string, std::string,
      std::optional<std::uint32_t> partition) override {
    return {partition, std::nullopt,
            std::make_exception_ptr(
                std::runtime_error("Kafka delivery: broker unavailable"))};
  }
};

struct FailingKafkaSinkHandler final {
  using State = int;
  bool* endedWithError;
  std::string getStreamId(servicelib::MessageContext, const std::string&) {
    return "failed-kafka-sid";
  }
  servicelib::BeginResult<State> beginRequest(
      servicelib::MessageContext context, auto&) {
    return {std::move(context), 0};
  }
  void consumeMessage(servicelib::MessageContext, auto&, State&,
                      const std::string& value, auto& message) {
    message.key = "failed-key";
    message.value = value;
    const auto delivery = message.sendSync();
    if (delivery.error) std::rethrow_exception(delivery.error);
  }
  void endRequest(servicelib::MessageContext, auto&, std::exception_ptr error,
                  State&) noexcept {
    *endedWithError = static_cast<bool>(error);
  }
};

TEST(KafkaDataSink, MapsDeliveryFailureToErrorOutputAndEndRequest) {
  TestEnvironment environment;
  FailingKafkaProducer producer;
  bool endedWithError = false;
  std::exception_ptr observed;
  TestSinkEndpointStream<std::string, int> stream{
      environment, 3, {},
      [&](servicelib::MessageContext,
          servicelib::Payload<std::exception_ptr> error) {
        observed = error.get();
      }};
  servicelib::datasink::kafka::Endpoint<std::string, int,
                                        FailingKafkaSinkHandler>
      endpoint{stream, producer, FailingKafkaSinkHandler{&endedWithError}};
  endpoint.start(servicelib::Context{});
  endpoint.consume(servicelib::MessageContext{},
                   servicelib::Payload<std::string>::make("payload"));
  ASSERT_TRUE(observed);
  EXPECT_TRUE(endedWithError);
  try {
    std::rethrow_exception(observed);
  } catch (const std::runtime_error& error) {
    EXPECT_STREQ(error.what(), "Kafka delivery: broker unavailable");
  }
}

struct AsyncFailingKafkaSinkHandler final {
  using State = int;
  std::string getStreamId(servicelib::MessageContext, const std::string&) {
    return "async-failed-kafka-sid";
  }
  servicelib::BeginResult<State> beginRequest(
      servicelib::MessageContext context, auto&) {
    return {std::move(context), 0};
  }
  void consumeMessage(servicelib::MessageContext, auto&, State&,
                      const std::string&, auto& message) {
    message.send([](const auto& delivery) {
      return delivery.error ? 41 : 0;
    });
  }
  void endRequest(servicelib::MessageContext, auto&, std::exception_ptr error,
                  State&) noexcept {
    EXPECT_FALSE(error);
  }
};

TEST(KafkaDataSink, PassesAsyncDeliveryFailureToCallback) {
  test_async::AsioRuntime runtime;
  TestEnvironment environment;
  FailingKafkaProducer producer;
  test_async::Event delivered;
  int observed = 0;
  TestSinkEndpointStream<std::string, int> stream{
      environment, 3,
      [&](servicelib::MessageContext, servicelib::Payload<int> result) {
        observed = result.get();
        delivered.Send();
      },
      [&](servicelib::MessageContext,
          servicelib::Payload<std::exception_ptr>) { FAIL(); }};
  servicelib::datasink::kafka::Endpoint<
      std::string, int, AsyncFailingKafkaSinkHandler>
      endpoint{stream, producer, AsyncFailingKafkaSinkHandler{}};
  endpoint.start(servicelib::Context{});
  endpoint.consume(servicelib::MessageContext{},
                   servicelib::Payload<std::string>::make("payload"));
  ASSERT_TRUE(delivered.WaitForEvent());
  endpoint.stop(servicelib::Context{});
  EXPECT_EQ(observed, 41);
}

class FakeKafkaConsumer final
    : public servicelib::datasource::kafka::ConsumerClient {
 public:
  void start(Callback callback) override {
    callback(servicelib::datasource::kafka::ConsumerMessage{
        "key", "payload", "events", 2, 9, [this] { committed = true; }});
  }
  void stop() noexcept override {}
  std::atomic<bool> committed{false};
};

TEST(KafkaDataSource, ExposesCommitAndMarkMessageOperations) {
  bool committed = false;
  bool marked = false;
  std::string metadata;
  servicelib::datasource::kafka::ConsumerMessage message{
      "key", "payload", "events", 2, 9,
      [&] { committed = true; },
      [&](std::string value) {
        marked = true;
        metadata = std::move(value);
      }};

  message.commit();
  message.markMessage("processed");

  EXPECT_TRUE(committed);
  EXPECT_TRUE(marked);
  EXPECT_EQ(metadata, "processed");
}

struct KafkaSourceHandler final {
  using State = int;
  test_async::Event* done;
  std::string* observed;
  int concurrency(auto&) { return 1; }
  servicelib::BeginResult<State> beginRequest(
      servicelib::MessageContext context, auto&) {
    return {std::move(context), 0};
  }
  void consumeMessage(
      servicelib::MessageContext, auto&, State&,
      const servicelib::datasource::kafka::ConsumerMessage& message,
      auto result) {
    *observed = message.topic() + ":" + message.key() + ":" + message.value();
    message.commit();
    result.done();
  }
  std::string getMessageId(servicelib::MessageContext, auto&, State&,
                           const int&) {
    return "result";
  }
  void endRequest(servicelib::MessageContext, auto&, std::exception_ptr error,
                  State&) noexcept {
    EXPECT_FALSE(error);
    done->Send();
  }
};

template <typename T, typename R>
class TestKafkaInput final {
 public:
  using ResultConsumer =
      std::function<void(servicelib::MessageContext, servicelib::Payload<R>)>;

  explicit TestKafkaInput(bool has_result) : has_result_(has_result) {}

  int getEndpointId() const noexcept { return 3; }
  std::size_t getConfigId() const noexcept { return 0; }
  const void* getResultStream() const noexcept {
    return has_result_ ? this : nullptr;
  }
  void setResultConsumer(ResultConsumer consumer) {
    result_consumer_ = std::move(consumer);
  }
  void consume(servicelib::MessageContext, servicelib::Payload<T>) {}
  void consumeError(servicelib::MessageContext,
                    servicelib::Payload<std::exception_ptr>) {}

 private:
  bool has_result_;
  ResultConsumer result_consumer_;
};

TEST(KafkaDataSource, CopiesRecordAndExposesCommit) {
  test_async::AsioRuntime runtime;
  TestEnvironment environment;
  FakeKafkaConsumer consumer;
  test_async::Event done;
  std::string observed;
  TestKafkaInput<std::string, int> input{false};
  auto endpoint = servicelib::datasource::kafka::Endpoint<
      std::string, int, KafkaSourceHandler>::make(
      environment, input, consumer, KafkaSourceHandler{&done, &observed});
  endpoint->start(servicelib::Context{});
  ASSERT_TRUE(done.WaitForEvent());
  endpoint->stop(servicelib::Context{});
  EXPECT_EQ(observed, "events:key:payload");
  EXPECT_TRUE(consumer.committed.load());
}

struct WaitingKafkaSourceHandler final {
  using State = int;
  test_async::Event* entered;
  test_async::Event* ended;
  int concurrency(auto&) { return 1; }
  servicelib::BeginResult<State> beginRequest(
      servicelib::MessageContext context, auto&) {
    return {std::move(context), 0};
  }
  void consumeMessage(
      servicelib::MessageContext, auto&, State&,
      const servicelib::datasource::kafka::ConsumerMessage&, auto) {
    entered->Send();
  }
  std::string getMessageId(servicelib::MessageContext, auto&, State&,
                           const int&) {
    return "result";
  }
  void endRequest(servicelib::MessageContext, auto&, std::exception_ptr error,
                  State&) noexcept {
    EXPECT_TRUE(error);
    ended->Send();
  }
};

TEST(KafkaDataSource, StopCancelsPendingInlineMessageBeforeConsumerStop) {
  test_async::AsioRuntime runtime;
  TestEnvironment environment;
  FakeKafkaConsumer consumer;
  test_async::Event entered;
  test_async::Event ended;
  TestKafkaInput<std::string, int> input{true};
  auto endpoint = servicelib::datasource::kafka::Endpoint<
      std::string, int, WaitingKafkaSourceHandler>::make(
      environment, input, consumer,
      WaitingKafkaSourceHandler{&entered, &ended});
  endpoint->start(servicelib::Context{});
  ASSERT_TRUE(entered.WaitForEvent());
  endpoint->stop(servicelib::Context{});
  EXPECT_TRUE(ended.WaitForEvent());
}

TEST(LibrdkafkaClients, ProduceConsumeAndCommitAgainstBrokerProtocol) {
  using namespace std::chrono_literals;
  MockKafkaCluster cluster;
  cluster.createTopic("events");
  servicelib::config::KafkaDataConnectorConfig connector;
  connector.brokers = cluster.brokers();
  connector.dialTimeout = 5000;
  servicelib::config::KafkaEndpointConfig endpoint;
  endpoint.topic = "events";
  endpoint.consumerGroup = "cppboostservicelib-tests";
  cluster.setGroupCoordinator(endpoint.consumerGroup);

  servicelib::datasink::kafka::LibrdkafkaProducerClient producer{
      servicelib::metrics::NoopMetrics::instance()};
  producer.start(connector);
  const auto first = producer.send("events", "first-key", "first-value", 0);
  ASSERT_FALSE(first.error);
  ASSERT_EQ(first.partition, 0);
  ASSERT_TRUE(first.offset.has_value());

  TestEnvironment environment;
  environment.config().kafkaConnector.brokers = connector.brokers;
  environment.config().kafkaConnector.dialTimeout = connector.dialTimeout;
  environment.config().kafkaEndpoint.topic = endpoint.topic;
  environment.config().kafkaEndpoint.consumerGroup = endpoint.consumerGroup;
  servicelib::datasource::kafka::LibrdkafkaConsumerClient consumer{
      environment, environment.config().kafkaEndpoint.id};
  test_async::Event received;
  std::string observed;
  std::exception_ptr consumerError;
  std::thread consumerThread{[&] {
    try {
      consumer.start(
          [&](servicelib::datasource::kafka::ConsumerMessage message) {
            observed = message.key() + ":" + message.value();
            message.commit();
            received.Send();
            consumer.stop();
          });
    } catch (...) {
      consumerError = std::current_exception();
      received.Send();
    }
  }};

  bool completed = false;
  for (int attempt = 0; attempt < 20 && !completed; ++attempt) {
    const auto sequence = std::to_string(attempt);
    const auto delivery = producer.send(
        "events", "key-" + sequence, "value-" + sequence, 0);
    ASSERT_FALSE(delivery.error);
    completed = received.WaitForEventFor(300ms);
  }
  consumer.stop();
  consumerThread.join();
  ASSERT_TRUE(completed);
  if (consumerError) std::rethrow_exception(consumerError);
  // The consumer explicitly uses auto.offset.reset=earliest, so a fresh
  // group must observe the record published before subscription first.
  EXPECT_EQ(observed, "first-key:first-value");
}

TEST(LibrdkafkaClients, PreservesPartitionOrderAndRunsPartitionsConcurrently) {
  using namespace std::chrono_literals;
  MockKafkaCluster cluster;
  cluster.createTopic("events", 2);
  servicelib::config::KafkaDataConnectorConfig connector;
  connector.brokers = cluster.brokers();
  connector.dialTimeout = 5000;
  const std::string group = "cppboostservicelib-partition-lanes";
  cluster.setGroupCoordinator(group);

  servicelib::datasink::kafka::LibrdkafkaProducerClient producer{
      servicelib::metrics::NoopMetrics::instance()};
  producer.start(connector);
  EXPECT_EQ(producer.partitionCount("events"), 2);
  ASSERT_FALSE(producer.send("events", "p0-0", "value", 0).error);
  ASSERT_FALSE(producer.send("events", "p0-1", "value", 0).error);
  ASSERT_FALSE(producer.send("events", "p1-0", "value", 1).error);

  TestEnvironment environment;
  environment.config().kafkaConnector.brokers = connector.brokers;
  environment.config().kafkaConnector.dialTimeout = connector.dialTimeout;
  environment.config().kafkaEndpoint.topic = "events";
  environment.config().kafkaEndpoint.consumerGroup = group;
  servicelib::datasource::kafka::LibrdkafkaConsumerClient consumer{
      environment, environment.config().kafkaEndpoint.id};

  std::mutex mutex;
  std::condition_variable changed;
  bool partitionOneObserved = false;
  std::map<std::uint32_t, std::vector<std::int64_t>> offsets;
  std::size_t receivedCount = 0;
  test_async::Event completed;
  std::exception_ptr consumerError;
  std::thread consumerThread{[&] {
    try {
      consumer.start(
          [&](servicelib::datasource::kafka::ConsumerMessage message) {
            if (message.partition() == 0 && message.offset() == 0) {
              std::unique_lock lock(mutex);
              EXPECT_TRUE(changed.wait_for(lock, 5s, [&] {
                return partitionOneObserved;
              }));
            }
            {
              std::lock_guard lock(mutex);
              offsets[message.partition()].push_back(message.offset());
              if (message.partition() == 1) partitionOneObserved = true;
              ++receivedCount;
              if (receivedCount == 3) {
                consumer.stop();
                completed.Send();
              }
            }
            changed.notify_all();
          });
    } catch (...) {
      consumerError = std::current_exception();
      completed.Send();
    }
  }};

  const auto allReceived = completed.WaitForEventFor(10s);
  consumer.stop();
  changed.notify_all();
  consumerThread.join();
  ASSERT_TRUE(allReceived);
  if (consumerError) std::rethrow_exception(consumerError);
  ASSERT_EQ(offsets[0].size(), 2);
  EXPECT_LT(offsets[0][0], offsets[0][1]);
  ASSERT_EQ(offsets[1].size(), 1);
}

TEST(LibrdkafkaAdmin, DisabledEndpointDoesNotContactBroker) {
  servicelib::config::KafkaDataConnectorConfig connector;
  servicelib::config::KafkaEndpointConfig endpoint;
  endpoint.enabled = false;
  endpoint.createTopic = true;
  endpoint.topic = "must-not-be-created";

  EXPECT_NO_THROW(servicelib::detail::EnsureKafkaTopic(connector, endpoint));
}

TEST(LibrdkafkaAdmin, CreateTopicFalseDoesNotContactBroker) {
  servicelib::config::KafkaDataConnectorConfig connector;
  servicelib::config::KafkaEndpointConfig endpoint;
  endpoint.enabled = true;
  endpoint.createTopic = false;
  endpoint.topic = "must-not-be-created";

  EXPECT_NO_THROW(servicelib::detail::EnsureKafkaTopic(connector, endpoint));
}

TEST(LibrdkafkaAdmin, ValidatesTopicAndBrokersBeforeConnecting) {
  servicelib::config::KafkaDataConnectorConfig connector;
  servicelib::config::KafkaEndpointConfig endpoint;
  endpoint.enabled = true;
  endpoint.createTopic = true;
  EXPECT_THROW(servicelib::detail::EnsureKafkaTopic(connector, endpoint),
               std::invalid_argument);

  endpoint.topic = "events";
  EXPECT_THROW(servicelib::detail::EnsureKafkaTopic(connector, endpoint),
               std::invalid_argument);
}

TEST(LibrdkafkaClients, BrokerLossReturnsErrorAndConsumerRemainsStoppable) {
  using namespace std::chrono_literals;
  MockKafkaCluster cluster;
  cluster.createTopic("events");
  servicelib::config::KafkaDataConnectorConfig connector;
  connector.brokers = cluster.brokers();
  // Sanitizers can make the mock broker's initial metadata and delivery
  // handshake substantially slower. Keep the timeout large enough to prove a
  // successful baseline delivery before simulating broker loss.
  connector.dialTimeout = 2000;
  servicelib::config::KafkaEndpointConfig endpoint;
  endpoint.topic = "events";
  endpoint.consumerGroup = "cppboostservicelib-broker-loss";
  cluster.setGroupCoordinator(endpoint.consumerGroup);

  servicelib::datasink::kafka::LibrdkafkaProducerClient producer{
      servicelib::metrics::NoopMetrics::instance()};
  producer.start(connector);
  const auto beforeLoss =
      producer.send("events", "before-key", "before-value", 0);
  ASSERT_FALSE(beforeLoss.error);

  cluster.setBrokerDown();
  const auto failed = producer.send("events", "lost-key", "lost-value", 0);
  if (!failed.error) {
    FAIL() << "producer unexpectedly delivered while broker was down";
  }
  EXPECT_FALSE(failed.offset.has_value());
  try {
    std::rethrow_exception(failed.error);
  } catch (const std::runtime_error& error) {
    EXPECT_TRUE(std::string{error.what()}.starts_with("Kafka delivery"));
  }

  // Start while the mock broker is already unavailable. This proves that the
  // blocking librdkafka polling boundary remains stoppable without asking the
  // single-node mock to recover an in-flight group SyncGroup. Real same-group
  // loss and recovery is covered by the generated Redpanda transport gate.
  auto consumerConnector = connector;
  consumerConnector.dialTimeout = 2000;
  TestEnvironment environment;
  environment.config().kafkaConnector.brokers = consumerConnector.brokers;
  environment.config().kafkaConnector.dialTimeout =
      consumerConnector.dialTimeout;
  environment.config().kafkaEndpoint.topic = endpoint.topic;
  environment.config().kafkaEndpoint.consumerGroup = endpoint.consumerGroup;
  servicelib::datasource::kafka::LibrdkafkaConsumerClient unavailableConsumer{
      environment, environment.config().kafkaEndpoint.id};
  std::exception_ptr consumerError;
  std::thread consumerThread{[&] {
    try {
      unavailableConsumer.start([](auto) {});
    } catch (...) {
      consumerError = std::current_exception();
    }
  }};
  std::this_thread::sleep_for(150ms);
  unavailableConsumer.stop();
  consumerThread.join();
  // A poll error is allowed while the broker is down; termination and join
  // are the lifecycle contract asserted here.
  static_cast<void>(consumerError);
}

}  // namespace

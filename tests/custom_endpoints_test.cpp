#include <gtest/gtest.h>

#include <servicelib/datasink/localsink/custom.hpp>
#include <servicelib/datasource/localsource/custom.hpp>
#include <servicelib/datasource/kafka/detail/endpoint.hpp>
#include <servicelib/runtime/environment/environment.hpp>
#include <servicelib/runtime/testlog/testlog.hpp>
#include <servicelib/runtime/testmetrics/testmetrics.hpp>
#include <servicelib/runtime/testtracing/testtracing.hpp>

#include "test_sink_endpoint_stream.hpp"

#include "test_async.hpp"

#include <atomic>
#include <exception>
#include <future>
#include <memory>
#include <semaphore>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace {

class TestConfig final : public servicelib::config::IConfig {
 public:
  TestConfig() {
    customEndpoint.id = 1;
    customEndpoint.name = "custom-messages";
    customEndpoint.idDataConnector = 2;
    customConnector.id = 2;
    customConnector.name = "custom";
    sinkStream.id = 101;
    sinkStream.name = "Publish Booking";
    sinkStream.pipeline = "booking";
    sinkStream.component = "Reserve Inventory";
    sinkStream.idEndpoint = 1;
  }

  std::vector<const servicelib::config::ServiceConfig*> GetServices()
      const override {
    return {};
  }
  std::vector<servicelib::config::StreamConfigRef> GetStreams() const override {
    return {sinkStream};
  }
  std::vector<servicelib::config::DataConnectorConfigRef> GetDataConnectors()
      const override {
    return {customConnector};
  }
  std::vector<servicelib::config::EndpointConfigRef> GetEndpoints()
      const override {
    return {customEndpoint};
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

  servicelib::config::CustomEndpointConfig customEndpoint;
  servicelib::config::SinkStreamConfig sinkStream;
  servicelib::config::CustomDataConnectorConfig customConnector;
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
    if (forbidRuntimeConfigReads) {
      throw std::logic_error("sink tracing reread runtime configuration");
    }
    return std::make_shared<const servicelib::config::RuntimeConfig>(
        runtimeConfig_);
  }
  std::shared_ptr<const servicelib::config::ServiceConfig>
  getServiceConfigSnapshot() const override {
    serviceConfigReads_.fetch_add(1, std::memory_order_relaxed);
    return std::make_shared<const servicelib::config::ServiceConfig>(service_);
  }
  servicelib::log::Logger& getLogger() override { return log_; }
  servicelib::metrics::Metrics& getMetrics() override { return metrics_; }
  servicelib::tracing::Tracing* getTracing() override { return tracingEngine; }
  servicelib::tracing::Tracing* tracingEngine{};
  bool forbidRuntimeConfigReads{};
  [[nodiscard]] std::size_t serviceConfigReads() const noexcept {
    return serviceConfigReads_.load(std::memory_order_relaxed);
  }

 private:
  TestConfig config_;
  servicelib::config::RuntimeConfig runtimeConfig_;
  servicelib::config::ServiceConfig service_;
  servicelib::testlog::TestLog log_;
  servicelib::testmetrics::TestMetrics metrics_;
  mutable std::atomic<std::size_t> serviceConfigReads_{};
};

class OneValueProducer final
    : public servicelib::datasource::localsource::DataProducer<std::string> {
 public:
  void start(servicelib::Context, Consumer consumer) override {
    consumer(servicelib::MessageContext{},
             servicelib::Payload<std::string>::make("input"));
  }
  void stop(servicelib::Context) override {}
};

struct CustomSourceHandler final {
  using State = int;
  test_async::Event* done;
  std::string* observed;
  int concurrency(auto&) { return 1; }
  servicelib::BeginResult<State> beginRequest(
      servicelib::MessageContext context, auto&) {
    return {std::move(context), 1};
  }
  void consumeMessage(servicelib::MessageContext, auto&, State&,
                      const std::string& value, auto result) {
    *observed = value;
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

TEST(CustomDataSource, RunsProducerAndHandlerLifecycle) {
  test_async::AsioRuntime runtime;
  TestEnvironment environment;
  OneValueProducer producer;
  test_async::Event done;
  std::string observed;
  using Endpoint =
      servicelib::datasource::localsource::Endpoint<std::string, int,
                                                    CustomSourceHandler>;
  Endpoint endpoint{
      environment,
      1,
      producer,
      CustomSourceHandler{&done, &observed},
      [](servicelib::MessageContext, servicelib::Payload<std::string>) {},
      false};
  endpoint.start(servicelib::Context{});
  ASSERT_TRUE(done.WaitForEvent());
  endpoint.stop(servicelib::Context{});
  EXPECT_EQ(observed, "input");
}

class ObservedReturnProducer final
    : public servicelib::datasource::localsource::DataProducer<std::string> {
 public:
  explicit ObservedReturnProducer(std::atomic<bool>& ended) : ended_(ended) {}
  void start(servicelib::Context, Consumer consumer) override {
    consumer(servicelib::MessageContext{},
             servicelib::Payload<std::string>::make("input"));
    returned.set_value(ended_.load(std::memory_order_acquire));
  }
  void stop(servicelib::Context) override {}
  std::promise<bool> returned;

 private:
  std::atomic<bool>& ended_;
};

struct GatedReturnSourceHandler final {
  using State = int;
  test_async::Event* entered;
  test_async::Event* release;
  std::atomic<bool>* ended;
  int concurrency(auto&) { return 1; }
  servicelib::BeginResult<State> beginRequest(
      servicelib::MessageContext context, auto&) {
    return {std::move(context), 0};
  }
  void consumeMessage(servicelib::MessageContext, auto&, State&,
                      const std::string&, auto result) {
    entered->Send();
    EXPECT_TRUE(release->WaitForEvent());
    result.done();
  }
  std::string getMessageId(servicelib::MessageContext, auto&, State&,
                           const int&) { return "result"; }
  void endRequest(servicelib::MessageContext, auto&, std::exception_ptr error,
                  State&) noexcept {
    EXPECT_FALSE(error);
    ended->store(true, std::memory_order_release);
  }
};

TEST(CustomDataSource, ProducerConsumeReturnsOnlyAfterEndRequest) {
  for (const bool hasResult : {false, true}) {
    SCOPED_TRACE(hasResult);
    test_async::AsioRuntime runtime;
    TestEnvironment environment;
    std::atomic<bool> ended{false};
    ObservedReturnProducer producer{ended};
    auto returned = producer.returned.get_future();
    test_async::Event entered;
    test_async::Event release;
    using Endpoint = servicelib::datasource::localsource::Endpoint<
        std::string, int, GatedReturnSourceHandler>;
    Endpoint endpoint{
        environment, 1, producer,
        GatedReturnSourceHandler{&entered, &release, &ended},
        [](servicelib::MessageContext, servicelib::Payload<std::string>) {},
        hasResult};
    endpoint.start(servicelib::Context{});
    EXPECT_TRUE(entered.WaitForEvent());
    EXPECT_EQ(returned.wait_for(std::chrono::milliseconds{100}),
              std::future_status::timeout);
    release.Send();
    const auto status = returned.wait_for(test_async::kMaxTestWaitTime);
    endpoint.stop(servicelib::Context{});
    ASSERT_EQ(status, std::future_status::ready);
    EXPECT_TRUE(returned.get());
    EXPECT_TRUE(ended.load(std::memory_order_acquire));
  }
}

struct DuplicateSourceProbe final {
  test_async::Event firstConsumed;
  test_async::Event duplicateReturned;
  test_async::Event reuseReturned;
  std::atomic<int> begins{0};
  std::atomic<int> consumes{0};
  std::atomic<int> results{0};
  std::atomic<int> successfulEnds{0};
  std::atomic<int> duplicateErrors{0};
  std::function<void()> rescueFirst;
};

class DuplicateSourceProducer final
    : public servicelib::datasource::localsource::DataProducer<std::string> {
 public:
  explicit DuplicateSourceProducer(DuplicateSourceProbe& probe) : probe_(probe) {}
  void start(servicelib::Context, Consumer consumer) override {
    const auto context = servicelib::MessageContext{}.withStreamId("source-collision");
    std::thread first([&] {
      consumer(context, servicelib::Payload<std::string>::make("first"));
    });
    EXPECT_TRUE(probe_.firstConsumed.WaitForEvent());
    consumer(context, servicelib::Payload<std::string>::make("duplicate"));
    probe_.duplicateReturned.Send();
    first.join();
    consumer(context, servicelib::Payload<std::string>::make("reuse"));
    probe_.reuseReturned.Send();
  }
  void stop(servicelib::Context) override {}

 private:
  DuplicateSourceProbe& probe_;
};

struct DuplicateSourceHandler final {
  using State = int;
  DuplicateSourceProbe* probe;
  int concurrency(auto&) { return 0; }
  servicelib::BeginResult<State> beginRequest(servicelib::MessageContext context, auto&) {
    return {std::move(context), ++probe->begins};
  }
  void consumeMessage(servicelib::MessageContext, auto&, State& ordinal,
                      const std::string&, auto result) {
    ++probe->consumes;
    if (ordinal != 1) {
      result.done();
      return;
    }
    result.setResultCallback("answer", [probe = probe, result](
        servicelib::MessageContext, auto&, State&, const int& value) mutable {
      EXPECT_EQ(value, 42);
      ++probe->results;
      result.done();
      return true;
    });
    probe->rescueFirst = [result]() mutable {
      result.setResultCallback("answer", nullptr);
      result.done();
    };
    probe->firstConsumed.Send();
  }
  std::string getMessageId(servicelib::MessageContext, auto&, State&,
                           const int&) { return "answer"; }
  void endRequest(servicelib::MessageContext, auto&, std::exception_ptr error,
                  State& ordinal) noexcept {
    if (ordinal == 2) {
      EXPECT_TRUE(error);
      if (error) {
        try {
          std::rethrow_exception(error);
        } catch (const servicelib::store::DuplicateKeyError&) {
          ++probe->duplicateErrors;
        } catch (...) {
          ADD_FAILURE() << "duplicate registration returned the wrong error";
        }
      }
    } else {
      EXPECT_FALSE(error);
      ++probe->successfulEnds;
    }
  }
};

template <typename MakeEndpoint>
void CheckDuplicateSourceKeepsOriginalResult(MakeEndpoint makeEndpoint) {
  test_async::AsioRuntime runtime;
  TestEnvironment environment;
  DuplicateSourceProbe probe;
  DuplicateSourceProducer producer{probe};
  auto endpoint = makeEndpoint(environment, producer, probe);
  endpoint->start(servicelib::Context{});
  EXPECT_TRUE(probe.duplicateReturned.WaitForEvent());
  endpoint->consumeResult(
      servicelib::MessageContext{}.withStreamId("source-collision"),
      servicelib::Payload<int>::make(42));
  EXPECT_EQ(probe.results.load(), 1);
  // Complete the first request explicitly if a broken duplicate cleanup lost it.
  if (probe.results.load() == 0 && probe.rescueFirst) probe.rescueFirst();
  EXPECT_TRUE(probe.reuseReturned.WaitForEvent());
  endpoint->stop(servicelib::Context{});
  probe.rescueFirst = {};
  EXPECT_EQ(probe.begins.load(), 3);
  EXPECT_EQ(probe.consumes.load(), 2);
  EXPECT_EQ(probe.duplicateErrors.load(), 1);
  EXPECT_EQ(probe.successfulEnds.load(), 2);
}

TEST(CustomDataSource, DuplicateRegistrationPreservesOriginalResult) {
  CheckDuplicateSourceKeepsOriginalResult([](auto& environment, auto& producer,
                                             auto& probe) {
    using Endpoint = servicelib::datasource::localsource::Endpoint<
        std::string, int, DuplicateSourceHandler>;
    return std::make_unique<Endpoint>(
        environment, 1, producer, DuplicateSourceHandler{&probe},
        [](servicelib::MessageContext, servicelib::Payload<std::string>) {}, true);
  });
}

TEST(KafkaSourceState, DuplicateRegistrationPreservesOriginalResult) {
  CheckDuplicateSourceKeepsOriginalResult([](auto& environment, auto& producer,
                                             auto& probe) {
    using Endpoint = servicelib::datasource::kafka::detail::EndpointState<
        std::string, int, DuplicateSourceHandler, std::exception_ptr,
        std::string, DuplicateSourceProducer>;
    return std::make_unique<Endpoint>(
        environment, 1, 0, producer, DuplicateSourceHandler{&probe},
        [](servicelib::MessageContext, servicelib::Payload<std::string>) {},
        true, "kafka-probe", "input", typename Endpoint::ErrorOutput{});
  });
}

class FourValueProducer final
    : public servicelib::datasource::localsource::DataProducer<std::string> {
 public:
  void start(servicelib::Context, Consumer consumer) override {
    std::vector<std::thread> producers;
    for (int index = 0; index < 4; ++index) {
      producers.emplace_back([consumer] {
        consumer(servicelib::MessageContext{},
                 servicelib::Payload<std::string>::make("input"));
      });
    }
    for (auto& producer : producers) producer.join();
  }
  void stop(servicelib::Context) override {}
};

struct BlockingSourceHandler final {
  using State = int;
  std::atomic<int>* started;
  test_async::Event* allStarted;
  std::counting_semaphore<4>* release;

  int concurrency(auto&) { return 0; }
  servicelib::BeginResult<State> beginRequest(
      servicelib::MessageContext context, auto&) {
    return {std::move(context), 0};
  }
  void consumeMessage(servicelib::MessageContext, auto&, State&,
                      const std::string&, auto) {
    if (started->fetch_add(1, std::memory_order_acq_rel) == 3) {
      allStarted->Send();
    }
    release->acquire();
  }
  std::string getMessageId(servicelib::MessageContext, auto&, State&,
                           const int&) {
    return {};
  }
  void endRequest(servicelib::MessageContext, auto&, std::exception_ptr,
                  State&) noexcept {}
};

TEST(CustomDataSource, BlockingHandlerDoesNotBlockReactorWorkers) {
  test_async::AsioRuntime runtime;
  TestEnvironment environment;
  FourValueProducer producer;
  std::atomic<int> started{0};
  test_async::Event allStarted;
  test_async::Event reactorProgress;
  std::counting_semaphore<4> release{0};
  using Endpoint = servicelib::datasource::localsource::Endpoint<
      std::string, int, BlockingSourceHandler>;
  Endpoint endpoint{
      environment,
      1,
      producer,
      BlockingSourceHandler{&started, &allStarted, &release},
      [](servicelib::MessageContext, servicelib::Payload<std::string>) {},
      false};

  endpoint.start(servicelib::Context{});
  ASSERT_TRUE(allStarted.WaitForEvent());
  servicelib::detail::ParallelExecutorRegistry::Post(
      [&reactorProgress] { reactorProgress.Send(); });
  EXPECT_TRUE(
      reactorProgress.WaitForEventFor(std::chrono::milliseconds{100}));
  release.release(4);
  endpoint.stop(servicelib::Context{});
}

struct CorrelatingSourceHandler final {
  using State = int;
  test_async::Event* done;
  int* observedResult;
  int concurrency(auto&) { return 0; }
  servicelib::BeginResult<State> beginRequest(
      servicelib::MessageContext context, auto&) {
    return {std::move(context), 5};
  }
  void consumeMessage(servicelib::MessageContext context, auto& stream, State&,
                      const std::string&, auto result) {
    result.setResultCallback(
        "answer", [result, observed = observedResult](
                      servicelib::MessageContext, auto&, State& state,
                      const int& value) mutable {
          *observed = state + value;
          result.done();
          return true;
        });
    stream.collect(std::move(context), std::string{"request"});
  }
  std::string getMessageId(servicelib::MessageContext, auto&, State&,
                           const int&) {
    return "answer";
  }
  void endRequest(servicelib::MessageContext, auto&, std::exception_ptr error,
                  State&) noexcept {
    EXPECT_FALSE(error);
    done->Send();
  }
};

TEST(CustomDataSource, CorrelatesPipelineResultUsingStreamContext) {
  test_async::AsioRuntime runtime;
  TestEnvironment environment;
  OneValueProducer producer;
  test_async::Event done;
  int observedResult = 0;
  using Endpoint =
      servicelib::datasource::localsource::Endpoint<std::string, int,
                                                    CorrelatingSourceHandler>;
  Endpoint* endpointPtr = nullptr;
  Endpoint endpoint{environment,
                    1,
                    producer,
                    CorrelatingSourceHandler{&done, &observedResult},
                    [&](servicelib::MessageContext context,
                        servicelib::Payload<std::string> value) {
                      EXPECT_EQ(value.get(), "request");
                      endpointPtr->consumeResult(
                          std::move(context),
                          servicelib::Payload<int>::make(37));
                    },
                    true};
  endpointPtr = &endpoint;
  endpoint.start(servicelib::Context{});
  ASSERT_TRUE(done.WaitForEvent());
  endpoint.stop(servicelib::Context{});
  EXPECT_EQ(observedResult, 42);
}

struct MultiResultSourceHandler final {
  using State = int;
  test_async::Event* done;
  std::atomic<int>* observed;
  int concurrency(auto&) { return 0; }
  servicelib::BeginResult<State> beginRequest(
      servicelib::MessageContext context, auto&) {
    return {std::move(context), 0};
  }
  void consumeMessage(servicelib::MessageContext context, auto& stream, State&,
                      const std::string&, auto result) {
    result.setResultCallback(
        "answer", [result, observed = observed, calls = 0](servicelib::MessageContext,
                                                auto&, State& state,
                                                const int& value) mutable {
          observed->fetch_add(value, std::memory_order_relaxed);
          EXPECT_EQ(++calls, ++state);
          if (state == 2) result.done();
          return state == 2;
        });
    stream.collect(context, std::string{"first"});
    stream.collect(std::move(context), std::string{"second"});
  }
  std::string getMessageId(servicelib::MessageContext, auto&, State&,
                           const int&) {
    return "answer";
  }
  void endRequest(servicelib::MessageContext, auto&, std::exception_ptr error,
                  State& state) noexcept {
    EXPECT_FALSE(error);
    EXPECT_EQ(state, 2);
    done->Send();
  }
};

TEST(CustomDataSource, SupportsMultiPushAndPersistentResultCallback) {
  test_async::AsioRuntime runtime;
  TestEnvironment environment;
  OneValueProducer producer;
  test_async::Event done;
  std::atomic<int> observed{0};
  using Endpoint = servicelib::datasource::localsource::Endpoint<
      std::string, int, MultiResultSourceHandler>;
  Endpoint* endpointPtr = nullptr;
  Endpoint endpoint{
      environment,
      1,
      producer,
      MultiResultSourceHandler{&done, &observed},
      [&](servicelib::MessageContext context,
          servicelib::Payload<std::string> value) {
        endpointPtr->consumeResult(
            std::move(context), servicelib::Payload<int>::make(
                                    value.get() == "first" ? 10 : 20));
      },
      true};
  endpointPtr = &endpoint;
  endpoint.start(servicelib::Context{});
  ASSERT_TRUE(done.WaitForEvent());
  endpoint.stop(servicelib::Context{});
  EXPECT_EQ(observed.load(std::memory_order_relaxed), 30);
}

struct CustomSinkHandler final {
  using State = int;
  std::string* observed;
  std::string getStreamId(servicelib::MessageContext, const std::string&) {
    return "sid";
  }
  servicelib::BeginResult<State> beginRequest(
      servicelib::MessageContext context, auto&) {
    return {std::move(context), 2};
  }
  void consumeMessage(servicelib::MessageContext context, auto& stream, State&,
                      const std::string& value) {
    *observed = std::string{context.streamId()} + ":" + value;
    stream.collect(context, 42);
  }
  void endRequest(servicelib::MessageContext, auto&, std::exception_ptr error,
                  State&) noexcept {
    EXPECT_FALSE(error);
  }
};

TEST(CustomDataSink, PreservesLifecycleAndCollectsResult) {
  TestEnvironment environment;
  std::string observed;
  int result = 0;
  TestSinkEndpointStream<std::string, int> stream{
      environment, 1,
      [&](servicelib::MessageContext context, servicelib::Payload<int> value) {
        EXPECT_EQ(context.streamId(), "sid");
        result = value.get();
      }};
  servicelib::datasink::localsink::Endpoint<std::string, int, CustomSinkHandler>
      endpoint{stream, CustomSinkHandler{&observed}};
  bool callbackCalled = false;
  endpoint.setSinkCallback([&](servicelib::MessageContext,
                               const std::string& value,
                               std::exception_ptr error) {
    EXPECT_EQ(value, "value");
    EXPECT_FALSE(error);
    callbackCalled = true;
  });
  endpoint.consume(servicelib::MessageContext{},
                   servicelib::Payload<std::string>::make("value"));
  EXPECT_EQ(observed, "sid:value");
  EXPECT_EQ(result, 42);
  EXPECT_TRUE(callbackCalled);
}

TEST(CustomDataSink, UnsampledRequestDoesNotResolveTracingConfiguration) {
  TestEnvironment environment;
  std::string observed;
  TestSinkEndpointStream<std::string, int> stream{environment, 1};
  servicelib::datasink::localsink::Endpoint<std::string, int,
                                            CustomSinkHandler>
      endpoint{stream, CustomSinkHandler{&observed}};
  const auto before = environment.serviceConfigReads();
  endpoint.consume(servicelib::MessageContext{},
                   servicelib::Payload<std::string>::make("value"));
  EXPECT_EQ(environment.serviceConfigReads(), before);
  EXPECT_EQ(observed, "sid:value");
}

TEST(CustomDataSink, SampledSpansUseCachedTypedGrouping) {
  servicelib::testtracing::TestTracing tracing;
  TestEnvironment environment;
  environment.tracingEngine = &tracing;
  std::string observed;
  TestSinkEndpointStream<std::string, int> stream{environment, 1, {}, {}, 101};
  servicelib::datasink::localsink::Endpoint<std::string, int, CustomSinkHandler>
      endpoint{stream, CustomSinkHandler{&observed}};
  environment.forbidRuntimeConfigReads = true;
  endpoint.consume(servicelib::MessageContext{},
                   servicelib::Payload<std::string>::make("unsampled"));
  EXPECT_TRUE(tracing.spans().empty());
  for (int call = 0; call < 2; ++call) {
    endpoint.consume(servicelib::MessageContext{}.withSampling(true),
                     servicelib::Payload<std::string>::make("sampled"));
  }
  EXPECT_EQ(observed, "sid:sampled");
  const auto spans = tracing.spans();
  ASSERT_EQ(spans.size(), 2);
  for (const auto& span : spans) {
    EXPECT_EQ(span.name, "local.output");
    std::size_t grouping = 0;
    for (const auto& attribute : span.attributes) {
      if (attribute.key() == "stream") {
        EXPECT_EQ(std::get<std::string>(attribute.value()), "Publish Booking");
      }
      if (attribute.key() == "pipeline") {
        EXPECT_EQ(std::get<std::string>(attribute.value()), "booking");
        ++grouping;
      }
      if (attribute.key() == "component") {
        EXPECT_EQ(std::get<std::string>(attribute.value()), "Reserve Inventory");
        ++grouping;
      }
      EXPECT_NE(attribute.key(), "component_instance");
    }
    EXPECT_EQ(grouping, 2);
  }
}

struct MultiPushSinkHandler final {
  using State = int;
  bool fail{};
  std::string getStreamId(servicelib::MessageContext, const std::string&) {
    return "multi-sid";
  }
  servicelib::BeginResult<State> beginRequest(
      servicelib::MessageContext context, auto&) {
    return {std::move(context), 0};
  }
  void consumeMessage(servicelib::MessageContext context, auto& stream, State&,
                      const std::string&) {
    if (fail) throw std::runtime_error("sink failure");
    stream.collect(context, 1);
    stream.collect(std::move(context), 2);
  }
  void endRequest(servicelib::MessageContext, auto&, std::exception_ptr,
                  State&) noexcept {}
};

TEST(CustomDataSink, SupportsMultiPushAndPropagatesErrors) {
  TestEnvironment environment;
  std::vector<int> results;
  std::exception_ptr collectedError;
  bool callbackSawError = false;
  TestSinkEndpointStream<std::string, int> stream{
      environment, 1,
      [&](servicelib::MessageContext, servicelib::Payload<int> value) {
        results.push_back(value.get());
      },
      [&](servicelib::MessageContext,
          servicelib::Payload<std::exception_ptr> error) {
        collectedError = error.get();
      }};
  servicelib::datasink::localsink::Endpoint<std::string, int,
                                            MultiPushSinkHandler>
      endpoint{stream, MultiPushSinkHandler{}};
  endpoint.consume(servicelib::MessageContext{},
                   servicelib::Payload<std::string>::make("ok"));
  EXPECT_EQ(results, (std::vector<int>{1, 2}));

  TestSinkEndpointStream<std::string, int> failingStream{
      environment, 1, {},
      [&](servicelib::MessageContext,
          servicelib::Payload<std::exception_ptr> error) {
        collectedError = error.get();
      }};
  servicelib::datasink::localsink::Endpoint<std::string, int,
                                            MultiPushSinkHandler>
      failing{failingStream, MultiPushSinkHandler{true}};
  failing.setSinkCallback(
      [&](servicelib::MessageContext, const std::string&,
          std::exception_ptr error) { callbackSawError = error != nullptr; });
  failing.consume(servicelib::MessageContext{},
                  servicelib::Payload<std::string>::make("fail"));
  EXPECT_TRUE(collectedError);
  EXPECT_TRUE(callbackSawError);
}

}  // namespace

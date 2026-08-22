#include <gtest/gtest.h>

#include <servicelib/datasink/localsink/custom.hpp>
#include <servicelib/datasource/localsource/custom.hpp>
#include <servicelib/runtime/environment/environment.hpp>
#include <servicelib/runtime/testlog/testlog.hpp>
#include <servicelib/runtime/testmetrics/testmetrics.hpp>

#include "test_sink_endpoint_stream.hpp"

#include "test_async.hpp"

#include <atomic>
#include <exception>
#include <memory>
#include <semaphore>
#include <string>
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
  servicelib::tracing::Tracing* getTracing() override { return nullptr; }
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

class FourValueProducer final
    : public servicelib::datasource::localsource::DataProducer<std::string> {
 public:
  void start(servicelib::Context, Consumer consumer) override {
    for (int index = 0; index < 4; ++index) {
      consumer(servicelib::MessageContext{},
               servicelib::Payload<std::string>::make("input"));
    }
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
        "answer", [result, observed = observed](servicelib::MessageContext,
                                                auto&, State& state,
                                                const int& value) mutable {
          observed->fetch_add(value, std::memory_order_relaxed);
          ++state;
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

#include <gtest/gtest.h>

#include <chrono>
#include <array>
#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include <servicelib/transformation/streams.hpp>

namespace {

struct TopologyDataTypes final {
  template <typename>
  struct DataType {};
};

servicelib::config::SinkStreamConfig sinkConfig(int id, std::string name) {
  servicelib::config::SinkStreamConfig config;
  config.id = id;
  config.name = std::move(name);
  return config;
}

struct RecordValue final {
  std::vector<int>* values{};
  void operator()(servicelib::MessageContext, const int& value) const {
    values->push_back(value);
  }
};

struct MixedSplitConfig final : servicelib::config::IConfig {
  servicelib::config::InputStreamConfig input;
  servicelib::config::SplitStreamConfig split;
  std::array<servicelib::config::SinkStreamConfig, 4> sinks;
  std::array<servicelib::config::LinkConfig, 4> links;

  explicit MixedSplitConfig(bool withPool = true) {
    input.id = 301;
    input.name = "mixed-split-input";
    split.id = 302;
    split.name = "mixed-split";
    for (std::size_t index = 0; index < sinks.size(); ++index) {
      sinks[index] = sinkConfig(303 + static_cast<int>(index),
                                "mixed-split-sink-" + std::to_string(index));
      links[index].from = split.id;
      links[index].to = sinks[index].id;
      links[index].callSemantics = servicelib::config::MakeCallSemanticsGroup(
          servicelib::api::CallSemantics::kFunctionCall, {}, 0,
          index == 1 || index == 2);
    }
    if (withPool) {
      links[1].callSemantics = servicelib::config::MakeCallSemanticsGroup(
          servicelib::api::CallSemantics::kTaskPool, "split-deferred");
    }
  }

  std::vector<const servicelib::config::ServiceConfig*> GetServices() const override {
    return {};
  }
  std::vector<servicelib::config::StreamConfigRef> GetStreams() const override {
    return {input, split, sinks[0], sinks[1], sinks[2], sinks[3]};
  }
  std::vector<servicelib::config::DataConnectorConfigRef> GetDataConnectors() const override {
    return {};
  }
  std::vector<servicelib::config::EndpointConfigRef> GetEndpoints() const override {
    return {};
  }
  std::vector<const servicelib::config::PoolConfig*> GetPools() const override { return {}; }
  std::vector<const servicelib::config::LinkConfig*> GetLinks() const override {
    return {&links[0], &links[1], &links[2], &links[3]};
  }
  std::vector<const servicelib::config::ModuleConfig*> GetModules() const override { return {}; }
  std::vector<const servicelib::config::TypeConfig*> GetTypes() const override { return {}; }
};

class SplitDeferredPool final : public servicelib::pool::ITaskPool {
 public:
  explicit SplitDeferredPool(std::vector<std::string>& trace) : trace_(trace) {}
  const std::string& getName() const noexcept override { return name_; }
  int getExecutorsCount() const override { return 1; }
  void start(servicelib::Context) override {}
  void stop(servicelib::Context) override {}
  void addTask(servicelib::Context, std::function<void()> task) override {
    trace_.push_back("enqueue");
    tasks.push_back(std::move(task));
  }
  void drain() {
    auto admitted = std::move(tasks);
    tasks.clear();
    for (auto& task : admitted) task();
  }
  std::vector<std::function<void()>> tasks;

 private:
  const std::string name_{"split-deferred"};
  std::vector<std::string>& trace_;
};

struct RecordSplitBranch final {
  std::vector<std::string>* trace;
  const char* name;
  void operator()(servicelib::MessageContext context, const int& value) const {
    EXPECT_EQ(context.streamId(), "split-call-" + std::to_string(value));
    trace->push_back(std::string(name) + ":" + std::to_string(value));
  }
};

class MixedSplitApp final
    : public servicelib::StreamExecutionEnvironment<MixedSplitApp, TopologyDataTypes> {
 public:
  explicit MixedSplitApp(bool withPool = true) : config(withPool) {}
  MixedSplitConfig config;
  std::shared_ptr<const servicelib::config::RuntimeConfig> snapshot{
      std::make_shared<const servicelib::config::RuntimeConfig>(config)};
  std::vector<std::string> trace;
  SplitDeferredPool pool{trace};
  std::shared_ptr<servicelib::InputStream<int, std::monostate, int, MixedSplitApp>> input;

  std::shared_ptr<const servicelib::config::RuntimeConfig>
  getRuntimeConfigSnapshot() const override { return snapshot; }
  servicelib::pool::ITaskPool* getTaskPool(const std::string& name) override {
    return name == pool.getName() ? &pool : nullptr;
  }
  void delay(servicelib::Context, servicelib::pool::IDelayPool::Duration,
             std::function<void()> task) override { task(); }
  void init() {
    input = servicelib::makeInputStream<int, std::monostate, int, MixedSplitApp>(
        config.input, nullptr, *this);
    auto& split = input->template split<4>(config.split);
    split.template get<0>().sink(config.sinks[0], servicelib::StreamType<int>{},
        servicelib::StreamFunction((RecordSplitBranch{&trace, "first-sync"})));
    split.template get<1>().sink(config.sinks[1], servicelib::StreamType<int>{},
        servicelib::StreamFunction((RecordSplitBranch{
            &trace, config.links[1].callSemantics->taskPool ? "pooled" : "first-priority"})));
    split.template get<2>().sink(config.sinks[2], servicelib::StreamType<int>{},
        servicelib::StreamFunction((RecordSplitBranch{&trace, "direct-async"})));
    split.template get<3>().sink(config.sinks[3], servicelib::StreamType<int>{},
        servicelib::StreamFunction((RecordSplitBranch{&trace, "last-sync"})));
    static_cast<void>(getExecutionRuntime<>());
  }
};

TEST(OperatorsTopology, SplitUsesConfiguredAsyncOrderWithoutDetachingFunctionCalls) {
  MixedSplitApp app;
  app.init();
  for (const int value : {1, 2}) {
    app.input->consume(
        servicelib::MessageContext{}.withStreamId("split-call-" + std::to_string(value)),
        servicelib::Payload<int>::make(value));
    app.trace.push_back("returned:" + std::to_string(value));
  }
  const std::vector<std::string> beforeDrain{
      "enqueue", "direct-async:1", "first-sync:1", "last-sync:1", "returned:1",
      "enqueue", "direct-async:2", "first-sync:2", "last-sync:2", "returned:2"};
  EXPECT_EQ(app.trace, beforeDrain);
  EXPECT_EQ(app.pool.tasks.size(), 2U);
  app.pool.drain();
  auto expected = beforeDrain;
  expected.push_back("pooled:1");
  expected.push_back("pooled:2");
  EXPECT_EQ(app.trace, expected);
  EXPECT_TRUE(app.pool.tasks.empty());
}

TEST(OperatorsTopology, SplitFunctionCallAsyncFlagChangesOnlyStableCallOrder) {
  // No task-pool or ParallelCall edge exists in this graph. The flag alone
  // must not schedule work or detach any branch from the invoking call.
  MixedSplitApp app{false};
  app.init();
  for (const int value : {1, 2}) {
    app.input->consume(
        servicelib::MessageContext{}.withStreamId("split-call-" + std::to_string(value)),
        servicelib::Payload<int>::make(value));
    app.trace.push_back("returned:" + std::to_string(value));
  }
  EXPECT_EQ(app.trace, (std::vector<std::string>{
      "first-priority:1", "direct-async:1", "first-sync:1", "last-sync:1", "returned:1",
      "first-priority:2", "direct-async:2", "first-sync:2", "last-sync:2", "returned:2"}));
  EXPECT_TRUE(app.pool.tasks.empty());
}

class TopologyApp final
    : public servicelib::StreamApp<TopologyApp, TopologyDataTypes> {
 public:
  void streamsInit() {
    servicelib::config::InputStreamConfig splitInputConfig;
    splitInputConfig.id = 1;
    splitInputConfig.name = "split-input";
    auto splitInput = servicelib::makeInputStream<
        int, std::monostate, int,
        TopologyApp::TStreamExecutionEnvironment>(splitInputConfig, nullptr,
                                                   *this);
    splitInput_ = splitInput.get();

    servicelib::config::SplitStreamConfig splitConfig;
    splitConfig.id = 2;
    splitConfig.name = "split";
    auto& split = splitInput->template split<2>(splitConfig);
    // Go SplitLink.GetSerde delegates to the parent; neither a branch nor a
    // same-type successor may replace or lose that serializer.
    ASSERT_NE(splitInput->getSerde(), nullptr);
    EXPECT_EQ(split.getSerde(), splitInput->getSerde());
    EXPECT_EQ(split.template get<0>().getSerde(), splitInput->getSerde());
    EXPECT_EQ(split.template get<1>().getSerde(), splitInput->getSerde());
    split.template get<0>().sink(
        sinkConfig(3, "split-left"), servicelib::StreamType<int>{},
        servicelib::StreamFunction(RecordValue{&splitLeft_}));
    servicelib::config::DelayStreamConfig delayConfig;
    delayConfig.id = 5;
    delayConfig.name = "split-right-delay";
    auto& delayed = split.template get<1>().delay(
        delayConfig,
        servicelib::StreamFunction(
            [](servicelib::MessageContext, servicelib::StreamBase&, const int&) {
              return std::chrono::milliseconds::zero();
            }));
    EXPECT_EQ(delayed.getSerde(), splitInput->getSerde());
    delayed.sink(
        sinkConfig(4, "split-right"), servicelib::StreamType<int>{},
        servicelib::StreamFunction(RecordValue{&splitRight_}));

    servicelib::config::InputStreamConfig caseInputConfig;
    caseInputConfig.id = 10;
    caseInputConfig.name = "case-input";
    auto caseInput = servicelib::makeInputStream<
        int, std::monostate, int,
        TopologyApp::TStreamExecutionEnvironment>(caseInputConfig, nullptr,
                                                   *this);
    caseInput_ = caseInput.get();

    servicelib::config::CaseStreamConfig caseConfig;
    caseConfig.id = 11;
    caseConfig.name = "case";
    auto& caseStream = caseInput->template case_<2>(
        caseConfig,
        servicelib::StreamFunction(
            [](servicelib::MessageContext, servicelib::StreamBase&,
               int& value) -> std::size_t {
              if (value == std::numeric_limits<int>::max()) return 2U;
              if (value == std::numeric_limits<int>::min()) return static_cast<std::size_t>(-1);
              return value >= 0 ? 0U : 1U;
            }));
    servicelib::config::WhenStreamConfig positiveConfig;
    positiveConfig.id = 12;
    positiveConfig.name = "positive";
    caseStream.template get<0>().configure(positiveConfig, nullptr, this);
    auto* positiveSerde = caseStream.template get<0>().getSerde();
    ASSERT_NE(positiveSerde, nullptr);
    EXPECT_EQ(positiveSerde->Deserialize(positiveSerde->Serialize(123)), 123);
    EXPECT_NE(positiveSerde, caseStream.getSerde());
    servicelib::config::WhenStreamConfig negativeConfig;
    negativeConfig.id = 13;
    negativeConfig.name = "negative";
    caseStream.template get<1>().configure(negativeConfig, nullptr, this);
    auto* negativeSerde = caseStream.template get<1>().getSerde();
    ASSERT_NE(negativeSerde, nullptr);
    EXPECT_EQ(negativeSerde->Deserialize(negativeSerde->Serialize(-456)), -456);
    EXPECT_NE(negativeSerde, caseStream.getSerde());
    caseStream.template get<0>().sink(
        sinkConfig(14, "positive-output"), servicelib::StreamType<int>{},
        servicelib::StreamFunction(RecordValue{&positive_}));
    caseStream.template get<1>().sink(
        sinkConfig(15, "negative-output"), servicelib::StreamType<int>{},
        servicelib::StreamFunction(RecordValue{&negative_}));

    servicelib::config::CycleLinkStreamConfig cycleConfig;
    cycleConfig.id = 20;
    cycleConfig.name = "cycle-link";
    cycle_ = servicelib::makeCycleLinkStream<int, TopologyApp>(
        cycleConfig, nullptr, *this);
    cycle_->sink(sinkConfig(21, "cycle-output"),
                 servicelib::StreamType<int>{},
                 servicelib::StreamFunction(RecordValue{&cycleValues_}));

    servicelib::config::InputStreamConfig cycleSourceConfig;
    cycleSourceConfig.id = 22;
    cycleSourceConfig.name = "cycle-source";
    cycleSource_ = servicelib::makeInputStream<
        int, std::monostate, int,
        TopologyApp::TStreamExecutionEnvironment>(cycleSourceConfig, nullptr,
                                                   *this);
    cycle_->setSource(*cycleSource_);
  }

  int start() { return 0; }

  void prepareTopology() { static_cast<void>(getExecutionRuntime<>()); }

  void delay(servicelib::Context, servicelib::pool::IDelayPool::Duration,
             std::function<void()> task) override {
    task();
  }

  void pushSplit(int value) {
    splitInput_->consume({}, servicelib::Payload<int>::make(value));
  }

  void pushCase(int value) {
    caseInput_->consume({}, servicelib::Payload<int>::make(value));
  }

  void pushCycle(int value) {
    cycleSource_->consume({}, servicelib::Payload<int>::make(value));
  }

  const std::vector<int>& splitLeft() const { return splitLeft_; }
  const std::vector<int>& splitRight() const { return splitRight_; }
  const std::vector<int>& positive() const { return positive_; }
  const std::vector<int>& negative() const { return negative_; }
  const std::vector<int>& cycleValues() const { return cycleValues_; }

 private:
  servicelib::Stream<int, servicelib::StreamConsumer<int>,
                     TopologyApp::TStreamExecutionEnvironment>* splitInput_{};
  servicelib::Stream<int, servicelib::StreamConsumer<int>,
                     TopologyApp::TStreamExecutionEnvironment>* caseInput_{};
  std::shared_ptr<servicelib::CycleLinkStream<int, TopologyApp>> cycle_;
  std::shared_ptr<servicelib::InputStream<
      int, std::monostate, int, TopologyApp::TStreamExecutionEnvironment>>
      cycleSource_;
  std::vector<int> splitLeft_;
  std::vector<int> splitRight_;
  std::vector<int> positive_;
  std::vector<int> negative_;
  std::vector<int> cycleValues_;
};

class ResultOwnershipApp final
    : public servicelib::StreamExecutionEnvironment<ResultOwnershipApp,
                                                     TopologyDataTypes> {
 public:
  using Input = servicelib::InputStream<int, int, int, ResultOwnershipApp>;

  void init() {
    servicelib::config::InputStreamConfig inputConfig;
    inputConfig.id = 101;
    inputConfig.name = "result-ownership-input";
    input_ = servicelib::makeInputStream<int, int, int, ResultOwnershipApp>(
        inputConfig, nullptr, *this);

    servicelib::config::MapStreamConfig mapConfig;
    mapConfig.id = 102;
    mapConfig.name = "result-ownership-map";
    auto& result = input_->map(
        mapConfig, servicelib::StreamType<int>{},
        servicelib::StreamFunction(
            [](servicelib::MessageContext context, servicelib::StreamBase&,
               int& value, auto&& output) {
              output.out(std::move(context), value);
            }));
    input_->setSource(result);
  }

  void startAndReleaseRuntime() {
    startExecutionRuntime();
    stopExecutionRuntime();
  }

  void releaseInput() { input_.reset(); }
  [[nodiscard]] const std::shared_ptr<Input>& input() const noexcept {
    return input_;
  }

  void delay(servicelib::Context, servicelib::pool::IDelayPool::Duration,
             std::function<void()> task) override {
    task();
  }

 private:
  std::shared_ptr<Input> input_;
};

struct SerdeInputProbe final { int value{}; };
struct SerdeOutputProbe final { int value{}; };
struct SerdePropagationTypes {
  template <typename> struct DataType {};
};

template <typename Result, bool Flat = false>
struct SerdePropagationMap final {
  int* calls;
  template <typename Output>
  void operator()(servicelib::MessageContext context, servicelib::StreamBase&,
                  SerdeInputProbe& value, Output&& output) const {
    ++*calls;
    output.out(context, Result{value.value + 1});
    if constexpr (Flat) output.out(std::move(context), Result{value.value + 2});
  }
};

template <typename Result, bool Flat = false>
class SerdePropagationApp final
    : public servicelib::StreamExecutionEnvironment<SerdePropagationApp<Result, Flat>,
                                                     SerdePropagationTypes> {
 public:
  using Entry = servicelib::SubStream<SerdeInputProbe, Result, SerdePropagationApp>;
  std::shared_ptr<Entry> entry;
  servicelib::StreamConsumer<SerdeInputProbe>* beforeMap{};
  servicelib::StreamConsumer<Result>* mapped{};
  servicelib::StreamConsumer<Result>* afterMap{};
  int mapCalls{};

  void init() {
    servicelib::config::SubStreamConfig rootConfig;
    rootConfig.id = 201;
    rootConfig.name = "serde-root";
    entry = servicelib::makeSubStream<SerdeInputProbe, Result, SerdePropagationApp>(
        rootConfig, *this);
    servicelib::config::FilterStreamConfig firstConfig;
    firstConfig.id = 202;
    firstConfig.name = "serde-before-map";
    auto& first = entry->filter(firstConfig, servicelib::StreamFunction(
        [](servicelib::MessageContext, servicelib::StreamBase&, SerdeInputProbe&) {
          return true;
        }));
    beforeMap = &first;
    auto& output = [&]() -> auto& {
      if constexpr (Flat) {
        servicelib::config::FlatMapStreamConfig mapConfig;
        mapConfig.id = 203;
        mapConfig.name = "serde-flatmap";
        return first.flatMap(mapConfig, servicelib::StreamType<Result>{},
            servicelib::StreamFunction((SerdePropagationMap<Result, true>{&mapCalls})));
      } else {
        servicelib::config::MapStreamConfig mapConfig;
        mapConfig.id = 203;
        mapConfig.name = "serde-map";
        return first.map(mapConfig, servicelib::StreamType<Result>{},
            servicelib::StreamFunction(SerdePropagationMap<Result>{&mapCalls}));
      }
    }();
    mapped = &output;
    servicelib::config::FilterStreamConfig lastConfig;
    lastConfig.id = 204;
    lastConfig.name = "serde-after-map";
    auto& last = output.filter(lastConfig, servicelib::StreamFunction(
        [](servicelib::MessageContext, servicelib::StreamBase&, Result&) {
          return true;
        }));
    afterMap = &last;
    entry->setSource(last);
  }
  void prepare() { static_cast<void>(this->template getExecutionRuntime<>()); }
  void delay(servicelib::Context, servicelib::pool::IDelayPool::Duration,
             std::function<void()> task) override { task(); }
};

template <typename Result, bool Flat = false>
void CheckSerdePropagationAcrossDirectCalls() {
  SerdePropagationApp<Result, Flat> app;
  app.init();
  auto* rootSerde = app.entry->getSerde();
  auto* outputSerde = app.mapped->getSerde();
  ASSERT_NE(rootSerde, nullptr);
  ASSERT_NE(outputSerde, nullptr);
  ASSERT_NE(rootSerde->ValueSerializer(), nullptr);
  ASSERT_NE(outputSerde->ValueSerializer(), nullptr);
  EXPECT_TRUE(rootSerde->ValueSerializer()->IsStub());
  EXPECT_TRUE(outputSerde->ValueSerializer()->IsStub());
  EXPECT_EQ(app.beforeMap->getSerde(), rootSerde);
  EXPECT_EQ(app.afterMap->getSerde(), outputSerde);
  EXPECT_NE(static_cast<const void*>(outputSerde), static_cast<const void*>(rootSerde));
  EXPECT_THROW(static_cast<void>(rootSerde->Serialize(SerdeInputProbe{1})),
               std::runtime_error);
  EXPECT_THROW(static_cast<void>(outputSerde->Serialize(Result{1})),
               std::runtime_error);

  app.prepare();
  EXPECT_EQ(app.entry->getSerde(), rootSerde);
  EXPECT_EQ(app.beforeMap->getSerde(), rootSerde);
  EXPECT_EQ(app.mapped->getSerde(), outputSerde);
  EXPECT_EQ(app.afterMap->getSerde(), outputSerde);
  int received = 0;
  for (const int value : {10, 20}) {
    int receivedInCall = 0;
    auto collector = std::make_shared<servicelib::SubStreamCollectorFunc<Result>>(
        [&](servicelib::MessageContext context, const Result& result) {
          EXPECT_EQ(context.streamId(), "serde-parent");
          EXPECT_EQ(result.value, value + 1 + receivedInCall);
          ++receivedInCall;
          ++received;
          return receivedInCall == (Flat ? 2 : 1);
        });
    EXPECT_NO_THROW(app.entry->consume(
        servicelib::MessageContext{}.withStreamId("serde-parent"),
        servicelib::Payload<SerdeInputProbe>::make(SerdeInputProbe{value}),
        std::move(collector)));
    EXPECT_EQ(receivedInCall, Flat ? 2 : 1);
  }
  EXPECT_EQ(received, Flat ? 4 : 2);
  EXPECT_EQ(app.mapCalls, 2);
}

TEST(OperatorsTopology, SameTypeMapResolvesOutputSerdeAndFiltersReuseTheirParents) {
  CheckSerdePropagationAcrossDirectCalls<SerdeInputProbe>();
}

TEST(OperatorsTopology, SameTypeFlatMapResolvesSerdeAndPreservesEmissionOrder) {
  CheckSerdePropagationAcrossDirectCalls<SerdeInputProbe, true>();
}

TEST(OperatorsTopology, TypeChangingFlatMapDoesNotSerializeDirectCallValues) {
  CheckSerdePropagationAcrossDirectCalls<SerdeOutputProbe, true>();
}

TEST(OperatorsTopology, TypeChangingMapDoesNotSerializeDirectCallValues) {
  CheckSerdePropagationAcrossDirectCalls<SerdeOutputProbe>();
}

TEST(OperatorsTopology, SplitBroadcastsAndCaseRoutesExactlyOneBranch) {
  auto& app = TopologyApp::createStreamApp();
  app.prepareTopology();

  app.pushSplit(7);
  EXPECT_EQ(app.splitLeft(), (std::vector<int>{7}));
  EXPECT_EQ(app.splitRight(), (std::vector<int>{7}));

  app.pushCase(3);
  app.pushCase(-4);
  EXPECT_EQ(app.positive(), (std::vector<int>{3}));
  EXPECT_EQ(app.negative(), (std::vector<int>{-4}));
#ifdef NDEBUG
  EXPECT_THROW(app.pushCase(std::numeric_limits<int>::max()), servicelib::StreamException);
  EXPECT_THROW(app.pushCase(std::numeric_limits<int>::min()), servicelib::StreamException);
#else
  // StreamException deliberately asserts before throwing in assertion-enabled
  // builds; verify that contract instead of trying to catch the assertion.
  EXPECT_DEATH(app.pushCase(std::numeric_limits<int>::max()), "StreamException");
  EXPECT_DEATH(app.pushCase(std::numeric_limits<int>::min()), "StreamException");
#endif
  EXPECT_EQ(app.positive(), (std::vector<int>{3}));
  EXPECT_EQ(app.negative(), (std::vector<int>{-4}));

  app.pushCycle(19);
  EXPECT_EQ(app.cycleValues(), (std::vector<int>{19}));
}

TEST(OperatorsTopology, ResultLinkDoesNotRetainReleasedInputGraph) {
  ResultOwnershipApp app;
  app.init();
  std::weak_ptr<ResultOwnershipApp::Input> input = app.input();

  app.startAndReleaseRuntime();
  app.releaseInput();

  EXPECT_TRUE(input.expired());
}

}  // namespace

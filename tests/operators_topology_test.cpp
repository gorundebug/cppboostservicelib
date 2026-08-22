#include <gtest/gtest.h>

#include <functional>
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
    split.template get<0>().sink(
        sinkConfig(3, "split-left"), servicelib::StreamType<int>{},
        servicelib::StreamFunction(RecordValue{&splitLeft_}));
    split.template get<1>().sink(
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
              return value >= 0 ? 0U : 1U;
            }));
    servicelib::config::WhenStreamConfig positiveConfig;
    positiveConfig.id = 12;
    positiveConfig.name = "positive";
    caseStream.template get<0>().configure(positiveConfig, nullptr, this);
    servicelib::config::WhenStreamConfig negativeConfig;
    negativeConfig.id = 13;
    negativeConfig.name = "negative";
    caseStream.template get<1>().configure(negativeConfig, nullptr, this);
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

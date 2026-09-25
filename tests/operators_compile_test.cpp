#include <gtest/gtest.h>

#include <chrono>
#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include <servicelib/runtime/caller.hpp>
#include <servicelib/runtime/testlog/testlog.hpp>
#include <servicelib/transformation/streams.hpp>

#include "test_async.hpp"

// This translation unit intentionally includes the complete public stream API.
// Most operators are templates, so compiling the header is the first parity
// check: an operator that is not reachable from Stream fails this target.

TEST(Operators, PublicApiHeadersCompileTogether) { SUCCEED(); }

namespace {

struct OperatorDataTypes final {
  template <typename>
  struct DataType {};
};

class OperatorApp final
    : public servicelib::StreamApp<OperatorApp, OperatorDataTypes> {
 public:
  void streamsInit() {}
  int start() { return 0; }
  void delay(servicelib::Context, servicelib::pool::IDelayPool::Duration,
             std::function<void()> task) override {
    task();
  }
  void parallel(std::function<void()> task) override { task(); }
};

OperatorApp& operatorApp() {
  static OperatorApp& app = OperatorApp::createStreamApp();
  return app;
}

servicelib::config::SinkStreamConfig sinkConfig(int id, std::string name) {
  servicelib::config::SinkStreamConfig config;
  config.id = id;
  config.name = std::move(name);
  return config;
}

template <typename T, typename R = std::monostate, typename E = int>
auto inputStream(OperatorApp& app, int id, std::string name) {
  servicelib::config::InputStreamConfig config;
  config.id = id;
  config.name = std::move(name);
  return servicelib::makeInputStream<T, R, E,
                                     OperatorApp::TStreamExecutionEnvironment>(
      config, nullptr, app);
}

template <typename Config>
Config operatorConfig(int id, std::string name) {
  Config config;
  config.id = id;
  config.name = std::move(name);
  return config;
}

servicelib::CallerBase::Params callerParams() {
  auto scope = servicelib::metrics::NoopMetrics::instance().scope("", {});
  return {.sourceName = "caller-source",
          .consumerName = "caller-consumer",
          .tracer = {},
          .messagesCounter = scope->counter("", "")};
}

class ImmediateTaskPool final : public servicelib::pool::ITaskPool {
 public:
  const std::string& getName() const noexcept override {
    ++nameReads;
    return name_;
  }
  mutable std::size_t nameReads{};
  int getExecutorsCount() const override { return 1; }
  void start(servicelib::Context) override {}
  void stop(servicelib::Context) override {}
  void addTask(servicelib::Context context,
               std::function<void()> task) override {
    lastCancelled = context.cancelled();
    task();
  }

  bool lastCancelled{};

 private:
  std::string name_{"task-pool"};
};

class ImmediatePriorityPool final
    : public servicelib::pool::IPriorityTaskPool {
 public:
  const std::string& getName() const noexcept override {
    ++nameReads;
    return name_;
  }
  mutable std::size_t nameReads{};
  int getExecutorsCount() const override { return 1; }
  void start(servicelib::Context) override {}
  void stop(servicelib::Context) override {}
  void addTask(servicelib::Context context, int priority,
               std::function<void()> task) override {
    lastCancelled = context.cancelled();
    lastPriority = priority;
    task();
  }

  int lastPriority{-1};
  bool lastCancelled{};

 private:
  std::string name_{"priority-pool"};
};

class MoveOnlyFilter final {
 public:
  explicit MoveOnlyFilter(std::unique_ptr<int> minimum)
      : minimum_(std::move(minimum)) {}
  MoveOnlyFilter(const MoveOnlyFilter&) = delete;
  MoveOnlyFilter& operator=(const MoveOnlyFilter&) = delete;
  MoveOnlyFilter(MoveOnlyFilter&&) = default;
  MoveOnlyFilter& operator=(MoveOnlyFilter&&) = default;

  bool operator()(servicelib::MessageContext, servicelib::StreamBase&,
                  const int& value) const {
    return value >= *minimum_;
  }

 private:
  std::unique_ptr<int> minimum_;
};

}  // namespace

TEST(Operators, GraphCanReferenceOneMoveOnlyFunctionWithoutCopyingIt) {
  auto& app = operatorApp();
  auto input = inputStream<int>(app, 170, "move-only-input");
  auto function = std::make_unique<MoveOnlyFilter>(std::make_unique<int>(2));
  auto& filtered = input->filter(
      operatorConfig<servicelib::config::FilterStreamConfig>(171,
                                                              "move-only"),
      servicelib::StreamFunction(std::ref(*function)));
  int observed = 0;
  filtered.sink(
      sinkConfig(172, "move-only-output"), servicelib::StreamType<int>{},
      servicelib::StreamFunction(
          [&observed](servicelib::MessageContext, const int& value) {
            observed += value;
          }));
  input->consume({}, servicelib::Payload<int>::make(1));
  input->consume({}, servicelib::Payload<int>::make(3));
  EXPECT_EQ(observed, 3);
}

TEST(Operators, SplitBranchesInheritRuntimeEnvironment) {
  auto& app = operatorApp();
  auto inputOwner = inputStream<int>(app, 30, "split-input");

  servicelib::config::SplitStreamConfig config;
  config.id = 31;
  config.name = "split";
  auto& split = inputOwner->template split<2>(config);

  EXPECT_EQ(split.getEnv(), &app);
  EXPECT_EQ(split.template get<0>().getEnv(), &app);
  EXPECT_EQ(split.template get<1>().getEnv(), &app);
}

TEST(Operators, RegisteredInputFeedsConfiguredTerminalSink) {
  auto& app = operatorApp();
  auto inputOwner = inputStream<int>(app, 40, "input");
  auto& input = *inputOwner;
  servicelib::config::SinkStreamConfig config;
  config.id = 41;
  config.name = "result-sink";
  config.idEndpoint = 17;

  int observed = 0;
  auto& sink =
      input.sink(config, servicelib::StreamType<int>{},
                 servicelib::StreamFunction(
                     [&observed](servicelib::MessageContext, const int& value) {
                       observed = value;
                     }));

  EXPECT_EQ(sink.getConfigId(), 41);
  EXPECT_EQ(sink.getEndpointId(), 17);
  input.consume(servicelib::MessageContext{},
                servicelib::Payload<int>::make(42));
  EXPECT_EQ(observed, 42);
}

TEST(Operators, ConfiguredInputOwnsEndpointResultAndErrorChannels) {
  auto& app = operatorApp();
  using Environment = OperatorApp::TStreamExecutionEnvironment;

  servicelib::config::InputStreamConfig config;
  config.id = 71;
  config.name = "http-input";
  config.idEndpoint = 29;
  auto inputOwner =
      servicelib::makeInputStream<int, std::string, int, Environment>(
          config, nullptr, app);
  auto& input = *inputOwner;

  int value = 0;
  input.sink(sinkConfig(72, "input-values"), servicelib::StreamType<int>{},
             servicelib::StreamFunction(
                 [&value](servicelib::MessageContext, const int& current) {
                   value = current;
                 }));

  int error = 0;
  input.getErrorStream().sink(
      sinkConfig(73, "input-errors"), servicelib::StreamType<int>{},
      servicelib::StreamFunction(
          [&error](servicelib::MessageContext, const int& current) {
            error = current;
          }));

  auto resultSourceOwner =
      inputStream<std::string>(app, 74, "http-result-source");
  auto& resultSource = *resultSourceOwner;
  input.setSource(resultSource);

  std::string result;
  input.setResultConsumer([&result](servicelib::MessageContext,
                                    servicelib::Payload<std::string> payload) {
    result = payload.get();
  });

  input.consume(servicelib::MessageContext{},
                servicelib::Payload<int>::make(11));
  input.consumeError(servicelib::MessageContext{},
                     servicelib::Payload<int>::make(7));
  resultSource.consume(servicelib::MessageContext{},
                       servicelib::Payload<std::string>::make("ok"));

  EXPECT_EQ(input.getConfigId(), 71);
  EXPECT_EQ(input.getEndpointId(), 29);
  EXPECT_EQ(input.getResultStream(), &resultSource);
  EXPECT_EQ(value, 11);
  EXPECT_EQ(error, 7);
  EXPECT_EQ(result, "ok");
}

TEST(Operators, ProcessExposesGoStyleErrorOutput) {
  auto& app = operatorApp();
  auto inputOwner = inputStream<long>(app, 50, "process-input");
  auto& input = *inputOwner;

  servicelib::config::ProcessStreamConfig processConfig;
  processConfig.id = 51;
  auto& process =
      input.process(processConfig, servicelib::StreamType<int>{},
                    servicelib::StreamType<int>{},
                    servicelib::StreamFunction(
                        [](servicelib::MessageContext context,
                           servicelib::StreamBase&, const long& value,
                           auto&& output, auto&& errors) {
                          if (value >= 0) {
                            output.out(context, static_cast<int>(value));
                          } else {
                            errors.out(context, static_cast<int>(-value));
                          }
                        }));

  int observed = 0;
  auto& sink = process.sink(
      sinkConfig(52, "process-output"), servicelib::StreamType<int>{},
      servicelib::StreamFunction(
          [&observed](servicelib::MessageContext, const int& value) {
            observed += value;
          }));
  process.getErrorStream().setConsumer(sink);

  input.consume(servicelib::MessageContext{},
                servicelib::Payload<long>::make(7));
  input.consume(servicelib::MessageContext{},
                servicelib::Payload<long>::make(-5));
  EXPECT_EQ(observed, 12);
}

TEST(Operators, SinkResultReentersTheStreamGraph) {
  auto& app = operatorApp();
  auto inputOwner = inputStream<short>(app, 60, "sink-input");
  auto& input = *inputOwner;

  servicelib::config::SinkStreamConfig config;
  config.id = 61;
  config.idEndpoint = 23;

  short request = 0;
  auto& sink = input.sinkWithResult(
      config, servicelib::StreamType<std::string>{},
      servicelib::StreamType<int>{},
      servicelib::StreamFunction(
          [&request](servicelib::MessageContext, const short& value) {
            request = value;
          }));

  std::string result;
  sink.sink(sinkConfig(62, "sink-result"), servicelib::StreamType<int>{},
            servicelib::StreamFunction(
                [&result](servicelib::MessageContext,
                          const std::string& value) { result = value; }));

  const auto context = servicelib::MessageContext{}.withStreamId("request-1");
  input.consume(context, servicelib::Payload<short>::make(9));
  sink.consumeResult(context, std::string{"done"});

  EXPECT_EQ(request, 9);
  EXPECT_EQ(result, "done");
  EXPECT_EQ(sink.getEndpointId(), 23);
}

TEST(Operators, DelayUsesRuntimeSchedulerAndPreservesMessageContext) {
  auto& app = operatorApp();
  auto inputOwner = inputStream<int>(app, 80, "delay-input");
  auto& input = *inputOwner;
  servicelib::config::DelayStreamConfig delayConfig;
  delayConfig.id = 81;

  bool sawDeadline = false;
  auto duration = [&sawDeadline](servicelib::MessageContext context,
                                 servicelib::StreamBase&, const int&) {
    sawDeadline = context.deadline().has_value();
    return std::chrono::milliseconds(1);
  };
  auto& delayed = input.delay(
      delayConfig,
      servicelib::make_function(std::move(duration), "context-aware-delay"));
  int observed = 0;
  delayed.sink(
      sinkConfig(82, "delayed-output"), servicelib::StreamType<int>{},
      servicelib::StreamFunction(
          [&observed](servicelib::MessageContext context, const int& value) {
            EXPECT_EQ(context.streamId(), "delayed-message");
            observed = value;
          }));

  input.consume(servicelib::MessageContext{}
                    .withStreamId("delayed-message")
                    .withDeadline(std::chrono::steady_clock::now() +
                                  std::chrono::seconds(1)),
                servicelib::Payload<int>::make(42));

  EXPECT_TRUE(sawDeadline);
  EXPECT_EQ(observed, 42);
}

TEST(Operators, MapCanEmitZeroOneOrManyValues) {
  auto& app = operatorApp();
  auto inputOwner = inputStream<int>(app, 90, "map-input");
  auto& mapped = inputOwner->map(
      operatorConfig<servicelib::config::MapStreamConfig>(91, "map"),
      servicelib::StreamType<int>{},
      servicelib::StreamFunction(
          [](servicelib::MessageContext context, servicelib::StreamBase&,
             int& value, auto&& output) {
            for (int index = 0; index < value; ++index) {
              output.out(context, index);
            }
          }));
  std::vector<int> observed;
  mapped.sink(
      sinkConfig(92, "map-output"), servicelib::StreamType<int>{},
      servicelib::StreamFunction(
          [&observed](servicelib::MessageContext, const int& value) {
            observed.push_back(value);
          }));

  inputOwner->consume({}, servicelib::Payload<int>::make(0));
  inputOwner->consume({}, servicelib::Payload<int>::make(3));
  EXPECT_EQ(observed, (std::vector<int>{0, 1, 2}));
}

TEST(Operators, FilterPassesOnlyMatchingValues) {
  auto& app = operatorApp();
  auto inputOwner = inputStream<int>(app, 100, "filter-input");
  auto& filtered = inputOwner->filter(
      operatorConfig<servicelib::config::FilterStreamConfig>(101, "filter"),
      servicelib::StreamFunction(
          [](servicelib::MessageContext, servicelib::StreamBase&, int& value) {
            return value >= 0;
          }));
  std::vector<int> observed;
  filtered.sink(
      sinkConfig(102, "filter-output"), servicelib::StreamType<int>{},
      servicelib::StreamFunction(
          [&observed](servicelib::MessageContext, const int& value) {
            observed.push_back(value);
          }));

  inputOwner->consume({}, servicelib::Payload<int>::make(-1));
  inputOwner->consume({}, servicelib::Payload<int>::make(0));
  inputOwner->consume({}, servicelib::Payload<int>::make(2));
  EXPECT_EQ(observed, (std::vector<int>{0, 2}));
}

TEST(Operators, FlatMapEmitsEveryProducedValue) {
  auto& app = operatorApp();
  auto inputOwner = inputStream<std::string>(app, 110, "flat-map-input");
  auto& flattened = inputOwner->flatMap(
      operatorConfig<servicelib::config::FlatMapStreamConfig>(111,
                                                              "flat-map"),
      servicelib::StreamType<char>{},
      servicelib::StreamFunction(
          [](servicelib::MessageContext context, servicelib::StreamBase&,
             std::string& value, auto&& output) {
            for (const char current : value) output.out(context, current);
          }));
  std::string observed;
  flattened.sink(
      sinkConfig(112, "flat-map-output"), servicelib::StreamType<int>{},
      servicelib::StreamFunction(
          [&observed](servicelib::MessageContext, const char& value) {
            observed.push_back(value);
          }));

  inputOwner->consume({}, servicelib::Payload<std::string>::make("abc"));
  EXPECT_EQ(observed, "abc");
}

TEST(Operators, FlatMapIterablePreservesElementOrder) {
  auto& app = operatorApp();
  auto inputOwner = inputStream<std::vector<int>>(app, 120, "iterable-input");
  auto& flattened = inputOwner->flatMapIterate(
      operatorConfig<servicelib::config::FlatMapIterableStreamConfig>(
          121, "flat-map-iterable"));
  std::vector<int> observed;
  flattened.sink(
      sinkConfig(122, "iterable-output"), servicelib::StreamType<int>{},
      servicelib::StreamFunction(
          [&observed](servicelib::MessageContext, const int& value) {
            observed.push_back(value);
          }));

  inputOwner->consume({},
                      servicelib::Payload<std::vector<int>>::make({3, 1, 2}));
  EXPECT_EQ(observed, (std::vector<int>{3, 1, 2}));
}

TEST(Operators, KeyByEmitsCanonicalKeyValue) {
  auto& app = operatorApp();
  auto inputOwner = inputStream<std::string>(app, 130, "key-by-input");
  auto& keyed = inputOwner->keyBy<std::string, std::size_t>(
      operatorConfig<servicelib::config::KeyByStreamConfig>(131, "key-by"),
      servicelib::StreamFunction(
          [](servicelib::MessageContext context, servicelib::StreamBase&,
             std::string& value, auto&& output) {
            output.out(context, servicelib::make_key_value(value, value.size()));
          }));
  std::vector<servicelib::KeyValueType<std::string, std::size_t>> observed;
  keyed.sink(
      sinkConfig(132, "key-by-output"), servicelib::StreamType<int>{},
      servicelib::StreamFunction(
          [&observed](servicelib::MessageContext,
                      const servicelib::KeyValueType<std::string,
                                                     std::size_t>& value) {
            observed.push_back(value);
          }));

  inputOwner->consume({}, servicelib::Payload<std::string>::make("key"));
  ASSERT_EQ(observed.size(), 1U);
  EXPECT_EQ(observed.front().first, "key");
  EXPECT_EQ(observed.front().second, 3U);
}

TEST(Operators, MergeForwardsEveryParentIntoOneOrderedOutput) {
  auto& app = operatorApp();
  auto firstOwner = inputStream<int>(app, 140, "merge-first");
  auto secondOwner = inputStream<int>(app, 141, "merge-second");
  auto& merged = firstOwner->merge(
      operatorConfig<servicelib::config::MergeStreamConfig>(142, "merge"),
      *secondOwner);
  std::vector<int> observed;
  merged.sink(
      sinkConfig(143, "merge-output"), servicelib::StreamType<int>{},
      servicelib::StreamFunction(
          [&observed](servicelib::MessageContext, const int& value) {
            observed.push_back(value);
          }));

  firstOwner->consume({}, servicelib::Payload<int>::make(1));
  secondOwner->consume({}, servicelib::Payload<int>::make(2));
  firstOwner->consume({}, servicelib::Payload<int>::make(3));
  EXPECT_EQ(observed, (std::vector<int>{1, 2, 3}));
}

TEST(Operators, MergeInheritsFirstParentSerdeRegardlessOfArrivalOrder) {
  auto& app = operatorApp();
  for (const bool reverse : {false, true}) {
    const int base = reverse ? 650 : 640;
    auto left = inputStream<int>(app, base, "serde-merge-left");
    auto right = inputStream<int>(app, base + 1, "serde-merge-right");
    ASSERT_NE(left->getSerde(), nullptr);
    ASSERT_NE(right->getSerde(), nullptr);
    ASSERT_NE(left->getSerde(), right->getSerde());
    auto& first = reverse ? *right : *left;
    auto& second = reverse ? *left : *right;
    auto& merged = first.merge(
        operatorConfig<servicelib::config::MergeStreamConfig>(base + 2, "serde-merge"),
        second);
    EXPECT_EQ(merged.getSerde(), first.getSerde());
    EXPECT_NE(merged.getSerde(), second.getSerde());
    std::vector<int> observed;
    merged.sink(
        sinkConfig(base + 3, "serde-merge-output"), servicelib::StreamType<int>{},
        servicelib::StreamFunction([&observed](servicelib::MessageContext context,
                                             const int& value) {
          EXPECT_EQ(context.streamId(), "merge-correlation");
          observed.push_back(value);
        }));
    const auto context = servicelib::MessageContext{}.withStreamId("merge-correlation");
    second.consume(context, servicelib::Payload<int>::make(7));
    EXPECT_EQ(observed, (std::vector<int>{7}));
    first.consume(context, servicelib::Payload<int>::make(8));
    EXPECT_EQ(observed, (std::vector<int>{7, 8}));
    EXPECT_EQ(merged.getSerde(), first.getSerde());
  }
}

namespace {
class IsolatedSplitMergeApp final
    : public servicelib::StreamExecutionEnvironment<IsolatedSplitMergeApp, OperatorDataTypes> {
 public:
  void delay(servicelib::Context, servicelib::pool::IDelayPool::Duration,
             std::function<void()> task) override { task(); }
  void prepare() { static_cast<void>(getExecutionRuntime<>()); }
};
}  // namespace

TEST(Operators, SplitMergeDeliversBothBranchesBeforeReturning) {
  IsolatedSplitMergeApp app;
  auto input = servicelib::makeInputStream<int, std::monostate, int, IsolatedSplitMergeApp>(
      operatorConfig<servicelib::config::InputStreamConfig>(660, "split-merge-input"),
      nullptr, app);
  auto& split = input->split<2>(
      operatorConfig<servicelib::config::SplitStreamConfig>(661, "split-merge-split"));
  std::vector<std::string> trace;
  auto record = [&trace](std::string label) {
    return [&trace, label = std::move(label)](
               servicelib::MessageContext context, servicelib::StreamBase&, const int&) {
      EXPECT_EQ(context.streamId(), "split-merge-correlation");
      trace.push_back(label);
      return true;
    };
  };
  auto& left = split.get<0>().filter(
      operatorConfig<servicelib::config::FilterStreamConfig>(662, "split-merge-left"),
      servicelib::StreamFunction(record("left")));
  auto& right = split.get<1>().filter(
      operatorConfig<servicelib::config::FilterStreamConfig>(663, "split-merge-right"),
      servicelib::StreamFunction(record("right")));
  auto& merged = right.merge(
      operatorConfig<servicelib::config::MergeStreamConfig>(664, "split-merge-merge"), left);
  EXPECT_EQ(split.getSerde(), input->getSerde());
  EXPECT_EQ(left.getSerde(), input->getSerde());
  EXPECT_EQ(right.getSerde(), input->getSerde());
  EXPECT_EQ(merged.getSerde(), right.getSerde());
  std::vector<int> observed;
  merged.sink(
      sinkConfig(665, "split-merge-output"), servicelib::StreamType<int>{},
      servicelib::StreamFunction(([&trace, &observed](servicelib::MessageContext context,
                                                   const int& value) {
        EXPECT_EQ(context.streamId(), "split-merge-correlation");
        trace.push_back("result");
        observed.push_back(value);
      })));
  const auto context = servicelib::MessageContext{}.withStreamId("split-merge-correlation");
  app.prepare();
  input->consume(context, servicelib::Payload<int>::make(7));
  input->consume(context, servicelib::Payload<int>::make(8));
  EXPECT_EQ(observed, (std::vector<int>{7, 7, 8, 8}));
  EXPECT_EQ(trace, (std::vector<std::string>{
      "left", "result", "right", "result", "left", "result", "right", "result"}));
}

namespace {

class DeferredMergePool final : public servicelib::pool::ITaskPool {
 public:
  const std::string& getName() const noexcept override { return name_; }
  int getExecutorsCount() const override { return 1; }
  void start(servicelib::Context) override {}
  void stop(servicelib::Context) override {}
  void addTask(servicelib::Context, std::function<void()> task) override {
    tasks.push_back(std::move(task));
  }
  void drain() {
    auto admitted = std::move(tasks);
    tasks.clear();
    for (auto& task : admitted) task();
  }
  std::vector<std::function<void()>> tasks;

 private:
  const std::string name_{"deferred-merge"};
};

}  // namespace

TEST(Operators, MergeDoesNotWaitForQueuedParentAndPreservesCorrelation) {
  auto& app = operatorApp();
  auto direct = inputStream<int>(app, 180, "merge-direct");
  auto deferred = inputStream<int>(app, 181, "merge-deferred");
  auto& merged = direct->merge(
      operatorConfig<servicelib::config::MergeStreamConfig>(182, "mixed-merge"),
      *deferred);
  std::vector<std::pair<int, std::string>> observed;
  merged.sink(
      sinkConfig(183, "mixed-merge-output"), servicelib::StreamType<int>{},
      servicelib::StreamFunction(
          [&observed](servicelib::MessageContext context, const int& value) {
            observed.emplace_back(value, std::string{context.streamId()});
          }));
  DeferredMergePool pool;
  servicelib::TaskPoolCaller<int> deferredCaller{
      *deferred, pool, app.getLogger(), callerParams()};
  servicelib::DirectCaller<int> directCaller{*direct, callerParams(), true};
  const auto context = servicelib::MessageContext{}.withStreamId("mixed-call");
  deferredCaller.consume(context, servicelib::Payload<int>::make(2));
  EXPECT_TRUE(observed.empty());
  ASSERT_EQ(pool.tasks.size(), 1U);
  directCaller.consume(context, servicelib::Payload<int>::make(1));
  EXPECT_TRUE(directCaller.isAsync());
  EXPECT_EQ(observed,
            (std::vector<std::pair<int, std::string>>{{1, "mixed-call"}}));
  pool.drain();
  EXPECT_EQ(observed,
            (std::vector<std::pair<int, std::string>>{
                {1, "mixed-call"}, {2, "mixed-call"}}));
}

TEST(Operators, MergeDoesNotSerializeIndependentParentCalls) {
  auto& app = operatorApp();
  auto first = inputStream<int>(app, 184, "concurrent-merge-first");
  auto second = inputStream<int>(app, 185, "concurrent-merge-second");
  auto& merged = first->merge(
      operatorConfig<servicelib::config::MergeStreamConfig>(186, "concurrent-merge"),
      *second);
  std::atomic<int> entered{0};
  std::atomic<int> total{0};
  test_async::Event bothEntered;
  test_async::Event release;
  merged.sink(
      sinkConfig(187, "concurrent-merge-output"), servicelib::StreamType<int>{},
      servicelib::StreamFunction(
          ([&entered, &total, &bothEntered, &release](
              servicelib::MessageContext context, const int& value) {
            EXPECT_EQ(context.streamId(), value == 1 ? "first-call" : "second-call");
            if (entered.fetch_add(1) == 1) bothEntered.Send();
            EXPECT_TRUE(release.WaitForEvent());
            total.fetch_add(value);
          })));
  std::thread firstCall([&] {
    first->consume(servicelib::MessageContext{}.withStreamId("first-call"),
                   servicelib::Payload<int>::make(1));
  });
  std::thread secondCall([&] {
    second->consume(servicelib::MessageContext{}.withStreamId("second-call"),
                    servicelib::Payload<int>::make(2));
  });
  EXPECT_TRUE(bothEntered.WaitForEvent());
  release.Send();
  firstCall.join();
  secondCall.join();
  EXPECT_EQ(entered.load(), 2);
  EXPECT_EQ(total.load(), 3);
}

TEST(Operators, CallerSemanticsDispatchPreserveContextPriorityAndStatistics) {
  test_async::AsioRuntime runtime;
  auto& app = operatorApp();
  auto inputOwner = inputStream<int>(app, 150, "caller-input");
  struct DeliveryState final {
    std::mutex mutex;
    std::condition_variable delivered;
    std::vector<std::pair<int, std::string>> observed;
  } state;
  auto& sink = inputOwner->sink(
      sinkConfig(151, "caller-output"), servicelib::StreamType<int>{},
      servicelib::StreamFunction(
          [&state](servicelib::MessageContext context, const int& value) {
            {
              std::lock_guard lock(state.mutex);
              state.observed.emplace_back(value, context.streamId());
            }
            state.delivered.notify_all();
          }));

  servicelib::DirectCaller<int> direct{sink, callerParams(), false};
  direct.consume(servicelib::MessageContext{}.withStreamId("direct"),
                 servicelib::Payload<int>::make(1));
  EXPECT_FALSE(direct.isAsync());
  EXPECT_EQ(direct.statistics().count(), 1);

  servicelib::DirectCaller<int> metadataAsync{sink, callerParams(), true};
  metadataAsync.consume(
      servicelib::MessageContext{}.withStreamId("function-async"),
      servicelib::Payload<int>::make(2));
  EXPECT_TRUE(metadataAsync.isAsync());

  ImmediateTaskPool taskPool;
  servicelib::testlog::TestLog logger;
  servicelib::TaskPoolCaller<int> task{sink, taskPool, logger, callerParams()};
  task.consume(servicelib::MessageContext{}.withStreamId("task"),
               servicelib::Payload<int>::make(3));
  EXPECT_TRUE(task.isAsync());
  EXPECT_FALSE(taskPool.lastCancelled);
  EXPECT_EQ(task.statistics().count(), 1);
  EXPECT_EQ(taskPool.nameReads, 0);

  ImmediatePriorityPool priorityPool;
  servicelib::PriorityTaskPoolCaller<int> priority{
      sink, priorityPool, 17, logger, callerParams()};
  priority.consume(
      servicelib::MessageContext{}.withStreamId("priority-default"),
      servicelib::Payload<int>::make(4));
  EXPECT_EQ(priorityPool.lastPriority, 17);
  priority.consume(servicelib::MessageContext{}
                       .withStreamId("priority-context")
                       .withPriority(0),
                   servicelib::Payload<int>::make(5));
  EXPECT_EQ(priorityPool.lastPriority, 0);
  EXPECT_EQ(priority.statistics().count(), 2);
  EXPECT_EQ(priorityPool.nameReads, 0);

  servicelib::ParallelCaller<int> parallel{sink, app, callerParams()};
  parallel.consume(servicelib::MessageContext{}.withStreamId("parallel"),
                   servicelib::Payload<int>::make(6));
  EXPECT_TRUE(parallel.isAsync());
  EXPECT_EQ(parallel.statistics().count(), 1);

  std::unique_lock lock(state.mutex);
  ASSERT_TRUE(state.delivered.wait_for(
      lock, std::chrono::seconds{5}, [&state] {
        return state.observed.size() == 6;
      }));
  EXPECT_EQ(state.observed,
            (std::vector<std::pair<int, std::string>>{
                {1, "direct"}, {2, "function-async"}, {3, "task"},
                {4, "priority-default"}, {5, "priority-context"},
                {6, "parallel"}}));
}

TEST(Operators, DirectCallerDoesNotQueueWhileCompletionIsPending) {
  auto& app = operatorApp();
  auto inputOwner = inputStream<int>(app, 160, "completion-input");
  std::vector<int> observed;
  std::vector<std::shared_ptr<servicelib::AsyncCompletionToken>> completions;
  auto& sink = inputOwner->sink(
      sinkConfig(161, "completion-output"), servicelib::StreamType<int>{},
      servicelib::make_function(
          [&observed, &completions](servicelib::MessageContext context,
                                    const int& value) {
            observed.push_back(value);
            completions.push_back(context.retainCompletion());
          },
          "deferred-completion"));

  servicelib::DirectCaller<int> direct{sink, callerParams(), false};
  int completed = 0;
  auto parent = servicelib::AsyncCompletionState::make([&completed] { ++completed; });
  const auto context = servicelib::MessageContext{}
                           .withStreamId("request-a")
                           .withCompletion(parent);
  direct.consume(context, servicelib::Payload<int>::make(1));
  direct.consume(context, servicelib::Payload<int>::make(2));
  direct.consume(servicelib::MessageContext{}.withStreamId("request-b"),
                 servicelib::Payload<int>::make(3));

  EXPECT_EQ(observed, (std::vector<int>{1, 2, 3}));
  ASSERT_EQ(completions.size(), 3U);
  EXPECT_TRUE(completions[0]);
  EXPECT_TRUE(completions[1]);
  EXPECT_FALSE(completions[2]);
  parent->release();
  EXPECT_EQ(completed, 0);
  completions.front().reset();
  EXPECT_EQ(completed, 0);
  EXPECT_EQ(observed, (std::vector<int>{1, 2, 3}));
  completions.clear();
  EXPECT_EQ(completed, 1);
  EXPECT_EQ(observed, (std::vector<int>{1, 2, 3}));
}

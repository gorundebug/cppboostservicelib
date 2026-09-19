#include <gtest/gtest.h>
#include <atomic>
#include <future>
#include <thread>
#include <servicelib/transformation/streams.hpp>

namespace {
using namespace servicelib;
using namespace std::chrono_literals;
struct Types { template <typename> struct DataType {}; };
struct Work {
  std::function<int(MessageContext, int)>* function;
  template <typename Output>
  void operator()(MessageContext context, StreamBase&, int& value, Output&& output) const {
    const auto mapped = *function ? (*function)(context, value) : value * 2;
    if (mapped < 0) return;
    output.out(context, mapped);
    output.out(context, mapped + 1);
  }
};
class App final : public StreamExecutionEnvironment<App, Types> {
 public:
  using Entry = SubStream<int, int, App>;
  std::shared_ptr<Entry> entry;
  std::function<int(MessageContext, int)> function;
  void init() {
    config::SubStreamConfig cfg;
    cfg.id = 101; cfg.name = "lookup";
    entry = makeSubStream<int, int, App>(cfg, *this);
    config::MapStreamConfig map;
    map.id = 102; map.name = "work";
    auto& result = entry->map(map, StreamType<int>{}, StreamFunction(Work{&function}));
    entry->setSource(result);
  }
  void prepare() { static_cast<void>(getExecutionRuntime<>()); }
  void delay(Context, pool::IDelayPool::Duration, std::function<void()> task) override { task(); }
};
std::shared_ptr<SubStreamCollector<int>> collect(SubStreamCollectorFunc<int>::Function function) {
  return std::make_shared<SubStreamCollectorFunc<int>>(std::move(function));
}

TEST(SubStream, ConcurrentCallsShareGraphAndStreamId) {
  App app; app.init(); app.prepare();
  auto context = MessageContext{}.withStreamId("same-parent");
  std::vector<std::future<void>> calls;
  for (int i = 0; i < 100; ++i) {
    calls.push_back(std::async(std::launch::async, [&, i] {
      int count = 0;
      app.entry->consume(context, Payload<int>::make(i), collect([&](MessageContext caller, const int& value) {
        EXPECT_EQ(caller.streamId(), "same-parent"); EXPECT_EQ(value, i * 2); ++count; return true;
      }));
      EXPECT_EQ(count, 1);
    }));
  }
  for (auto& call : calls) call.get();
}

TEST(SubStream, FalseKeepsWaitingAndNestedBodyRestoresOuterCall) {
  App app; app.init(); app.prepare();
  app.function = [&](MessageContext context, int depth) {
    if (depth == 0) return 0;
    int result = -1;
    app.entry->consume(context, Payload<int>::make(depth - 1), collect([&](MessageContext, const int& value) {
      result = value + 10; return true;
    }));
    return result;
  };
  std::vector<int> results;
  app.entry->consume({}, Payload<int>::make(5), collect([&](MessageContext, const int& value) {
    results.push_back(value); return results.size() == 2;
  }));
  EXPECT_EQ(results, (std::vector<int>{50, 51}));
}

TEST(SubStream, CollectorCanCallSameEntry) {
  App app; app.init(); app.prepare();
  int result = 0;
  app.entry->consume({}, Payload<int>::make(3), collect([&](MessageContext context, const int& value) {
    app.entry->consume(context, Payload<int>::make(value), collect([&](MessageContext, const int& nested) {
      result = nested; return true;
    }));
    return true;
  }));
  EXPECT_EQ(result, 12);
}

TEST(SubStream, CancellationClearsRetainedContextAndDoesNotCancelSibling) {
  App app; app.init(); app.prepare();
  std::promise<MessageContext> held;
  app.function = [&](MessageContext context, int value) {
    if (value < 0) { held.set_value(context); return -1; }
    return value;
  };
  std::stop_source stop;
  auto callback = collect([](MessageContext, const int&) { ADD_FAILURE(); return true; });
  std::weak_ptr<SubStreamCollector<int>> weak = callback;
  auto call = std::async(std::launch::async, [&, callback = std::move(callback)]() mutable {
    EXPECT_THROW(app.entry->consume(MessageContext{}.withStopToken(stop.get_token()), Payload<int>::make(-1), std::move(callback)), std::runtime_error);
  });
  auto retained = held.get_future().get();
  app.entry->consume({}, Payload<int>::make(7), collect([](MessageContext, const int& value) { EXPECT_EQ(value, 7); return true; }));
  stop.request_stop(); call.get();
  EXPECT_TRUE(weak.expired());
  EXPECT_FALSE(retained.streamId().size());
}

TEST(SubStream, DeadlineAndExternalCancellation) {
  App app; app.init(); app.prepare();
  app.function = [](MessageContext, int) { return -1; };
  auto callback = collect([](MessageContext, const int&) { return true; });
  EXPECT_THROW(app.entry->consume(MessageContext{}.withDeadline(std::chrono::steady_clock::now() + 5ms), Payload<int>::make(1), callback), std::runtime_error);
  std::stop_source stop;
  auto pending = std::async(std::launch::async, [&] {
    EXPECT_THROW(app.entry->consume(MessageContext{}.withExternalCancellation(stop.get_token()), Payload<int>::make(1), callback), std::runtime_error);
  });
  stop.request_stop(); pending.get();
}

TEST(SubStream, CancellationDrainsActiveCollector) {
  App app; app.init(); app.prepare();
  std::promise<void> entered, release;
  auto released = release.get_future().share();
  std::stop_source stop;
  auto pending = std::async(std::launch::async, [&] {
    EXPECT_THROW(app.entry->consume(MessageContext{}.withStopToken(stop.get_token()), Payload<int>::make(1), collect([&](MessageContext, const int&) {
      entered.set_value(); released.wait(); return true;
    })), std::runtime_error);
  });
  entered.get_future().wait(); stop.request_stop();
  EXPECT_EQ(pending.wait_for(5ms), std::future_status::timeout);
  release.set_value(); pending.get();
}

TEST(SubStream, CollectorExceptionPropagatesAndClosesCall) {
  App app; app.init(); app.prepare();
  int count = 0;
  EXPECT_THROW(app.entry->consume({}, Payload<int>::make(1), collect([&](MessageContext, const int&) -> bool {
    ++count; throw std::runtime_error("collector failure");
  })), std::runtime_error);
  EXPECT_EQ(count, 1);
}

TEST(SubStream, TypedContextKeysRemainLocalAndNested) {
  ContextKey<int> first, second;
  auto outer = MessageContext{}.withLocalValue(first, std::make_shared<int>(1));
  auto inner = outer.withLocalValue(first, std::make_shared<int>(2)).withLocalValue(second, std::make_shared<int>(3));
  EXPECT_EQ(*outer.localValue(first), 1); EXPECT_EQ(*inner.localValue(first), 2);
  EXPECT_EQ(*inner.withPriority(5).localValue(second), 3);
  EXPECT_FALSE(MessageContext{}.withStreamId(std::string(inner.streamId())).localValue(first));
}
}  // namespace

TEST(SubStreamCall, ConcurrentResultsSerializeCollectorAndDropLateValues) {
  std::atomic<int> active{0}, maximum{0}, received{0};
  auto callback = std::make_shared<servicelib::SubStreamCollectorFunc<int>>(
      [&](servicelib::MessageContext, const int&) {
        const int count = active.fetch_add(1) + 1;
        maximum.store(std::max(maximum.load(), count));
        std::this_thread::yield();
        active.fetch_sub(1);
        return received.fetch_add(1) + 1 == 8;
      });
  auto call = std::make_shared<servicelib::detail::SubStreamCall<int>>(
      servicelib::MessageContext{}, callback);
  std::vector<std::thread> producers;
  for (int i = 0; i < 8; ++i) producers.emplace_back([call, i] { call->deliver(i); });
  for (auto& producer : producers) producer.join();
  call->wait({});
  call->deliver(100);
  EXPECT_EQ(received.load(), 8);
  EXPECT_EQ(maximum.load(), 1);
}

TEST(SubStreamCall, ClosedInvocationReleasesCallbackAndIgnoresLateResults) {
  int received = 0;
  auto callback = std::make_shared<servicelib::SubStreamCollectorFunc<int>>(
      [&](servicelib::MessageContext, const int&) { ++received; return false; });
  std::weak_ptr<servicelib::SubStreamCollector<int>> weak = callback;
  auto call = std::make_shared<servicelib::detail::SubStreamCall<int>>(
      servicelib::MessageContext{}, std::move(callback));
  call->close();
  EXPECT_THROW(call->wait({}), std::runtime_error);
  call->deliver(1);
  EXPECT_TRUE(weak.expired());
  EXPECT_EQ(received, 0);
}

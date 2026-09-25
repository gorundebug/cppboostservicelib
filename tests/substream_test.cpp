#include <gtest/gtest.h>
#include <atomic>
#include <future>
#include <thread>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
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

TEST(SubStream, ConcurrentNestedCallsKeepResultsIsolated) {
  App app; app.init(); app.prepare();
  ContextKey<int> key;
  const auto context = MessageContext{}.withStreamId("shared-parent")
      .withLocalValue(key, std::make_shared<int>(77));
  app.function = [&](MessageContext current, int value) {
    EXPECT_EQ(current.streamId(), "shared-parent");
    EXPECT_EQ(*current.localValue(key), 77);
    if (value < 1000) return value * 2;
    int result = -1;
    app.entry->consume(current, Payload<int>::make(value - 1000),
        collect([&](MessageContext caller, const int& nested) {
          EXPECT_EQ(caller.streamId(), "shared-parent");
          EXPECT_EQ(*caller.localValue(key), 77);
          result = nested + 100;
          return true;
        }));
    return result;
  };
  std::vector<std::future<void>> calls;
  for (int value = 0; value < 32; ++value) {
    calls.push_back(std::async(std::launch::async, [&, value] {
      int count = 0;
      app.entry->consume(context, Payload<int>::make(2000 + value),
          collect([&](MessageContext caller, const int& result) {
            EXPECT_EQ(caller.streamId(), "shared-parent");
            EXPECT_EQ(*caller.localValue(key), 77);
            EXPECT_EQ(result, value * 2 + 200);
            ++count;
            return true;
          }));
      EXPECT_EQ(count, 1);
    }));
  }
  for (auto& call : calls) call.get();
}

TEST(SubStream, WaitingInvocationDoesNotBlockSiblingOnOneWorker) {
  App app; app.init(); app.prepare();
  boost::asio::io_context io;
  detail::SingleUseEvent waiting;
  std::stop_source stop;
  app.function = [&](MessageContext, int value) {
    if (value == 0) { waiting.Send(); return -1; }
    return value;
  };
  bool siblingCompleted = false;
  auto blocked = boost::asio::co_spawn(io, detail::CooperativeExecution::Run([&] {
    EXPECT_THROW(app.entry->consume(
        MessageContext{}.withStreamId("same-parent").withStopToken(stop.get_token()),
        Payload<int>::make(0), collect([](MessageContext, const int&) {
          ADD_FAILURE(); return true;
        })), std::runtime_error);
    EXPECT_TRUE(siblingCompleted);
  }), boost::asio::use_future);
  auto sibling = boost::asio::co_spawn(io, detail::CooperativeExecution::Run([&] {
    waiting.Wait();
    app.entry->consume(MessageContext{}.withStreamId("same-parent"),
        Payload<int>::make(7), collect([&](MessageContext, const int& value) {
          EXPECT_EQ(value, 7); siblingCompleted = true; return true;
        }));
    stop.request_stop();
  }), boost::asio::use_future);
  io.run();
  blocked.get(); sibling.get();
}

TEST(SubStream, ConcurrentNestedInvocationsCanSuspendOnOneWorker) {
  App app; app.init(); app.prepare();
  boost::asio::io_context io;
  detail::SingleUseEvent allWaiting, release;
  int waiting = 0;
  app.function = [&](MessageContext context, int value) {
    if (value >= 1000) {
      int nested = -1;
      app.entry->consume(context, Payload<int>::make(value - 1000),
          collect([&](MessageContext, const int& result) {
            nested = result; return true;
          }));
      return nested + 10;
    }
    if (++waiting == 100) allWaiting.Send();
    release.Wait();
    return value * 2;
  };
  std::vector<std::future<void>> calls;
  for (int value = 0; value < 100; ++value) {
    calls.push_back(boost::asio::co_spawn(io, detail::CooperativeExecution::Run([&, value] {
      int received = 0;
      app.entry->consume(MessageContext{}.withStreamId("same-parent"),
          Payload<int>::make(1000 + value), collect([&](MessageContext context, const int& result) {
            EXPECT_EQ(context.streamId(), "same-parent");
            EXPECT_EQ(result, value * 2 + 10); ++received; return true;
          }));
      EXPECT_EQ(received, 1);
    }), boost::asio::use_future));
  }
  auto releaser = boost::asio::co_spawn(io, detail::CooperativeExecution::Run([&] {
    allWaiting.Wait(); release.Send();
  }), boost::asio::use_future);
  io.run();
  for (auto& call : calls) call.get();
  releaser.get();
}

TEST(SubStream, CancellationDrainsSuspendedCollectorOnOneWorker) {
  App app; app.init(); app.prepare();
  boost::asio::io_context io;
  detail::SingleUseEvent entered, release;
  std::stop_source stop;
  bool exited = false;
  auto call = boost::asio::co_spawn(io, detail::CooperativeExecution::Run([&] {
    EXPECT_THROW(app.entry->consume(MessageContext{}.withStopToken(stop.get_token()),
        Payload<int>::make(1), collect([&](MessageContext, const int&) {
          entered.Send(); release.Wait(); exited = true; return false;
        })), std::runtime_error);
    EXPECT_TRUE(exited);
  }), boost::asio::use_future);
  auto canceller = boost::asio::co_spawn(io, detail::CooperativeExecution::Run([&] {
    entered.Wait(); stop.request_stop(); EXPECT_FALSE(exited); release.Send();
  }), boost::asio::use_future);
  io.run();
  call.get(); canceller.get();
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

TEST(SubStream, CancelledContextPrecedesCollectorAndTopologyValidation) {
  App app; app.init(); app.prepare();
  std::stop_source stop;
  stop.request_stop();
  const auto context = MessageContext{}.withStopToken(stop.get_token());
  EXPECT_THROW(app.entry->consume(context, Payload<int>::make(1), nullptr),
               std::runtime_error);
  config::SubStreamConfig missingConfig;
  missingConfig.id = 103;
  missingConfig.name = "missing-body";
  auto missing = makeSubStream<int, int, App>(missingConfig, app);
  auto callback = collect([](MessageContext, const int&) {
    ADD_FAILURE() << "cancelled invocation reached collector";
    return true;
  });
  EXPECT_THROW(missing->consume(context, Payload<int>::make(1), callback),
               std::runtime_error);
  EXPECT_THROW(app.entry->consume({}, Payload<int>::make(1), nullptr),
               std::invalid_argument);
  EXPECT_THROW(missing->consume({}, Payload<int>::make(1), callback),
               std::logic_error);
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
  for (const bool completed : {false, true}) {
  SCOPED_TRACE(completed);
  App app; app.init(); app.prepare();
  std::promise<void> entered, release;
  auto released = release.get_future().share();
  std::stop_source stop;
  auto pending = std::async(std::launch::async, [&] {
    auto consume = [&] {
      app.entry->consume(MessageContext{}.withStopToken(stop.get_token()), Payload<int>::make(1), collect([&](MessageContext, const int&) {
        entered.set_value(); released.wait(); return completed;
      }));
    };
    if (completed) {
      EXPECT_NO_THROW(consume());
    } else {
      EXPECT_THROW(consume(), std::runtime_error);
    }
  });
  entered.get_future().wait(); stop.request_stop();
  EXPECT_EQ(pending.wait_for(5ms), std::future_status::timeout);
  release.set_value(); pending.get();
  }
}

TEST(SubStream, CollectorCompletionWinsItsOwnCancellation) {
  for (const bool completed : {false, true}) {
    SCOPED_TRACE(completed);
    App app; app.init(); app.prepare();
    std::stop_source stop;
    int called = 0;
    auto consume = [&] {
      app.entry->consume(MessageContext{}.withStopToken(stop.get_token()), Payload<int>::make(1), collect([&](MessageContext, const int&) {
        ++called;
        stop.request_stop();
        return completed;
      }));
    };
    if (completed) {
      EXPECT_NO_THROW(consume());
    } else {
      EXPECT_THROW(consume(), std::runtime_error);
    }
    EXPECT_EQ(called, 1);
  }
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

TEST(SubStream, ExternalCancellationAfterAdmissionReleasesOnlyItsInvocation) {
  App app; app.init(); app.prepare();
  boost::asio::io_context io;
  detail::SingleUseEvent entered;
  std::stop_source stop;
  std::optional<MessageContext> retained;
  app.function = [&](MessageContext context, int value) {
    if (value == 0) {
      retained = std::move(context);
      entered.Send();
      return -1;
    }
    return value;
  };
  auto callback = collect([](MessageContext, const int&) {
    ADD_FAILURE() << "cancelled invocation produced a result";
    return true;
  });
  std::weak_ptr<SubStreamCollector<int>> weak = callback;
  bool returned = false;
  bool siblingCompleted = false;
  const auto parent = MessageContext{}.withStreamId("shared-parent");
  auto pending = boost::asio::co_spawn(io,
      detail::CooperativeExecution::Run([&, callback = std::move(callback)]() mutable {
        EXPECT_THROW(app.entry->consume(
            parent.withExternalCancellation(stop.get_token()),
            Payload<int>::make(0), std::move(callback)), std::runtime_error);
        returned = true;
        EXPECT_TRUE(weak.expired());
      }), boost::asio::use_future);
  auto canceller = boost::asio::co_spawn(io,
      detail::CooperativeExecution::Run([&] {
        entered.Wait();
        EXPECT_FALSE(returned);
        EXPECT_TRUE(retained.has_value());
        EXPECT_TRUE(stop.request_stop());
        app.entry->consume(parent, Payload<int>::make(7),
            collect([&](MessageContext context, const int& value) {
              EXPECT_FALSE(context.cancelled());
              EXPECT_EQ(context.streamId(), "shared-parent");
              EXPECT_EQ(value, 7);
              siblingCompleted = true;
              return true;
            }));
      }), boost::asio::use_future);
  io.run();
  pending.get();
  canceller.get();
  EXPECT_TRUE(returned);
  EXPECT_TRUE(siblingCompleted);
  EXPECT_TRUE(retained.has_value());
  EXPECT_TRUE(weak.expired());
}

TEST(SubStream, DeadlineDuringAdmittedCollectorDrainsAndPreservesCompletion) {
  for (const bool completed : {false, true}) {
    SCOPED_TRACE(completed);
    App app; app.init(); app.prepare();
    boost::asio::io_context io;
    boost::asio::steady_timer timer{io};
    detail::SingleUseEvent release;
    bool collectorExited = false;
    int callbacks = 0;
    auto pending = boost::asio::co_spawn(io,
        detail::CooperativeExecution::Run([&] {
          const auto deadline = std::chrono::steady_clock::now() + 100ms;
          const auto consume = [&] {
            app.entry->consume(MessageContext{}.withDeadline(deadline),
                Payload<int>::make(1), collect([&](MessageContext context, const int&) {
                  ++callbacks;
                  EXPECT_FALSE(context.cancelled());
                  timer.expires_at(deadline + 1ms);
                  timer.async_wait([&](const boost::system::error_code& error) {
                    EXPECT_FALSE(error);
                    EXPECT_FALSE(collectorExited);
                    release.Send();
                  });
                  release.Wait();
                  EXPECT_TRUE(context.cancelled());
                  collectorExited = true;
                  return completed;
                }));
          };
          if (completed) {
            EXPECT_NO_THROW(consume());
          } else {
            EXPECT_THROW(consume(), std::runtime_error);
          }
          EXPECT_TRUE(collectorExited);
          EXPECT_EQ(callbacks, 1);
        }), boost::asio::use_future);
    io.run();
    pending.get();
  }
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

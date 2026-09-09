#include <any>
#include <barrier>
#include <thread>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/use_future.hpp>
#include <servicelib/runtime/detail/sync.hpp>
#include <atomic>
#include <chrono>
#include <stop_token>
#include <string>
#include <utility>
#include <vector>


#include <gtest/gtest.h>

#include "test_async.hpp"

#include <servicelib/runtime/environment/environment.hpp>
#include <servicelib/runtime/store/hashmap.hpp>
#include <servicelib/runtime/store/rotatingmap.hpp>
#include <servicelib/runtime/testlog/testlog.hpp>
#include <servicelib/runtime/testmetrics/testmetrics.hpp>

namespace {

test_async::AsioRuntime asioRuntime;

using namespace std::chrono_literals;

constexpr char kServiceName[] = "store-test-service";
constexpr char kJoinStoreName[] = "test-join-store";

class TestConfig final : public servicelib::config::IConfig {
 public:
  std::vector<const servicelib::config::ServiceConfig*> GetServices()
      const override {
    return {};
  }
  std::vector<servicelib::config::StreamConfigRef> GetStreams() const override {
    return {};
  }
  std::vector<servicelib::config::DataConnectorConfigRef> GetDataConnectors()
      const override {
    return {};
  }
  std::vector<servicelib::config::EndpointConfigRef> GetEndpoints()
      const override {
    return {};
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
};

class TestEnvironment final : public servicelib::IServiceEnvironment {
 public:
  TestEnvironment() : runtime_config_(config_) {
    service_config_.name = kServiceName;
  }

  std::shared_ptr<const servicelib::config::RuntimeConfig>
  getRuntimeConfigSnapshot() const override {
    return std::make_shared<const servicelib::config::RuntimeConfig>(
        runtime_config_);
  }
  std::shared_ptr<const servicelib::config::ServiceConfig>
  getServiceConfigSnapshot() const override {
    return std::make_shared<const servicelib::config::ServiceConfig>(
        service_config_);
  }
  servicelib::log::Logger& getLogger() override { return log_; }
  servicelib::metrics::Metrics& getMetrics() override { return metrics_; }
  servicelib::tracing::Tracing* getTracing() override { return nullptr; }

  servicelib::testmetrics::TestMetrics& metrics() { return metrics_; }

 private:
  TestConfig config_;
  servicelib::config::RuntimeConfig runtime_config_;
  servicelib::config::ServiceConfig service_config_;
  servicelib::testlog::TestLog log_;
  servicelib::testmetrics::TestMetrics metrics_;
};

template <typename Storage>
class StopStorageOnExit final {
 public:
  explicit StopStorageOnExit(Storage& storage) : storage_(storage) {}
  ~StopStorageOnExit() { storage_.stop(servicelib::Context{}); }

  StopStorageOnExit(const StopStorageOnExit&) = delete;
  StopStorageOnExit& operator=(const StopStorageOnExit&) = delete;

 private:
  Storage& storage_;
};

servicelib::metrics::Labels JoinLabels() {
  return {{"name", kJoinStoreName}, {"service", kServiceName}};
}

servicelib::store::JoinStorageConfig JoinConfig(
    std::chrono::steady_clock::duration ttl = {}, bool renew_ttl = false) {
  return {.name = kJoinStoreName, .ttl = ttl, .renewTtl = renew_ttl};
}

TEST(SingleUseEvent, SendBeforeWaitAndTimeoutAreStable) {
  servicelib::detail::SingleUseEvent event;
  EXPECT_FALSE(event.IsReady());
  EXPECT_FALSE(event.WaitUntil(std::chrono::steady_clock::now() + 1ms));
  event.Send();
  event.Send();
  EXPECT_TRUE(event.IsReady());
  event.Wait();
  EXPECT_TRUE(event.WaitUntil(std::chrono::steady_clock::now() - 1ms));
}

TEST(SingleUseEvent, RegistrationRacingWithSendDoesNotLoseWakeups) {
  for (int iteration = 0; iteration != 200; ++iteration) {
    servicelib::detail::SingleUseEvent event;
    std::barrier start{5};
    std::atomic<int> received{};
    std::vector<std::jthread> waiters;
    for (int n = 0; n != 4; ++n) waiters.emplace_back([&] {
      start.arrive_and_wait();
      if (event.WaitUntil(std::chrono::steady_clock::now() + 2s)) ++received;
    });
    start.arrive_and_wait();
    event.Send();
    waiters.clear();
    EXPECT_EQ(received, 4);
  }
}

TEST(SingleUseEvent, CancellingOneAsyncWaiterDoesNotSignalOthers) {
  boost::asio::io_context io;
  auto guard = boost::asio::make_work_guard(io);
  servicelib::detail::SingleUseEvent event;
  std::stop_source cancel;
  auto context = servicelib::Context{}.withStopToken(cancel.get_token());
  auto cancelled = boost::asio::co_spawn(io, event.AsyncWait(context), boost::asio::use_future);
  std::vector<std::future<void>> pending;
  for (int n = 0; n != 8; ++n)
    pending.push_back(boost::asio::co_spawn(io, event.AsyncWait(), boost::asio::use_future));
  std::jthread worker([&] { io.run(); });
  cancel.request_stop();
  EXPECT_EQ(cancelled.wait_for(2s), std::future_status::ready);
  cancelled.get();
  EXPECT_FALSE(event.IsReady());
  for (auto& waiter : pending) EXPECT_EQ(waiter.wait_for(0ms), std::future_status::timeout);
  event.Send();
  for (auto& waiter : pending) {
    EXPECT_EQ(waiter.wait_for(2s), std::future_status::ready);
    waiter.get();
  }
  guard.reset();
}

TEST(RotatingMap, BasicOperationsAndLifecycle) {
  using Map = servicelib::store::RotatingMap<std::string, int>;

  EXPECT_THROW(Map(0ms), std::invalid_argument);
  Map map{1h};

  map.set("one", 1);
  ASSERT_TRUE(map.get("one").has_value());
  EXPECT_EQ(*map.get("one"), 1);
  EXPECT_EQ(map.size(), 1);
  EXPECT_THROW(map.set("one", 2), servicelib::store::DuplicateKeyError);
  EXPECT_FALSE(map.get("missing").has_value());

  ASSERT_TRUE(map.pop("one").has_value());
  EXPECT_FALSE(map.pop("one").has_value());
  EXPECT_EQ(map.size(), 0);

  map.start(servicelib::Context{});
  EXPECT_THROW(map.start(servicelib::Context{}),
               servicelib::store::StoreAlreadyStartedError);
  map.stop(servicelib::Context{});
  map.stop(servicelib::Context{});

  // Go's lifecycle only controls the rotation timer; map operations remain
  // available after stop.
  map.set("after-stop", 7);
  EXPECT_EQ(*map.get("after-stop"), 7);
}

TEST(RotatingMap, RotationPreservesBothGenerations) {
  servicelib::store::RotatingMap<std::string, int> map{25ms, 0};
  map.set("before", 1);
  map.start(servicelib::Context{});
  StopStorageOnExit stop_guard{map};

  test_async::SleepFor(60ms);
  EXPECT_EQ(*map.get("before"), 1);
  EXPECT_THROW(map.set("before", 2), servicelib::store::DuplicateKeyError);

  map.set("after", 2);
  EXPECT_EQ(map.size(), 2);
  EXPECT_EQ(*map.get("before"), 1);
  EXPECT_EQ(*map.get("after"), 2);
  EXPECT_EQ(*map.pop("before"), 1);
  EXPECT_EQ(*map.pop("after"), 2);
  EXPECT_EQ(map.size(), 0);
}

TEST(RotatingMap, ConcurrentFactoryRunsOnceAndFailureDoesNotInsert) {
  servicelib::store::RotatingMap<std::string, std::shared_ptr<int>> map{1h};
  std::barrier start{17};
  std::atomic<int> factories{}, existing{};
  std::vector<std::jthread> tasks;
  for (int n = 0; n != 16; ++n) tasks.emplace_back([&] {
    start.arrive_and_wait();
    const auto [value, found] = map.getOrCreate("same", [&] {
      ++factories;
      return std::make_shared<int>(42);
    });
    EXPECT_EQ(*value, 42);
    if (found) ++existing;
  });
  start.arrive_and_wait();
  tasks.clear();
  EXPECT_EQ(factories, 1);
  EXPECT_EQ(existing, 15);
  EXPECT_THROW(static_cast<void>(map.getOrCreate("error", []() -> std::shared_ptr<int> {
    throw std::runtime_error("factory failed");
  })), std::runtime_error);
  EXPECT_FALSE(map.get("error"));
  EXPECT_EQ(map.size(), 1U);
  EXPECT_FALSE(map.getOrCreate("error", [] { return std::make_shared<int>(7); }).second);
}

TEST(RotatingMap, SupportsMoveOnlyValuesAndConcurrentRotation) {
  servicelib::store::RotatingMap<std::string, std::unique_ptr<int>> map{1ms, 0};
  map.start(servicelib::Context{});
  StopStorageOnExit stop{map};
  map.set("retained", std::make_unique<int>(77));
  std::vector<std::jthread> tasks;
  for (int t = 0; t != 4; ++t) tasks.emplace_back([&, t] {
    for (int n = 0; n != 5000; ++n) {
      const auto key = std::to_string(t) + ":" + std::to_string(n);
      map.set(key, std::make_unique<int>(n));
      const auto value = map.pop(key);
      ASSERT_TRUE(value);
      EXPECT_EQ(**value, n);
    }
  });
  tasks.clear();
  EXPECT_EQ(map.size(), 1U);
  EXPECT_EQ(**map.pop("retained"), 77);
}

TEST(HashMapJoinStorage, LifecycleAggregationAndMetrics) {
  TestEnvironment environment;
  servicelib::store::HashMapJoinStorage<std::string> storage{environment,
                                                             JoinConfig()};

  EXPECT_THROW(storage.joinValue(servicelib::Context{}, "key", 0, 1,
                                 [](auto&) { return false; }),
               servicelib::store::StoreNotStartedError);
  storage.start(servicelib::Context{});
  StopStorageOnExit stop_guard{storage};
  EXPECT_THROW(storage.start(servicelib::Context{}),
               servicelib::store::StoreAlreadyStartedError);

  std::atomic<int> callbacks{0};
  const auto callback = [&](servicelib::store::JoinValues& values) {
    callbacks.fetch_add(1, std::memory_order_relaxed);
    if (values.size() < 2 || values[0].empty() || values[1].empty()) {
      return false;
    }
    EXPECT_EQ(std::any_cast<int>(values[0][0]), 42);
    EXPECT_EQ(std::any_cast<std::string>(values[1][0]), "value");
    return true;
  };

  storage.joinValue(servicelib::Context{}, "key", 0, 42, callback);
  EXPECT_EQ(storage.size(), 1);
  storage.joinValue(servicelib::Context{}, "key", 1, std::string{"value"},
                    callback);

  EXPECT_EQ(callbacks.load(std::memory_order_relaxed), 2);
  EXPECT_EQ(storage.size(), 0);
  EXPECT_EQ(environment.metrics()
                .gauge("hashmap_join_storage.count", JoinLabels())
                .value(),
            0);

  storage.stop(servicelib::Context{});
  EXPECT_THROW(storage.joinValue(servicelib::Context{}, "key", 0, 1, callback),
               servicelib::store::StoreStoppedError);
}

TEST(HashMapJoinStorage, ConcurrentSameKeySerializesCallbacks) {
  TestEnvironment environment;
  servicelib::store::HashMapJoinStorage<std::string> storage{environment,
                                                             JoinConfig()};
  storage.start(servicelib::Context{});
  StopStorageOnExit stop_guard{storage};

  constexpr std::size_t kTasks = 64;
  std::atomic<std::size_t> callbacks{0};
  std::atomic<std::size_t> largest_batch{0};
  const auto callback = [&](servicelib::store::JoinValues& values) {
    callbacks.fetch_add(1, std::memory_order_relaxed);
    const std::size_t size = values[0].size();
    std::size_t observed = largest_batch.load(std::memory_order_relaxed);
    while (observed < size && !largest_batch.compare_exchange_weak(
                                  observed, size, std::memory_order_relaxed)) {
    }
    return false;
  };

  std::vector<test_async::TaskWithResult<void>> tasks;
  tasks.reserve(kTasks);
  for (std::size_t i = 0; i < kTasks; ++i) {
    tasks.push_back(test_async::Async("join-store-test", [&, i] {
      storage.joinValue(servicelib::Context{}, "shared", 0, i, callback);
    }));
  }
  for (auto& task : tasks) {
    task.Get();
  }

  EXPECT_EQ(callbacks.load(std::memory_order_relaxed), kTasks);
  EXPECT_EQ(largest_batch.load(std::memory_order_relaxed), kTasks);
  EXPECT_EQ(storage.size(), 1);
  EXPECT_EQ(environment.metrics()
                .gauge("hashmap_join_storage.count", JoinLabels())
                .value(),
            1);
}

TEST(HashMapJoinStorage, StopDrainsAdmittedCallbackBeforeReturning) {
  TestEnvironment environment;
  servicelib::store::HashMapJoinStorage<std::string> storage{environment,
                                                             JoinConfig()};
  storage.start(servicelib::Context{});
  StopStorageOnExit stop_guard{storage};

  test_async::Event callback_started;
  test_async::Event release_callback;
  std::atomic<bool> stop_returned{false};
  auto join_task = test_async::Async("join-store-admitted", [&] {
    storage.joinValue(
        servicelib::Context{}, "key", 0, 1,
        [&](servicelib::store::JoinValues&) {
          callback_started.Send();
          static_cast<void>(release_callback.WaitForEvent());
          return false;
        });
  });
  ASSERT_TRUE(callback_started.WaitForEvent());

  auto stop_task = test_async::Async("join-store-stop", [&] {
    storage.stop(servicelib::Context{});
    stop_returned.store(true, std::memory_order_release);
  });
  test_async::SleepFor(30ms);
  EXPECT_FALSE(stop_returned.load(std::memory_order_acquire));

  release_callback.Send();
  join_task.Get();
  stop_task.Get();
  EXPECT_TRUE(stop_returned.load(std::memory_order_acquire));
  EXPECT_EQ(storage.size(), 0U);
  EXPECT_THROW(storage.joinValue(servicelib::Context{}, "after-stop", 0, 2,
                                 [](auto&) { return false; }),
               servicelib::store::StoreStoppedError);
}

TEST(HashMapJoinStorage, TtlExpiryRemovesItemExactlyOnce) {
  TestEnvironment environment;
  servicelib::store::HashMapJoinStorage<std::string> storage{environment,
                                                             JoinConfig(50ms)};
  storage.start(servicelib::Context{});
  StopStorageOnExit stop_guard{storage};

  std::atomic<int> callbacks{0};
  test_async::Event expired;
  const auto callback = [&](servicelib::store::JoinValues&) {
    if (callbacks.fetch_add(1, std::memory_order_acq_rel) == 1) {
      expired.Send();
    }
    return false;
  };

  storage.joinValue(servicelib::Context{}, "key", 0, 1, callback);
  ASSERT_TRUE(expired.WaitForEventFor(test_async::kMaxTestWaitTime));
  test_async::SleepFor(30ms);

  EXPECT_EQ(callbacks.load(std::memory_order_relaxed), 2);
  EXPECT_EQ(storage.size(), 0);
  EXPECT_EQ(environment.metrics()
                .counter("hashmap_join_storage.evictions_total", JoinLabels())
                .count(),
            1);
  EXPECT_EQ(environment.metrics()
                .gauge("hashmap_join_storage.count", JoinLabels())
                .value(),
            0);
}

TEST(HashMapJoinStorage, ContextDeadlineOverridesConfiguredTtl) {
  TestEnvironment environment;
  servicelib::store::HashMapJoinStorage<std::string> storage{environment,
                                                             JoinConfig(1h)};
  storage.start(servicelib::Context{});
  StopStorageOnExit stop_guard{storage};

  std::atomic<int> callbacks{0};
  test_async::Event expired;
  const auto callback = [&](servicelib::store::JoinValues&) {
    if (callbacks.fetch_add(1, std::memory_order_acq_rel) == 1) {
      expired.Send();
    }
    return false;
  };

  storage.joinValue(servicelib::Context{}.withDeadline(
                        std::chrono::steady_clock::now() + 50ms),
                    "key", 0, 1, callback);

  ASSERT_TRUE(expired.WaitForEventFor(test_async::kMaxTestWaitTime));
  EXPECT_EQ(callbacks.load(std::memory_order_relaxed), 2);
  EXPECT_EQ(storage.size(), 0);
}

TEST(HashMapJoinStorage, ExpiredContextIsEvaluatedImmediatelyLikeGo) {
  TestEnvironment environment;
  servicelib::store::HashMapJoinStorage<std::string> storage{environment,
                                                             JoinConfig(1h)};
  storage.start(servicelib::Context{});
  StopStorageOnExit stop_guard{storage};

  std::atomic<int> callbacks{0};
  storage.joinValue(
      servicelib::Context{}.withDeadline(std::chrono::steady_clock::now() - 1s),
      "key", 0, 1, [&](servicelib::store::JoinValues&) {
        callbacks.fetch_add(1, std::memory_order_relaxed);
        return true;
      });

  EXPECT_EQ(callbacks.load(std::memory_order_relaxed), 1);
  EXPECT_EQ(storage.size(), 0);
}

TEST(HashMapJoinStorage, ExplicitCancellationExpiresItem) {
  TestEnvironment environment;
  servicelib::store::HashMapJoinStorage<std::string> storage{environment,
                                                             JoinConfig(1h)};
  storage.start(servicelib::Context{});
  StopStorageOnExit stop_guard{storage};

  std::atomic<int> callbacks{0};
  test_async::Event expired;
  const auto callback = [&](servicelib::store::JoinValues&) {
    if (callbacks.fetch_add(1, std::memory_order_acq_rel) == 1) {
      expired.Send();
    }
    return false;
  };

  std::stop_source source;
  storage.joinValue(servicelib::Context{}.withStopToken(source.get_token()),
                    "key", 0, 1, callback);
  source.request_stop();

  ASSERT_TRUE(expired.WaitForEventFor(test_async::kMaxTestWaitTime));
  EXPECT_EQ(callbacks.load(std::memory_order_relaxed), 2);
  EXPECT_EQ(storage.size(), 0);
  EXPECT_EQ(environment.metrics()
                .counter("hashmap_join_storage.evictions_total", JoinLabels())
                .count(),
            1);
}

TEST(HashMapJoinStorage, RenewTtlInvalidatesOldTimer) {
  TestEnvironment environment;
  servicelib::store::HashMapJoinStorage<std::string> storage{
      environment, JoinConfig(120ms, true)};
  storage.start(servicelib::Context{});
  StopStorageOnExit stop_guard{storage};

  std::atomic<int> callbacks{0};
  test_async::Event expired;
  const auto callback = [&](servicelib::store::JoinValues&) {
    if (callbacks.fetch_add(1, std::memory_order_acq_rel) == 2) {
      expired.Send();
    }
    return false;
  };

  storage.joinValue(servicelib::Context{}, "key", 0, 1, callback);
  test_async::SleepFor(80ms);
  storage.joinValue(servicelib::Context{}, "key", 1, 2, callback);

  test_async::SleepFor(70ms);
  EXPECT_EQ(callbacks.load(std::memory_order_relaxed), 2);
  EXPECT_EQ(storage.size(), 1);

  ASSERT_TRUE(expired.WaitForEventFor(test_async::kMaxTestWaitTime));
  EXPECT_EQ(callbacks.load(std::memory_order_relaxed), 3);
  EXPECT_EQ(storage.size(), 0);
  EXPECT_EQ(environment.metrics()
                .counter("hashmap_join_storage.evictions_total", JoinLabels())
                .count(),
            1);
}

}  // namespace

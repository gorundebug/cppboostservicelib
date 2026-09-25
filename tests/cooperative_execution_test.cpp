#include <servicelib/runtime/detail/sync.hpp>
#include <servicelib/runtime/environment.hpp>

#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <shared_mutex>
#include <servicelib/datasource/detail/result_context.hpp>
#include <thread>
#include <vector>

namespace {
using servicelib::detail::CooperativeExecution;
using servicelib::detail::CooperativeMutex;
using servicelib::detail::SingleUseEvent;
using namespace std::chrono_literals;

struct GraphDrainTypes final {
  template <typename> struct DataType {};
};

template <int Scenario>
class GraphDrainApp final
    : public servicelib::StreamExecutionEnvironment<GraphDrainApp<Scenario>, GraphDrainTypes> {
 public:
  using Base = servicelib::StreamExecutionEnvironment<GraphDrainApp<Scenario>, GraphDrainTypes>;
  using Base::startExecutionRuntime;
  using Base::drainExecutionRuntime;
  using Base::stopExecutionRuntime;
  std::shared_ptr<const servicelib::config::RuntimeConfig>
  getRuntimeConfigSnapshot() const override { return {}; }
  std::shared_ptr<const servicelib::config::ServiceConfig>
  getServiceConfigSnapshot() const override { return {}; }
};

TEST(CooperativeExecution, GraphDrainWaitsForInputsAndParallelChildrenWithoutBlocking) {
  boost::asio::io_context io;
  // ExecutionRuntime is cached per concrete application type. Keep its owner
  // alive across repeated executions of this regression.
  static GraphDrainApp<1> app;
  servicelib::detail::ParallelExecutorRegistry::Set(io.get_executor());
  app.startExecutionRuntime();
  SingleUseEvent inputStarted, parallelStarted, draining, releaseInput;
  SingleUseEvent releaseParent, childStarted, releaseChild;
  bool inputFinished = false, parentFinished = false, childFinished = false;
  bool drainReturned = false;
  auto input = boost::asio::co_spawn(io, CooperativeExecution::Run([&] {
    auto invocation = app.beginInputInvocation();
    inputStarted.Send();
    releaseInput.Wait();
    inputFinished = true;
  }), boost::asio::use_future);
  app.parallel([&] {
    parallelStarted.Send();
    releaseParent.Wait();
    app.parallel([&] {
      childStarted.Send();
      releaseChild.Wait();
      childFinished = true;
    });
    parentFinished = true;
  });
  auto stopper = boost::asio::co_spawn(io, CooperativeExecution::Run([&] {
    inputStarted.Wait();
    parallelStarted.Wait();
    draining.Send();
    const bool drained = app.drainExecutionRuntime(
        servicelib::Context{}.withDeadline(std::chrono::steady_clock::now() + 1s));
    drainReturned = true;
    EXPECT_TRUE(drained);
    EXPECT_TRUE(inputFinished);
    EXPECT_TRUE(parentFinished);
    EXPECT_TRUE(childFinished);
  }), boost::asio::use_future);
  auto releaser = boost::asio::co_spawn(io, CooperativeExecution::Run([&] {
    draining.Wait();
    releaseInput.Send();
    releaseParent.Send();
    childStarted.Wait();
    EXPECT_FALSE(drainReturned);
    releaseChild.Send();
  }), boost::asio::use_future);
  io.run();
  input.get();
  stopper.get();
  releaser.get();
  app.stopExecutionRuntime();
  servicelib::detail::ParallelExecutorRegistry::Clear();
}

TEST(CooperativeExecution, GraphDrainTimeoutKeepsOwnershipUntilFinalStop) {
  boost::asio::io_context io;
  static GraphDrainApp<2> app;
  servicelib::detail::ParallelExecutorRegistry::Set(io.get_executor());
  app.startExecutionRuntime();
  SingleUseEvent started, timedOut, release;
  bool completed = false;
  auto input = boost::asio::co_spawn(io, CooperativeExecution::Run([&] {
    auto invocation = app.beginInputInvocation();
    started.Send();
    release.Wait();
    completed = true;
  }), boost::asio::use_future);
  auto stopper = boost::asio::co_spawn(io, CooperativeExecution::Run([&] {
    started.Wait();
    EXPECT_FALSE(app.drainExecutionRuntime(
        servicelib::Context{}.withDeadline(std::chrono::steady_clock::now() + 1ms)));
    EXPECT_FALSE(completed);
    timedOut.Send();
    // The bounded retry makes the original blocking implementation fail
    // without hanging fixture cleanup in its unbounded final safety join.
    EXPECT_TRUE(app.drainExecutionRuntime(
        servicelib::Context{}.withDeadline(std::chrono::steady_clock::now() + 1s)));
    EXPECT_TRUE(completed);
  }), boost::asio::use_future);
  auto releaser = boost::asio::co_spawn(io, CooperativeExecution::Run([&] {
    timedOut.Wait();
    release.Send();
  }), boost::asio::use_future);
  io.run();
  input.get();
  stopper.get();
  releaser.get();
  app.stopExecutionRuntime();
  servicelib::detail::ParallelExecutorRegistry::Clear();
}

TEST(CooperativeExecution, GraphDrainWakesNativeAndCooperativeWaiters) {
  boost::asio::io_context io;
  static GraphDrainApp<3> app;
  servicelib::detail::ParallelExecutorRegistry::Set(io.get_executor());
  app.startExecutionRuntime();
  std::optional<typename GraphDrainApp<3>::InputInvocation> invocation;
  invocation.emplace(app.beginInputInvocation());
  SingleUseEvent nativeReady, cooperativeReady, release;
  std::atomic<int> nativeStarted{0}, nativeCompleted{0};
  std::atomic<bool> childCompleted{false};
  int cooperativeStarted = 0;
  app.parallel([&] {
    release.Wait();
    app.parallel([&] { childCompleted = true; });
  });
  std::vector<std::thread> native;
  for (int index = 0; index < 2; ++index) {
    native.emplace_back([&] {
      if (nativeStarted.fetch_add(1) + 1 == 2) nativeReady.Send();
      const bool drained = app.drainExecutionRuntime(
          servicelib::Context{}.withDeadline(std::chrono::steady_clock::now() + 3s));
      EXPECT_TRUE(drained);
      EXPECT_TRUE(childCompleted.load());
      if (drained) ++nativeCompleted;
    });
  }
  std::vector<std::future<void>> cooperative;
  for (int index = 0; index < 2; ++index) {
    cooperative.push_back(boost::asio::co_spawn(io, CooperativeExecution::Run([&] {
      if (++cooperativeStarted == 2) cooperativeReady.Send();
      EXPECT_TRUE(app.drainExecutionRuntime(
          servicelib::Context{}.withDeadline(std::chrono::steady_clock::now() + 3s)));
      EXPECT_TRUE(childCompleted.load());
    }), boost::asio::use_future));
  }
  auto releaser = boost::asio::co_spawn(io, CooperativeExecution::Run([&] {
    nativeReady.Wait();
    cooperativeReady.Wait();
    invocation.reset();
    release.Send();
  }), boost::asio::use_future);
  io.run();
  for (auto& waiter : native) waiter.join();
  for (auto& waiter : cooperative) waiter.get();
  releaser.get();
  EXPECT_EQ(nativeCompleted.load(), 2);
  app.stopExecutionRuntime();
  servicelib::detail::ParallelExecutorRegistry::Clear();
}

TEST(CooperativeExecution, GraphFinalStopWaitsWithoutOccupyingTheOnlyWorker) {
  boost::asio::io_context io;
  static GraphDrainApp<4> app;
  servicelib::detail::ParallelExecutorRegistry::Set(io.get_executor());
  app.startExecutionRuntime();
  SingleUseEvent entered, stopping, release;
  bool completed = false;
  std::promise<void> stopped;
  auto stoppedFuture = stopped.get_future();
  std::atomic<bool> rescued{false};
  auto input = boost::asio::co_spawn(io, CooperativeExecution::Run([&] {
    auto invocation = app.beginInputInvocation();
    entered.Send();
    release.Wait();
    completed = true;
  }), boost::asio::use_future);
  auto stopper = boost::asio::co_spawn(io, CooperativeExecution::Run([&] {
    entered.Wait();
    stopping.Send();
    app.stopExecutionRuntime();
    EXPECT_TRUE(completed);
    stopped.set_value();
  }), boost::asio::use_future);
  auto releaser = boost::asio::co_spawn(io, CooperativeExecution::Run([&] {
    stopping.Wait();
    release.Send();
  }), boost::asio::use_future);
  std::thread watchdog([&] {
    if (stoppedFuture.wait_for(3s) != std::future_status::ready) {
      // Rescue a blocking regression so the assertion, rather than fixture
      // teardown, reports it. This thread does not execute work on success.
      rescued = true;
      io.run_for(1s);
    }
  });
  io.run();
  watchdog.join();
  input.get();
  stopper.get();
  releaser.get();
  EXPECT_FALSE(rescued.load());
  servicelib::detail::ParallelExecutorRegistry::Clear();
}

TEST(CooperativeExecution, GraphDrainIncludesParallelCallbackCaptureCleanup) {
  static GraphDrainApp<5> app;
  for (bool throwFromCallback : {false, true}) {
    boost::asio::io_context io;
    servicelib::detail::ParallelExecutorRegistry::Set(io.get_executor());
    app.startExecutionRuntime();
    std::promise<void> cleanupStarted, releaseCleanup;
    auto started = cleanupStarted.get_future();
    auto release = releaseCleanup.get_future();
    std::atomic<bool> cleanupCompleted{false};
    auto owned = std::shared_ptr<int>(new int(42), [&](int* value) {
      delete value;
      cleanupStarted.set_value();
      // A native destructor deliberately holds the callback's ownership
      // boundary. No dangling graph access is needed to expose early drain.
      cleanupCompleted = release.wait_for(3s) == std::future_status::ready;
    });
    app.parallel([owned = std::move(owned), throwFromCallback] {
      EXPECT_EQ(*owned, 42);
      if (throwFromCallback) throw std::runtime_error("parallel callback failed");
    });
    std::thread worker([&] { io.run(); });
    const bool entered = started.wait_for(3s) == std::future_status::ready;
    const bool drainedBeforeCleanup = app.drainExecutionRuntime(
        servicelib::Context{}.withDeadline(std::chrono::steady_clock::now()));
    releaseCleanup.set_value();
    worker.join();
    EXPECT_TRUE(entered);
    EXPECT_FALSE(drainedBeforeCleanup);
    EXPECT_TRUE(cleanupCompleted.load());
    EXPECT_TRUE(app.drainExecutionRuntime());
    app.stopExecutionRuntime();
    servicelib::detail::ParallelExecutorRegistry::Clear();
  }
}

TEST(CooperativeExecution, ControlTasksWaitWithoutBlockingAndPreserveErrors) {
  boost::asio::io_context io;
  SingleUseEvent waiting, release;
  std::weak_ptr<int> capture;
  auto caller = boost::asio::co_spawn(io, CooperativeExecution::Run([&] {
    auto owned = std::make_shared<int>(42);
    capture = owned;
    servicelib::detail::ControlTask<int> value([owned = std::move(owned), &release] {
      if (!release.WaitUntil(std::chrono::steady_clock::now() + 3s))
        throw std::runtime_error("reactor blocked during control task");
      return *owned;
    });
    servicelib::detail::ControlTask<void> failure([&release] {
      if (!release.WaitUntil(std::chrono::steady_clock::now() + 3s))
        throw std::logic_error("reactor blocked during failing control task");
      throw std::runtime_error("control failure");
    });
    EXPECT_EQ(value.wait_for(0ms), std::future_status::timeout);
    EXPECT_EQ(value.wait_for(1ms), std::future_status::timeout);
    EXPECT_FALSE(capture.expired());
    waiting.Send();
    EXPECT_EQ(value.get(), 42);
    EXPECT_TRUE(capture.expired());
    EXPECT_THROW(failure.get(), std::runtime_error);
  }), boost::asio::use_future);
  auto responder = boost::asio::co_spawn(io, CooperativeExecution::Run([&] {
    waiting.Wait();
    release.Send();
  }), boost::asio::use_future);
  io.run();
  caller.get();
  responder.get();
}

TEST(CooperativeExecution, ControlTaskUnwindingStillDrainsCooperatively) {
  boost::asio::io_context io;
  SingleUseEvent waiting, release;
  std::atomic<bool> completed{false};
  auto caller = boost::asio::co_spawn(io, CooperativeExecution::Run([&] {
    try {
      servicelib::detail::ControlTask<void> task([&] {
        completed = release.WaitUntil(std::chrono::steady_clock::now() + 3s);
      });
      servicelib::detail::ControlTask<void> moved(std::move(task));
      waiting.Send();
      throw std::runtime_error("later task setup failed");
    } catch (const std::runtime_error&) {
      EXPECT_TRUE(completed.load());
    }
  }), boost::asio::use_future);
  auto responder = boost::asio::co_spawn(io, CooperativeExecution::Run([&] {
    waiting.Wait();
    release.Send();
  }), boost::asio::use_future);
  io.run();
  caller.get();
  responder.get();
}

TEST(CooperativeExecution, WaitingCallReleasesTheOnlyWorker) {
  boost::asio::io_context io;
  SingleUseEvent started, response;
  std::vector<int> order;
  auto caller = boost::asio::co_spawn(io, CooperativeExecution::Run([&] {
    order.push_back(1);
    started.Send();
    response.Wait();
    EXPECT_TRUE(CooperativeExecution::Active());
    order.push_back(3);
    return 42;
  }), boost::asio::use_future);
  auto responder = boost::asio::co_spawn(io, CooperativeExecution::Run([&] {
    started.Wait();
    order.push_back(2);
    response.Send();
  }), boost::asio::use_future);
  io.run();
  EXPECT_EQ(caller.get(), 42);
  responder.get();
  EXPECT_EQ(order, (std::vector<int>{1, 2, 3}));
  EXPECT_FALSE(CooperativeExecution::Active());
}

TEST(CooperativeExecution, DeadlineAndCancellationDoNotBlockTheWorker) {
  boost::asio::io_context io;
  SingleUseEvent absent, started;
  std::stop_source stop;
  auto waiter = boost::asio::co_spawn(io, CooperativeExecution::Run([&] {
    EXPECT_FALSE(absent.WaitUntil(std::chrono::steady_clock::now() + 1ms));
    auto context = servicelib::Context{}.withExternalCancellation(stop.get_token());
    started.Send();
    CooperativeExecution::Await([&] { return absent.AsyncWait(context); });
    EXPECT_TRUE(context.cancelled());
  }), boost::asio::use_future);
  auto canceller = boost::asio::co_spawn(io, CooperativeExecution::Run([&] {
    started.Wait();
    stop.request_stop();
  }), boost::asio::use_future);
  io.run();
  waiter.get();
  canceller.get();
}

TEST(CooperativeExecution, ContendedCollectorLockReleasesTheOnlyWorker) {
  boost::asio::io_context io;
  CooperativeMutex mutex;
  SingleUseEvent held, attempting, release;
  std::vector<int> order;
  auto owner = boost::asio::co_spawn(io, CooperativeExecution::Run([&] {
    std::lock_guard lock(mutex);
    held.Send();
    release.Wait();
    order.push_back(1);
  }), boost::asio::use_future);
  auto contender = boost::asio::co_spawn(io, CooperativeExecution::Run([&] {
    held.Wait();
    attempting.Send();
    std::lock_guard lock(mutex);
    order.push_back(2);
  }), boost::asio::use_future);
  auto releaser = boost::asio::co_spawn(io, CooperativeExecution::Run([&] {
    attempting.Wait();
    release.Send();
  }), boost::asio::use_future);
  io.run();
  owner.get();
  contender.get();
  releaser.get();
  EXPECT_EQ(order, (std::vector<int>{1, 2}));
}

TEST(CooperativeExecution, SharedCallbacksStayConcurrentAndWriterDoesNotStarve) {
  boost::asio::io_context io;
  servicelib::detail::CooperativeSharedMutex mutex;
  SingleUseEvent firstEntered, bothEntered, writerAttempting, release;
  int finishedReaders = 0;
  std::vector<int> order;
  auto first = boost::asio::co_spawn(io, CooperativeExecution::Run([&] {
    std::shared_lock lock(mutex);
    firstEntered.Send();
    release.Wait();
    ++finishedReaders;
  }), boost::asio::use_future);
  auto second = boost::asio::co_spawn(io, CooperativeExecution::Run([&] {
    firstEntered.Wait();
    std::shared_lock lock(mutex);
    bothEntered.Send();
    release.Wait();
    ++finishedReaders;
  }), boost::asio::use_future);
  auto writer = boost::asio::co_spawn(io, CooperativeExecution::Run([&] {
    bothEntered.Wait();
    writerAttempting.Send();
    std::unique_lock lock(mutex);
    EXPECT_EQ(finishedReaders, 2);
    order.push_back(1);
  }), boost::asio::use_future);
  auto lastReader = boost::asio::co_spawn(io, CooperativeExecution::Run([&] {
    writerAttempting.Wait();
    release.Send();
    std::shared_lock lock(mutex);
    order.push_back(2);
  }), boost::asio::use_future);
  io.run();
  first.get(); second.get(); writer.get(); lastReader.get();
  EXPECT_EQ(order, (std::vector<int>{1, 2}));
}

TEST(CooperativeExecution, SourceResultLifetimeLockAllowsProgressOnOneWorker) {
  boost::asio::io_context io;
  using Result = servicelib::datasource::localsource::PendingResult<
      int, int, int, std::exception_ptr>;
  Result result{0, nullptr};
  SingleUseEvent readerEntered, writerAttempting, release;
  bool readerFinished = false;
  auto reader = boost::asio::co_spawn(io, CooperativeExecution::Run([&] {
    std::shared_lock lock(result.lifetimeMutex);
    readerEntered.Send();
    release.Wait();
    readerFinished = true;
  }), boost::asio::use_future);
  auto writer = boost::asio::co_spawn(io, CooperativeExecution::Run([&] {
    readerEntered.Wait();
    writerAttempting.Send();
    std::unique_lock lock(result.lifetimeMutex);
    EXPECT_TRUE(readerFinished);
  }), boost::asio::use_future);
  auto releaser = boost::asio::co_spawn(io, CooperativeExecution::Run([&] {
    writerAttempting.Wait();
    release.Send();
  }), boost::asio::use_future);
  io.run();
  reader.get();
  writer.get();
  releaser.get();
}

TEST(CooperativeExecution, NestedCallsAndErrorsPreserveTheCallingScope) {
  boost::asio::io_context io;
  auto call = boost::asio::co_spawn(io, CooperativeExecution::Run([] {
    const auto result = CooperativeExecution::Await([] {
      EXPECT_FALSE(CooperativeExecution::Active());
      return CooperativeExecution::Run([] {
        EXPECT_TRUE(CooperativeExecution::Active());
        return 7;
      });
    });
    EXPECT_EQ(result, 7);
    EXPECT_TRUE(CooperativeExecution::Active());
    EXPECT_THROW(CooperativeExecution::Await([] {
      return CooperativeExecution::Run([]() -> int {
        throw std::runtime_error("business failure");
      });
    }), std::runtime_error);
    EXPECT_TRUE(CooperativeExecution::Active());
  }), boost::asio::use_future);
  io.run();
  call.get();
  EXPECT_FALSE(CooperativeExecution::Active());
}

TEST(CooperativeExecution, ForcedWorkerMigrationRestoresScopeOnTheResumingThread) {
  boost::asio::io_context io;
  auto work = boost::asio::make_work_guard(io);
  SingleUseEvent response;
  std::atomic<bool> entered{};
  std::promise<void> suspended, allowFirstWorkerExit;
  auto suspendedFuture = suspended.get_future();
  auto exitFuture = allowFirstWorkerExit.get_future();
  bool firstWorkerScopeLeaked = false;
  bool secondWorkerScopeLeaked = false;
  auto call = boost::asio::co_spawn(io, CooperativeExecution::Run([&] {
    EXPECT_TRUE(CooperativeExecution::Active());
    entered.store(true);
    const auto result = CooperativeExecution::Await([&] {
      return CooperativeExecution::Run([&] {
        response.Wait();
        return 42;
      });
    });
    EXPECT_EQ(result, 42);
    // Do not perform another Await here: a missing scope must fail an
    // assertion, rather than crash by dereferencing the missing scope.
    return CooperativeExecution::Active();
  }), boost::asio::use_future);
  std::jthread firstWorker([&] {
    while (!entered.load()) io.run_one();
    // run_one returns only after the business stack has suspended. Keep this
    // OS thread alive, but no longer let it service the executor.
    suspended.set_value();
    exitFuture.wait();
    firstWorkerScopeLeaked = CooperativeExecution::Active();
  });
  suspendedFuture.wait();
  response.Send();
  work.reset();
  std::jthread secondWorker([&] {
    io.run();
    secondWorkerScopeLeaked = CooperativeExecution::Active();
  });
  const bool differentThreads = firstWorker.get_id() != secondWorker.get_id();
  secondWorker.join();
  allowFirstWorkerExit.set_value();
  firstWorker.join();
  EXPECT_TRUE(differentThreads);
  EXPECT_TRUE(call.get());
  EXPECT_FALSE(firstWorkerScopeLeaked);
  EXPECT_FALSE(secondWorkerScopeLeaked);
}

TEST(CooperativeExecution, ConcurrentCallsPreserveScopeAcrossWorkerResumption) {
  boost::asio::io_context io;
  std::atomic<int> completed{};
  std::vector<std::future<void>> calls;
  for (int i = 0; i < 100; ++i) {
    calls.push_back(boost::asio::co_spawn(io, CooperativeExecution::Run([&, i] {
      for (int iteration = 0; iteration < 10; ++iteration) {
        const auto value = CooperativeExecution::Await([i] {
          EXPECT_FALSE(CooperativeExecution::Active());
          return CooperativeExecution::Run([i] { return i; });
        });
        EXPECT_EQ(value, i);
        EXPECT_TRUE(CooperativeExecution::Active());
      }
      ++completed;
    }), boost::asio::use_future));
  }
  std::vector<std::jthread> workers;
  for (int i = 0; i < 4; ++i) workers.emplace_back([&] {
    io.run();
    EXPECT_FALSE(CooperativeExecution::Active());
  });
  for (auto& call : calls) call.get();
  workers.clear();
  EXPECT_EQ(completed.load(), 100);
}
}  // namespace

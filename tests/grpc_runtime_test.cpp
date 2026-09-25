#include <servicelib/runtime/detail/grpc_runtime.hpp>
#include <servicelib/runtime/testmetrics/testmetrics.hpp>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <atomic>
#include <algorithm>
#include <chrono>
#include <ctime>
#include <future>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <thread>

namespace asio = boost::asio;
using namespace std::chrono_literals;

asio::awaitable<void> MarkCompleted(std::atomic<int>& completed) {
  co_await asio::post(asio::use_awaitable);
  completed.fetch_add(1, std::memory_order_release);
}

void Require(bool condition, const char* message) {
  if (!condition) throw std::runtime_error(message);
}

void CheckIdleAndWakeup(std::size_t workers) {
  servicelib::async::GrpcRuntime runtime({.workers = workers});
  runtime.Start();
  // Exclude one-time thread startup. Measure process CPU rather than wall time
  // so an overloaded CI host does not make an idle runtime look busy.
  std::this_thread::sleep_for(100ms);
  const auto cpuStart = std::clock();
  const auto wallStart = std::chrono::steady_clock::now();
  std::this_thread::sleep_for(1s);
  const auto cpuSeconds = static_cast<double>(std::clock() - cpuStart) /
                          CLOCKS_PER_SEC;
  const auto wallSeconds = std::chrono::duration<double>(
      std::chrono::steady_clock::now() - wallStart).count();
  std::cout << "idle workers=" << workers << " cpu_seconds=" << cpuSeconds
            << " wall_seconds=" << wallSeconds << '\n';
  Require(cpuSeconds < wallSeconds * 0.03,
          "idle gRPC runtime consumes more than 3% of one CPU");

  std::promise<void> completion;
  auto ready = completion.get_future();
  // Wake both event loops from outside, including a completion posted back
  // from the transport thread to a business worker.
  asio::post(runtime.grpcContext(), [&] {
    asio::post(runtime.executor(), [&] { completion.set_value(); });
  });
  Require(ready.wait_for(1s) == std::future_status::ready,
          "idle runtime did not wake for posted work");
  asio::steady_timer timer(runtime.ioContext(), 10ms);
  std::promise<void> timerCompletion;
  auto timerReady = timerCompletion.get_future();
  timer.async_wait([&](const boost::system::error_code& error) {
    if (!error) timerCompletion.set_value();
  });
  Require(timerReady.wait_for(1s) == std::future_status::ready,
          "idle runtime did not wake for an Asio timer");
  const auto stopStart = std::chrono::steady_clock::now();
  runtime.Stop();
  runtime.Join();
  Require(std::chrono::steady_clock::now() - stopStart < 1s,
          "idle runtime shutdown stalled");
}

void CheckImmediateAndWorkerStop() {
  for (int iteration = 0; iteration < 32; ++iteration) {
    servicelib::async::GrpcRuntime runtime({.workers = 1});
    runtime.Start();
    if (iteration % 2 == 0) {
      runtime.Stop();
    } else {
      asio::post(runtime.executor(), [&] { runtime.Stop(); });
    }
    runtime.Join();
    Require(runtime.state() == servicelib::async::GrpcRuntime::State::kStopped,
            "runtime did not stop after startup or from its own worker");
  }
}

void CheckSchedulerIsolation() {
  servicelib::async::GrpcRuntime::Options options;
  options.workers = 2;
  servicelib::async::GrpcRuntime runtime(options);
  runtime.Start();

  std::promise<void> release;
  auto gate = release.get_future().share();
  std::promise<void> businessStarted;
  auto businessReady = businessStarted.get_future();
  std::atomic<int> businessWorkers{0};
  for (int index = 0; index < 2; ++index) {
    boost::asio::post(runtime.executor(), [&] {
      if (businessWorkers.fetch_add(1) == 1) businessStarted.set_value();
      gate.wait();
    });
  }
  const bool businessConcurrent =
      businessReady.wait_for(std::chrono::seconds(2)) == std::future_status::ready;

  std::promise<void> completionsStarted;
  auto completionsReady = completionsStarted.get_future();
  std::atomic<int> completionWorkers{0};
  for (int index = 0; index < 2; ++index) {
    boost::asio::post(runtime.grpcContext().get_executor(), [&] {
      if (completionWorkers.fetch_add(1) == 1) completionsStarted.set_value();
      gate.wait();
    });
  }
  const bool completionsConcurrent =
      completionsReady.wait_for(std::chrono::seconds(2)) == std::future_status::ready;
  release.set_value();
  runtime.Stop();
  runtime.Join();
  Require(businessConcurrent, "Asio business workers must run concurrently");
  Require(completionsConcurrent,
          "Multiple gRPC workers must progress independently of busy Asio workers");
}

asio::awaitable<void> HoldStateUntilShutdown(
    std::shared_ptr<int> state, std::promise<void>& entered) {
  asio::steady_timer timer(co_await asio::this_coro::executor);
  timer.expires_after(1h);
  entered.set_value();
  co_await timer.async_wait(asio::use_awaitable);
  // Keep the state part of the suspended operation, not an unrelated owner.
  Require(*state == 42, "suspended operation state changed");
}

void CheckJoinReleasesSuspendedState() {
  for (const auto workers : {std::size_t{1}, std::size_t{4}}) {
    servicelib::async::GrpcRuntime runtime({.workers = workers});
    runtime.Start();
    auto state = std::make_shared<int>(42);
    std::weak_ptr<int> retained = state;
    std::promise<void> entered;
    auto ready = entered.get_future();
    runtime.SpawnIo(HoldStateUntilShutdown(std::move(state), entered));
    const bool started = ready.wait_for(2s) == std::future_status::ready;
    const bool heldBeforeStop = !retained.expired();
    runtime.Stop();
    runtime.Join();
    Require(started, "shutdown ownership test did not enter its operation");
    Require(heldBeforeStop, "active operation released its state prematurely");
    Require(retained.expired(), "Join retained suspended operation state");
    // A completed shutdown remains safe to repeat, including destruction.
    runtime.Stop();
    runtime.Join();
  }
}

int main() {
  CheckJoinReleasesSuspendedState();
  CheckSchedulerIsolation();
  CheckIdleAndWakeup(1);
  CheckIdleAndWakeup(18);
  CheckImmediateAndWorkerStop();
  std::atomic<int> completed{0};
  servicelib::testmetrics::TestMetrics metrics;
  servicelib::async::GrpcRuntime runtime(
      {.workers = 2, .unhandledException = {}, .metrics = &metrics});
  runtime.Start();

  runtime.SpawnIo(MarkCompleted(completed));
  runtime.SpawnGrpc(MarkCompleted(completed));

  const auto deadline = std::chrono::steady_clock::now() + 2s;
  while (completed.load(std::memory_order_acquire) != 2 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  Require(completed.load(std::memory_order_acquire) == 2,
          "both runtime spawn methods must complete their operations");
  Require(runtime.workers() == 2, "runtime worker count changed");
  Require(runtime.state() == servicelib::async::GrpcRuntime::State::kRunning,
          "runtime stopped before shutdown was requested");
  const auto registered = metrics.registeredNames();
  Require(std::find(registered.begin(), registered.end(),
                    "runtime.active_work") != registered.end(),
          "runtime active-work metric is not registered");
  Require(std::find(registered.begin(), registered.end(),
                    "runtime.event_loop_lag_seconds") != registered.end(),
          "runtime event-loop lag metric is not registered");
  Require(std::find(registered.begin(), registered.end(),
                    "runtime.worker_utilization") != registered.end(),
          "runtime worker utilization metric is not registered");

  runtime.Stop();
  runtime.Join();
  Require(runtime.state() == servicelib::async::GrpcRuntime::State::kStopped,
          "runtime did not finish shutdown");
}

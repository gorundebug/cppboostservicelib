#include <servicelib/runtime/detail/asio_runtime.hpp>
#include <servicelib/runtime/testmetrics/testmetrics.hpp>

#include <boost/asio/post.hpp>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <csignal>
#include <condition_variable>
#include <future>
#include <mutex>
#include <set>
#include <stdexcept>
#include <thread>

int main() {
  bool rejectedZeroWorkers = false;
  try {
    servicelib::async::Runtime invalid(
        {.workers = 0, .unhandledException = {}});
  } catch (const std::invalid_argument&) {
    rejectedZeroWorkers = true;
  }
  assert(rejectedZeroWorkers);

  servicelib::testmetrics::TestMetrics metrics;
  servicelib::async::Runtime runtime(
      {.workers = 2, .unhandledException = {}, .metrics = &metrics});
  runtime.start();
  assert(runtime.state() == servicelib::async::Runtime::State::kRunning);
  assert(runtime.workers() == 2);

  std::mutex mutex;
  std::condition_variable ready;
  std::set<std::thread::id> workerIds;
  int completed = 0;
  for (int index = 0; index < 32; ++index) {
    boost::asio::post(runtime.executor(), [&] {
      {
        std::lock_guard lock(mutex);
        workerIds.insert(std::this_thread::get_id());
        ++completed;
      }
      ready.notify_one();
    });
  }

  {
    std::unique_lock lock(mutex);
    ready.wait(lock, [&] { return completed == 32; });
  }
  assert(!workerIds.empty());
  assert(workerIds.size() <= 2);

  const auto registered = metrics.registeredNames();
  assert(std::find(registered.begin(), registered.end(),
                   "runtime.active_work") != registered.end());
  assert(std::find(registered.begin(), registered.end(),
                   "runtime.event_loop_lag_seconds") != registered.end());
  assert(std::find(registered.begin(), registered.end(),
                   "runtime.worker_utilization") != registered.end());
#if defined(CPPBOOSTSERVICELIB_COROUTINE_DIAGNOSTICS)
  assert(std::find(registered.begin(), registered.end(),
                   "runtime.handler_queued") != registered.end());
  assert(std::find(registered.begin(), registered.end(),
                   "runtime.handler_running") != registered.end());
  assert(std::find(registered.begin(), registered.end(),
                   "runtime.handler_suspended") != registered.end());
#endif

  std::mutex busyMutex;
  std::condition_variable busyCondition;
  int busyHandlers = 0;
  const auto busyUntil =
      std::chrono::steady_clock::now() + std::chrono::milliseconds{250};
  for (int index = 0; index < 2; ++index) {
    boost::asio::post(runtime.executor(), [&] {
      while (std::chrono::steady_clock::now() < busyUntil) {
        std::atomic_signal_fence(std::memory_order_seq_cst);
      }
      {
        std::lock_guard lock(busyMutex);
        ++busyHandlers;
      }
      busyCondition.notify_all();
    });
  }
  {
    std::unique_lock lock(busyMutex);
    assert(busyCondition.wait_for(lock, std::chrono::seconds{2},
                                  [&] { return busyHandlers == 2; }));
  }
  const auto metricsDeadline =
      std::chrono::steady_clock::now() + std::chrono::seconds{2};
  while ((metrics.observableGauge("runtime.active_work", {}).value() != 2 ||
          metrics.observableGauge("runtime.worker_utilization", {}).value() <
              0.5 ||
          metrics.observableGauge("runtime.event_loop_lag_seconds", {}).value() <
              0.02) &&
         std::chrono::steady_clock::now() < metricsDeadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds{1});
  }
  assert(metrics.observableGauge("runtime.active_work", {}).value() == 2);
  assert(metrics.observableGauge("runtime.worker_utilization", {}).value() >=
         0.5);
  assert(metrics.observableGauge("runtime.event_loop_lag_seconds", {}).value() >=
         0.02);

  const auto idleDeadline =
      std::chrono::steady_clock::now() + std::chrono::seconds{2};
  while ((metrics.observableGauge("runtime.active_work", {}).value() != 0 ||
          metrics.observableGauge("runtime.worker_utilization", {}).value() >=
              0.05) &&
         std::chrono::steady_clock::now() < idleDeadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds{1});
  }
  assert(metrics.observableGauge("runtime.active_work", {}).value() == 0);
  assert(metrics.observableGauge("runtime.worker_utilization", {}).value() <
         0.05);

  std::promise<int> receivedSignal;
  auto signalFuture = receivedSignal.get_future();
  runtime.waitForSignals({SIGUSR1},
                         [&](int signal) { receivedSignal.set_value(signal); });
  std::raise(SIGUSR1);
  assert(signalFuture.wait_for(std::chrono::seconds{2}) ==
         std::future_status::ready);
  assert(signalFuture.get() == SIGUSR1);
  assert(runtime.state() == servicelib::async::Runtime::State::kRunning);

  runtime.stop();
  runtime.join();
  assert(runtime.state() == servicelib::async::Runtime::State::kStopped);

  bool registryCleared = false;
  try {
    static_cast<void>(servicelib::detail::ParallelExecutorRegistry::Get());
  } catch (const std::logic_error&) {
    registryCleared = true;
  }
  assert(registryCleared);
}

#include <servicelib/runtime/detail/grpc_runtime.hpp>
#include <servicelib/runtime/testmetrics/testmetrics.hpp>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <atomic>
#include <algorithm>
#include <cassert>
#include <chrono>
#include <thread>

namespace asio = boost::asio;
using namespace std::chrono_literals;

asio::awaitable<void> MarkCompleted(std::atomic<int>& completed) {
  co_await asio::post(asio::use_awaitable);
  completed.fetch_add(1, std::memory_order_release);
}

int main() {
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
  assert(completed.load(std::memory_order_acquire) == 2);
  assert(runtime.workers() == 2);
  assert(runtime.state() == servicelib::async::GrpcRuntime::State::kRunning);
  const auto registered = metrics.registeredNames();
  assert(std::find(registered.begin(), registered.end(),
                   "runtime.active_work") != registered.end());
  assert(std::find(registered.begin(), registered.end(),
                   "runtime.event_loop_lag_seconds") != registered.end());
  assert(std::find(registered.begin(), registered.end(),
                   "runtime.worker_utilization") != registered.end());

  runtime.Stop();
  runtime.Join();
  assert(runtime.state() == servicelib::async::GrpcRuntime::State::kStopped);
}

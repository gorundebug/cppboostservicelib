#pragma once

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/thread_pool.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <ctime>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

#if defined(__linux__)
#include <pthread.h>
#endif

#include <servicelib/runtime/detail/asio_dispatch.hpp>
#if defined(CPPBOOSTSERVICELIB_COROUTINE_DIAGNOSTICS)
#include <servicelib/runtime/detail/asio_handler_diagnostics.hpp>
#endif
#include <servicelib/runtime/environment/metrics/metrics.hpp>

namespace servicelib::async {

namespace runtime_detail {

class RuntimeMetrics final {
 public:
  static std::shared_ptr<RuntimeMetrics> Create(metrics::Metrics& metrics,
                                                std::size_t workers) {
    auto result = std::shared_ptr<RuntimeMetrics>(new RuntimeMetrics(workers));
    auto scope = metrics.scope("runtime", {});
    const std::weak_ptr<RuntimeMetrics> weak = result;
    result->activeWorkGauge_ = scope->observableFloat64Gauge(
        "active_work",
        "Number of event-loop workers that executed CPU work in the latest sample",
        [weak] {
          const auto locked = weak.lock();
          return locked ? static_cast<double>(locked->activeWork()) : 0.0;
        });
    result->eventLoopLagGauge_ = scope->observableFloat64Gauge(
        "event_loop_lag_seconds",
        "Delay between a scheduled event-loop probe and its execution",
        [weak] {
          const auto locked = weak.lock();
          return locked ? locked->eventLoopLagSeconds() : 0.0;
        });
    result->workerUtilizationGauge_ = scope->observableFloat64Gauge(
        "worker_utilization",
        "Fraction of event-loop worker CPU capacity used in the latest sample",
        [weak] {
          const auto locked = weak.lock();
          return locked ? locked->workerUtilization() : 0.0;
        });
#if defined(CPPBOOSTSERVICELIB_COROUTINE_DIAGNOSTICS)
    result->queuedHandlersGauge_ = scope->observableFloat64Gauge(
        "handler_queued",
        "Asio handlers known to be ready but not yet invoked",
        [] {
          return static_cast<double>(asio_handler_diagnostics::Read().queued);
        });
    result->runningHandlersGauge_ = scope->observableFloat64Gauge(
        "handler_running", "Asio completion handlers currently executing",
        [] {
          return static_cast<double>(asio_handler_diagnostics::Read().running);
        });
    result->suspendedHandlersGauge_ = scope->observableFloat64Gauge(
        "handler_suspended",
        "Asio handlers waiting for an asynchronous completion",
        [] {
          return static_cast<double>(
              asio_handler_diagnostics::Read().suspended);
        });
#endif
    return result;
  }

  RuntimeMetrics(const RuntimeMetrics&) = delete;
  RuntimeMetrics& operator=(const RuntimeMetrics&) = delete;

  void observeLag(std::chrono::steady_clock::duration lag) noexcept {
    const auto seconds = std::chrono::duration<double>(lag).count();
    eventLoopLagNanoseconds_.store(
        static_cast<std::int64_t>(seconds * 1'000'000'000.0),
        std::memory_order_relaxed);
  }

  void initializeWorkers(std::vector<std::thread>& workers) noexcept {
#if defined(__linux__)
    workerClocks_.clear();
    workerCpuNanoseconds_.clear();
    workerClocks_.reserve(workers.size());
    workerCpuNanoseconds_.reserve(workers.size());
    for (auto& worker : workers) {
      clockid_t clock{};
      if (::pthread_getcpuclockid(worker.native_handle(), &clock) != 0) {
        workerClocks_.clear();
        workerCpuNanoseconds_.clear();
        return;
      }
      workerClocks_.push_back(clock);
      workerCpuNanoseconds_.push_back(ReadClock(clock));
    }
    lastSample_ = std::chrono::steady_clock::now();
#else
    static_cast<void>(workers);
#endif
  }

  void observeWorkerSample(std::chrono::steady_clock::time_point now) noexcept {
#if defined(__linux__)
    if (workerClocks_.size() != workers_ || lastSample_ == TimePoint{}) return;
    const auto wallNanoseconds =
        std::chrono::duration_cast<std::chrono::nanoseconds>(now - lastSample_)
            .count();
    if (wallNanoseconds <= 0) return;
    std::int64_t totalCpuNanoseconds{};
    std::int64_t activeWorkers{};
    // Ignore the tiny cost of the sampling callback itself. One percent of a
    // sample is well below a saturated worker but keeps an idle runtime at 0.
    const auto activeThreshold = wallNanoseconds / 100;
    for (std::size_t index = 0; index < workerClocks_.size(); ++index) {
      const auto current = ReadClock(workerClocks_[index]);
      const auto delta = std::max<std::int64_t>(
          current - workerCpuNanoseconds_[index], 0);
      workerCpuNanoseconds_[index] = current;
      totalCpuNanoseconds += delta;
      if (delta >= activeThreshold) ++activeWorkers;
    }
    lastSample_ = now;
    const auto utilization = std::clamp(
        static_cast<double>(totalCpuNanoseconds) /
            (static_cast<double>(wallNanoseconds) *
             static_cast<double>(workers_)),
        0.0, 1.0);
    activeWork_.store(activeWorkers, std::memory_order_relaxed);
    workerUtilization_.store(utilization, std::memory_order_relaxed);
#else
    static_cast<void>(now);
#endif
  }

  [[nodiscard]] std::int64_t activeWork() const noexcept {
    return activeWork_.load(std::memory_order_relaxed);
  }

  [[nodiscard]] double eventLoopLagSeconds() const noexcept {
    return static_cast<double>(
               eventLoopLagNanoseconds_.load(std::memory_order_relaxed)) /
           1'000'000'000.0;
  }

  [[nodiscard]] double workerUtilization() const noexcept {
    return workerUtilization_.load(std::memory_order_relaxed);
  }

 private:
  explicit RuntimeMetrics(std::size_t workers) : workers_(workers) {}

#if defined(__linux__)
  using TimePoint = std::chrono::steady_clock::time_point;

  static std::int64_t ReadClock(clockid_t clock) noexcept {
    timespec value{};
    if (::clock_gettime(clock, &value) != 0) return 0;
    return static_cast<std::int64_t>(value.tv_sec) * 1'000'000'000LL +
           static_cast<std::int64_t>(value.tv_nsec);
  }
#endif

  // Keeps the three instruments alive for the lifetime of the runtime. The
  // owning Metrics backend must outlive Runtime, as it already does in the
  // generated main function.
  std::size_t workers_;
  std::atomic<std::int64_t> activeWork_{0};
  std::atomic<std::int64_t> eventLoopLagNanoseconds_{0};
  std::atomic<double> workerUtilization_{0.0};
#if defined(__linux__)
  std::vector<clockid_t> workerClocks_;
  std::vector<std::int64_t> workerCpuNanoseconds_;
  TimePoint lastSample_{};
#endif
  std::unique_ptr<metrics::ObservableFloat64Gauge> activeWorkGauge_;
  std::unique_ptr<metrics::ObservableFloat64Gauge> eventLoopLagGauge_;
  std::unique_ptr<metrics::ObservableFloat64Gauge> workerUtilizationGauge_;
#if defined(CPPBOOSTSERVICELIB_COROUTINE_DIAGNOSTICS)
  std::unique_ptr<metrics::ObservableFloat64Gauge> queuedHandlersGauge_;
  std::unique_ptr<metrics::ObservableFloat64Gauge> runningHandlersGauge_;
  std::unique_ptr<metrics::ObservableFloat64Gauge> suspendedHandlersGauge_;
#endif
};

}  // namespace runtime_detail

// Boost.Asio replacement for the process/runtime mechanics owned by userver.
// It deliberately does not introduce a second ServiceLib lifecycle: generated
// services continue to derive from and be owned by canonical ServiceApp.
class Runtime final {
 public:
  enum class State { kCreated, kRunning, kStopping, kStopped };

  struct Options final {
    std::size_t workers{1};
    std::function<void(std::exception_ptr)> unhandledException;
    metrics::Metrics* metrics{};
  };

  explicit Runtime(Options options)
      : options_(Validate(std::move(options))),
        ioContext_(static_cast<int>(options_.workers)),
        work_(boost::asio::make_work_guard(ioContext_)),
        metrics_(options_.metrics && options_.metrics->enabled()
                     ? runtime_detail::RuntimeMetrics::Create(
                           *options_.metrics, options_.workers)
                     : nullptr),
        executor_(ioContext_.get_executor()) {}

  Runtime(const Runtime&) = delete;
  Runtime& operator=(const Runtime&) = delete;
  ~Runtime() {
    Stop();
    Join();
  }

  void Start() {
    State expected = State::kCreated;
    if (!state_.compare_exchange_strong(expected, State::kRunning)) {
      throw std::logic_error("Asio runtime can only be started once");
    }
    detail::ParallelExecutorRegistry::Set(executor_);
    try {
      blockingPool_ = std::make_unique<boost::asio::thread_pool>(
          BlockingWorkers(options_.workers));
      detail::BlockingExecutorRegistry::Set(blockingPool_->get_executor());
      workers_.reserve(options_.workers);
      for (std::size_t index = 0; index < options_.workers; ++index) {
        workers_.emplace_back([this] { RunWorker(); });
      }
      if (metrics_) metrics_->initializeWorkers(workers_);
      StartMetricsProbe();
    } catch (...) {
      Stop();
      Join();
      throw;
    }
  }

  void start() { Start(); }

  void Stop() noexcept {
    auto state = state_.load(std::memory_order_acquire);
    while (state == State::kCreated || state == State::kRunning) {
      if (state_.compare_exchange_weak(state, State::kStopping)) {
        work_.reset();
        if (metricsTimer_) {
          try {
            metricsTimer_->cancel();
          } catch (...) {
          }
        }
        {
          std::lock_guard lock(signalMutex_);
          if (signalSet_) {
            boost::system::error_code ignored;
            signalSet_->cancel(ignored);
          }
        }
        ioContext_.stop();
        return;
      }
    }
  }

  void stop() noexcept { Stop(); }

  void Join() noexcept {
    std::lock_guard lock(joinMutex_);
    for (auto& worker : workers_) {
      if (worker.joinable()) worker.join();
    }
    workers_.clear();
    if (state_.load(std::memory_order_acquire) == State::kStopping) {
      state_.store(State::kStopped, std::memory_order_release);
    }
    if (blockingPool_) {
      blockingPool_->stop();
      blockingPool_->join();
      blockingPool_.reset();
    }
    detail::BlockingExecutorRegistry::Clear();
    detail::ParallelExecutorRegistry::Clear();
  }

  void join() noexcept { Join(); }

  void waitForSignals(std::vector<int> signals,
                      std::function<void(int)> callback = {}) {
    RequireRunning();
    if (signals.empty()) throw std::invalid_argument("signal list is empty");
    auto signalSet = std::make_unique<boost::asio::signal_set>(ioContext_);
    for (const int signal : signals) signalSet->add(signal);
    auto* set = signalSet.get();
    {
      std::lock_guard lock(signalMutex_);
      if (signalSet_) {
        throw std::logic_error("runtime signal handler is already installed");
      }
      signalSet_ = std::move(signalSet);
    }
    set->async_wait([this, callback = std::move(callback)](
                        const boost::system::error_code& error, int signal) {
      if (error) return;
      if (callback) {
        callback(signal);
      } else {
        Stop();
      }
    });
  }

  template <typename Awaitable>
  void Spawn(Awaitable&& operation) {
    RequireRunning();
    boost::asio::co_spawn(
        executor_, std::forward<Awaitable>(operation),
        [this](std::exception_ptr error) { Report(error); });
  }

  [[nodiscard]] boost::asio::io_context& ioContext() noexcept {
    return ioContext_;
  }
  [[nodiscard]] boost::asio::any_io_executor executor() noexcept {
    return executor_;
  }
  [[nodiscard]] std::size_t workers() const noexcept { return options_.workers; }
  [[nodiscard]] State state() const noexcept {
    return state_.load(std::memory_order_acquire);
  }

 private:
  using Work = boost::asio::executor_work_guard<
      boost::asio::io_context::executor_type>;

  static Options Validate(Options options) {
    if (options.workers == 0) {
      throw std::invalid_argument("Asio runtime workers must be positive");
    }
    return options;
  }

  static std::size_t BlockingWorkers(std::size_t workers) noexcept {
    // This is an isolation pool, not additional request-processing capacity.
    // Keep it bounded while leaving room for one lifetime-long producer and
    // one synchronous completion boundary at the common two-worker setting.
    return std::max<std::size_t>(workers, 2);
  }

  void RequireRunning() const {
    if (state() != State::kRunning) {
      throw std::logic_error("cannot schedule on a stopped Asio runtime");
    }
  }

  void RunWorker() noexcept {
    try {
      ioContext_.run();
    } catch (...) {
      Report(std::current_exception());
      Stop();
    }
  }

  void StartMetricsProbe() {
    if (!metrics_) return;
    metricsTimer_ = std::make_unique<boost::asio::steady_timer>(ioContext_);
    ScheduleMetricsProbe();
  }

  void ScheduleMetricsProbe() {
    if (!metricsTimer_ || state() != State::kRunning) return;
    constexpr auto interval = std::chrono::milliseconds{100};
    const auto expected = std::chrono::steady_clock::now() + interval;
    metricsTimer_->expires_at(expected);
    metricsTimer_->async_wait(
        [this, expected](const boost::system::error_code& error) {
          if (error || state() != State::kRunning) return;
          const auto now = std::chrono::steady_clock::now();
          metrics_->observeWorkerSample(now);
          metrics_->observeLag(now > expected ? now - expected
                                              : decltype(now - expected)::zero());
          ScheduleMetricsProbe();
        });
  }

  void Report(std::exception_ptr error) noexcept {
    if (!error || !options_.unhandledException) return;
    try {
      options_.unhandledException(std::move(error));
    } catch (...) {
    }
  }

  Options options_;
  boost::asio::io_context ioContext_;
  Work work_;
  std::shared_ptr<runtime_detail::RuntimeMetrics> metrics_;
  boost::asio::any_io_executor executor_;
  std::unique_ptr<boost::asio::steady_timer> metricsTimer_;
  std::atomic<State> state_{State::kCreated};
  std::mutex joinMutex_;
  std::mutex signalMutex_;
  std::unique_ptr<boost::asio::signal_set> signalSet_;
  std::vector<std::thread> workers_;
  std::unique_ptr<boost::asio::thread_pool> blockingPool_;
};

}  // namespace servicelib::async

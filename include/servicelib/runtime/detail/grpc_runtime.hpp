#pragma once

#include <utility>

#include <agrpc/grpc_context.hpp>
#include <agrpc/grpc_executor.hpp>
#include <agrpc/run.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/thread_pool.hpp>
#include <grpcpp/server_builder.h>

#include <atomic>
#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <vector>

#include <servicelib/runtime/detail/asio_runtime.hpp>

namespace servicelib::async {

namespace runtime_detail {

struct CooperativeGrpcRunTraits final {
  static constexpr std::chrono::microseconds MAX_LATENCY{50};

  static bool poll(boost::asio::io_context& context) {
    constexpr std::size_t kBatchSize = 8;
    std::size_t processed{};
    while (processed < kBatchSize && context.poll_one() != 0) ++processed;
    return processed != 0;
  }

  template <typename Rep, typename Period>
  static bool run_for(boost::asio::io_context& context,
                      std::chrono::duration<Rep, Period> duration) {
    return context.run_one_for(duration) != 0;
  }

  static bool is_stopped(boost::asio::io_context& context) {
    return context.stopped();
  }
};

class RuntimeIoContext : public boost::asio::io_context {
 public:
  using boost::asio::io_context::io_context;

  // asio-grpc operations dispatched from GrpcContext destruction may own
  // handlers registered in this context. Expose the protected service-shutdown
  // phase so GrpcRuntime can retire those handlers while both contexts still
  // exist, before either execution_context releases its service storage.
  void ShutdownServices() noexcept { shutdown(); }
};

}  // namespace runtime_detail

// gRPC 1.71 enables EventEngine client/listener paths that exhibit severe
// latency spikes and extra background scheduling under the fixed CPU quotas
// used by generated services. Apply the stable transport default before the
// first ServerBuilder/channel initializes gRPC, while preserving an explicit
// user choice verbatim.
inline void ConfigureGrpcRuntimeDefaults() noexcept {
#if defined(__unix__) || defined(__APPLE__)
  if (std::getenv("GRPC_EXPERIMENTS") == nullptr) {
    static_cast<void>(::setenv(
        "GRPC_EXPERIMENTS",
        "-event_engine_client,-event_engine_listener", 0));
  }
#endif
}

// Runs Asio continuations and raw gRPC CompletionQueue events on one fixed-width
// worker set. Bounded Asio batches prevent a continuously-ready graph from
// starving the CompletionQueue while allowing every worker to execute either
// kind of work. All gRPC coroutine completion tokens are associated with the
// shared Asio executor; no request-path wait may block a worker.
class GrpcRuntime final {
 public:
  enum class State { kCreated, kRunning, kStopping, kStopped };
  struct Options final {
    std::size_t workers{1};
    std::function<void(std::exception_ptr)> unhandledException;
    metrics::Metrics* metrics{};
  };

  explicit GrpcRuntime(Options options)
      : GrpcRuntime(std::move(options), nullptr) {}

  GrpcRuntime(Options options,
              std::unique_ptr<grpc::ServerCompletionQueue> serverQueue)
      : options_(Validate(std::move(options))),
        grpcContext_(serverQueue
                         ? std::make_unique<agrpc::GrpcContext>(
                               std::move(serverQueue), options_.workers)
                         : std::make_unique<agrpc::GrpcContext>(options_.workers)),
        ioContext_(static_cast<int>(options_.workers)),
        grpcWork_(boost::asio::make_work_guard(*grpcContext_)),
        ioWork_(boost::asio::make_work_guard(ioContext_)),
        metrics_(options_.metrics && options_.metrics->enabled()
                     ? runtime_detail::RuntimeMetrics::Create(
                           *options_.metrics, options_.workers)
                     : nullptr),
        executor_(ioContext_.get_executor()),
        grpcExecutor_(executor_) {}

  GrpcRuntime(const GrpcRuntime&) = delete;
  GrpcRuntime& operator=(const GrpcRuntime&) = delete;
  ~GrpcRuntime() { Stop(); Join(); }

  static std::unique_ptr<grpc::ServerCompletionQueue> AddCompletionQueue(
      grpc::ServerBuilder& builder) {
    return builder.AddCompletionQueue();
  }

  void Start() {
    State expected = State::kCreated;
    if (!state_.compare_exchange_strong(expected, State::kRunning))
      throw std::logic_error("gRPC runtime can only be started once");
    detail::ParallelExecutorRegistry::Set(executor_);
    try {
      blockingPool_ = std::make_unique<boost::asio::thread_pool>(
          BlockingWorkers(options_.workers));
      detail::BlockingExecutorRegistry::Set(blockingPool_->get_executor());
      workers_.reserve(options_.workers);
      for (std::size_t index = 0; index < options_.workers; ++index)
        workers_.emplace_back([this, index] { RunWorker(index); });
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
        ioWork_.reset();
        grpcWork_.reset();
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
        grpcContext_->stop();
        ioContext_.stop();
        return;
      }
    }
  }

  void stop() noexcept { Stop(); }

  void Join() noexcept {
    std::lock_guard lock(joinMutex_);
    if (joined_) return;
    for (auto& worker : workers_)
      if (worker.joinable()) worker.join();
    workers_.clear();
    if (state_.load() == State::kStopping) state_.store(State::kStopped);
    if (blockingPool_) {
      blockingPool_->stop();
      blockingPool_->join();
      blockingPool_.reset();
    }
    detail::BlockingExecutorRegistry::Clear();
    detail::ParallelExecutorRegistry::Clear();
    {
      std::lock_guard signalLock(signalMutex_);
      signalSet_.reset();
    }
    metricsTimer_.reset();
    grpcExecutor_ = {};
    executor_ = {};
    // Join is the public quiescence boundary: no coroutine completion may
    // retain caller-owned state after it returns. Retire Asio services while
    // GrpcContext is alive, drain/destroy the CQ while the Asio scheduler is
    // alive, then retire completions dispatched by that drain.
    ioContext_.ShutdownServices();
    grpcContext_.reset();
    ioContext_.ShutdownServices();
    joined_ = true;
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
      if (signalSet_)
        throw std::logic_error("runtime signal handler is already installed");
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
  void SpawnIo(Awaitable&& operation) {
    RequireRunning();
    boost::asio::co_spawn(
        executor_, std::forward<Awaitable>(operation),
        [this](std::exception_ptr error) { Report(error); });
  }

  template <typename Awaitable>
  void SpawnGrpc(Awaitable&& operation) {
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
  [[nodiscard]] boost::asio::any_io_executor grpcExecutor() noexcept {
    return grpcExecutor_;
  }
  [[nodiscard]] agrpc::GrpcContext& grpcContext() noexcept {
    return *grpcContext_;
  }
  [[nodiscard]] std::size_t workers() const noexcept { return options_.workers; }
  [[nodiscard]] State state() const noexcept { return state_.load(); }

 private:
  using IoWork = boost::asio::executor_work_guard<
      boost::asio::io_context::executor_type>;
  using GrpcWork = boost::asio::executor_work_guard<
      agrpc::GrpcContext::executor_type>;

  static Options Validate(Options options) {
    if (options.workers == 0)
      throw std::invalid_argument("gRPC runtime workers must be positive");
    return options;
  }
  static std::size_t BlockingWorkers(std::size_t workers) noexcept {
    return std::max<std::size_t>(workers, 2);
  }
  void RequireRunning() const {
    if (state() != State::kRunning)
      throw std::logic_error("cannot spawn on a stopped gRPC runtime");
  }
  void RunWorker(std::size_t) noexcept {
    try {
      const auto stopCondition = [this] {
        return state_.load(std::memory_order_acquire) != State::kRunning;
      };
      agrpc::run_completion_queue<runtime_detail::CooperativeGrpcRunTraits>(
          *grpcContext_, ioContext_, stopCondition);
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
    try { options_.unhandledException(std::move(error)); } catch (...) {}
  }

  Options options_;
  std::unique_ptr<agrpc::GrpcContext> grpcContext_;
  runtime_detail::RuntimeIoContext ioContext_;
  GrpcWork grpcWork_;
  IoWork ioWork_;
  std::shared_ptr<runtime_detail::RuntimeMetrics> metrics_;
  boost::asio::any_io_executor executor_;
  boost::asio::any_io_executor grpcExecutor_;
  std::unique_ptr<boost::asio::steady_timer> metricsTimer_;
  std::atomic<State> state_{State::kCreated};
  std::mutex joinMutex_;
  bool joined_{};
  std::mutex signalMutex_;
  std::unique_ptr<boost::asio::signal_set> signalSet_;
  std::vector<std::thread> workers_;
  std::unique_ptr<boost::asio::thread_pool> blockingPool_;
};

}  // namespace servicelib::async

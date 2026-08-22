/*
 * Config-driven stream execution environment.
 * Graph execution mechanics used by ServiceApp.
 */
#pragma once

#include <algorithm>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <tuple>
#include <unordered_map>
#include <vector>

#include <servicelib/runtime/caller.hpp>
#include <servicelib/runtime/status/status.hpp>

namespace servicelib {

template <typename TStreamApp, typename TDataTypeFactory>
class StreamExecutionEnvironment : public NotCopyableOrMovable,
                                   public IRuntimeEnvironment {
  template <typename, typename, typename>
  friend class Stream;
  template <typename, typename, typename, typename>
  friend class InputStream;
  template <typename, typename>
  friend class StreamApp;

 public:
  using StreamAppType = TStreamApp;
  using DataTypeFactory = TDataTypeFactory;

 private:
  using EnvironmentContext =
      StreamExecutionEnvironment<TStreamApp, TDataTypeFactory>;

  class ExecutionRuntime final : public NotCopyableOrMovable {
   public:
    explicit ExecutionRuntime(EnvironmentContext& environment)
        : environment_(environment) {}

    void runtimeInit() { environment_.initializeRuntime(*this); }
    void runtimeRelease() { environment_.releaseRuntime(*this); }
    EnvironmentContext& getExecutionEnvironment() noexcept {
      return environment_;
    }

   private:
    EnvironmentContext& environment_;
  };

  template <typename App, typename Runtime, typename = void>
  struct HasRuntimeInit : std::false_type {};

  template <typename App, typename Runtime>
  struct HasRuntimeInit<App, Runtime,
                        std::void_t<decltype(std::declval<App&>().runtimeInit(
                            std::declval<Runtime&>()))>> : std::true_type {};

  template <typename Runtime>
  void initializeRuntime(Runtime& runtime) {
    if constexpr (HasRuntimeInit<TStreamApp, Runtime>::value) {
      getApp().runtimeInit(runtime);
    }
  }

  template <typename Runtime>
  void releaseRuntime(Runtime&) {
    releaseStreams();
  }

 public:
  pool::ITaskPool* getTaskPool(const std::string&) override { return nullptr; }
  pool::IPriorityTaskPool* getPriorityTaskPool(const std::string&) override {
    return nullptr;
  }
  std::shared_ptr<const config::RuntimeConfig> getRuntimeConfigSnapshot()
      const override {
    return config::RuntimeConfigRegistry::Snapshot();
  }
  std::shared_ptr<const config::ServiceConfig> getServiceConfigSnapshot()
      const override {
    auto runtime = getRuntimeConfigSnapshot();
    const auto* service = runtime ? runtime->GetOnlyServiceConfig() : nullptr;
    return service ? std::shared_ptr<const config::ServiceConfig>(
                         std::move(runtime), service)
                   : nullptr;
  }
  const std::string& getServiceName() const noexcept override {
    return serviceName_;
  }
  log::Logger& getLogger() override { return log::NoopLogger::instance(); }
  metrics::Metrics& getMetrics() override {
    return metrics::NoopMetrics::instance();
  }
  tracing::Tracing* getTracing() override { return nullptr; }

  void parallel(std::function<void()> task) override {
    {
      std::lock_guard lock(parallelMutex_);
      if (!parallelAccepting_) {
        throw std::logic_error("parallel graph scheduler is stopped");
      }
      ++parallelActive_;
    }
    try {
      detail::ParallelExecutorRegistry::Post(
          [this, task = std::move(task)]() mutable {
            try {
              std::invoke(std::move(task));
            } catch (...) {
              // ParallelCall has no synchronous error channel, matching the
              // canonical goroutine-per-message semantics.
            }
            std::lock_guard lock(parallelMutex_);
            if (--parallelActive_ == 0) parallelDrained_.notify_all();
          });
    } catch (...) {
      std::lock_guard lock(parallelMutex_);
      if (--parallelActive_ == 0) parallelDrained_.notify_all();
      throw;
    }
  }

  void registerStream(std::shared_ptr<StreamBase> stream) override {
    if (!stream) {
      throw std::invalid_argument("registered stream must not be null");
    }
    if (std::find(streams_.begin(), streams_.end(), stream) != streams_.end()) {
      throw StreamException("stream is already registered");
    }
    streams_.push_back(std::move(stream));
  }

  template <typename Value, typename Producer, typename Consumer>
  Caller<Value>* prepareCaller(Producer& producer, Consumer& consumer,
                               std::string sourceNameOverride = {}) {
    const config::LinkID link{
        static_cast<int>(producer.getConfigId()),
        static_cast<int>(consumer.getBase().getConfigId())};
    if (link.from == 0 || link.to == 0) {
      throw StreamException(
          "stream link has no config identity; every logical edge must use "
          "Caller");
    }

    {
      std::shared_lock lock(callersMutex_);
      const auto found = callers_.find(link);
      if (found != callers_.end()) {
        return static_cast<Caller<Value>*>(found->second.get());
      }
    }

    std::unique_lock lock(callersMutex_);
    if (!callers_.contains(link)) {
      callers_.emplace(link,
                       makeCallerFromEnv<Value>(producer, consumer, this, link,
                                                std::move(sourceNameOverride)));
    }
    return static_cast<Caller<Value>*>(callers_.at(link).get());
  }

  template <typename Value, typename Producer, typename Consumer>
  void consume(MessageContext context, Producer& producer, Consumer& consumer,
               Payload<Value> payload) {
    static_cast<void>(consumer);
    if constexpr (requires {
                    producer.hasPreparedCaller();
                    producer.dispatchPrepared(std::move(context),
                                              std::move(payload));
                  }) {
      if (producer.hasPreparedCaller()) {
        producer.dispatchPrepared(std::move(context), std::move(payload));
        return;
      }
    }
    throw StreamException(
        "caller was not prepared during single-threaded topology build");
  }

  static EnvironmentContext& getExecutionEnvironment() { return *instance_; }
  TStreamApp& getApp() noexcept { return static_cast<TStreamApp&>(*this); }
  const TStreamApp& getApp() const noexcept {
    return static_cast<const TStreamApp&>(*this);
  }

  const std::string& getCode() const noexcept { return topologyCode_; }

  std::string makeStatusNetworkDataJson() const {
    const auto runtime = getRuntimeConfigSnapshot();
    if (!runtime) return R"({"nodes":[],"edges":[]})";
    StatusTopologyPrinter topology;
    printTopology(topology);
    return status::MakeNetworkDataJson(*runtime, topology, [this](config::LinkID link) {
      std::shared_lock lock(callersMutex_);
      const auto found = callers_.find(link);
      return found == callers_.end() ? std::int64_t{0}
                                     : found->second->statistics().count();
    });
  }

  std::string makeStatusGraphYaml() const {
    const auto runtime = getRuntimeConfigSnapshot();
    return runtime ? status::MakeGraphYaml(*runtime) : std::string{};
  }

  void printTopology(TopologyPrinter& printer) const {
    std::unordered_set<size_t> visited;
    for (const auto& stream : streams_) {
      stream->printTopology(printer, visited);
    }
  }

 protected:
  StreamExecutionEnvironment() {
    if (instance_ != nullptr) {
      throw StreamException("stream execution environment already exists");
    }
    instance_ = this;
  }

  ~StreamExecutionEnvironment() {
    if (instance_ == this) instance_ = nullptr;
  }

  void startExecutionRuntime() {
    if (activeRuntime_) {
      throw std::logic_error("stream execution runtime is already started");
    }
    {
      std::lock_guard lock(parallelMutex_);
      if (parallelActive_ != 0) {
        throw std::logic_error("parallel graph operations were not drained");
      }
      parallelAccepting_ = true;
    }
    if (const auto service = getServiceConfigSnapshot()) {
      serviceName_ = service->name;
    }
    auto& runtime = getExecutionRuntime<>();
    try {
      runtime.runtimeInit();
      activeRuntime_ = &runtime;
    } catch (...) {
      std::lock_guard lock(parallelMutex_);
      parallelAccepting_ = false;
      throw;
    }
  }

  void stopExecutionRuntime() noexcept {
    if (!activeRuntime_) return;
    auto* runtime = activeRuntime_;
    activeRuntime_ = nullptr;
    {
      std::unique_lock lock(parallelMutex_);
      // Sources and managed pools have already stopped. While active work is
      // draining it may schedule nested ParallelCall operations; once the
      // count reaches zero no producer remains, so admission closes atomically.
      parallelDrained_.wait(lock, [this] { return parallelActive_ == 0; });
      parallelAccepting_ = false;
    }
    runtime->runtimeRelease();
  }

  template <bool Compiled = false, size_t N = 0, typename... Args>
  ExecutionRuntime& getExecutionRuntime(
      const std::array<const char*, N>& = std::array<const char*, N>{},
      std::tuple<Args...>&& = std::tuple<>()) {
    static_assert(!Compiled,
                  "runtime topology compilation was removed; generate the "
                  "config-driven C++ graph directly");
    buildTopology();
    verifyTopology();
    static ExecutionRuntime runtime(*this);
    return runtime;
  }

 private:
  void releaseStreams() noexcept {
    {
      std::unique_lock lock(callersMutex_);
      callers_.clear();
    }
    streams_.clear();
    topologyCode_.clear();
    topologyBuilt_ = false;
  }

  std::mutex parallelMutex_;
  std::condition_variable parallelDrained_;
  std::size_t parallelActive_{};
  bool parallelAccepting_{};

  void buildTopology() {
    if (topologyBuilt_) return;
    StreamBuilderContext context;
    size_t nextId = 0;
    for (const auto& stream : streams_) {
      nextId = stream->buildTopology(context, nextId + 1, nullptr, false);
    }
    for (const auto& link : context.getLinks()) {
      if (link.second->getId() == 0) {
        throw StreamException("link target is not part of the topology");
      }
    }
    topologyCode_ = context.getCode();
    topologyBuilt_ = true;
  }

  void verifyTopology() const {
    StreamVerifyContext context;
    for (const auto& stream : streams_) {
      stream->verifyTopology(context);
    }
  }

  inline static EnvironmentContext* instance_{nullptr};
  std::string serviceName_;
  std::vector<std::shared_ptr<StreamBase>> streams_;
  std::unordered_map<config::LinkID, std::unique_ptr<CallerBase>,
                     config::LinkIDHash>
      callers_;
  mutable std::shared_mutex callersMutex_;
  std::string topologyCode_;
  bool topologyBuilt_{false};
  ExecutionRuntime* activeRuntime_{nullptr};
};

// Service-oriented name used by ServiceApp. StreamExecutionEnvironment is
// retained as the lower-level/legacy spelling for graph-only applications.
template <typename TService, typename TDataTypeFactory>
using ServiceExecutionEnvironment =
    StreamExecutionEnvironment<TService, TDataTypeFactory>;

}  // namespace servicelib

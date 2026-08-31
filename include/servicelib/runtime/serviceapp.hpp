/*
 * Service-wide lifecycle registry.
 * Go analog: runtime.ServiceApp.
 */
#pragma once

#include <array>
#include <chrono>
#include <exception>
#include <functional>
#include <future>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <servicelib/runtime/context.hpp>
#include <servicelib/runtime/detail/asio_dispatch.hpp>
#include <servicelib/runtime/environment.hpp>
#include <servicelib/runtime/pool/delaypool.hpp>
#include <servicelib/runtime/pool/prioritytaskpool.hpp>
#include <servicelib/runtime/pool/taskpool.hpp>

namespace servicelib {

enum class ServiceComponentKind : std::size_t {
  kDataSource,
  kDataSink,
  kStorage,
  kDelayPool,
  kTaskPool,
  kPriorityTaskPool,
  kComponent,
  kCount,
};

// Owns registered runtime objects and applies the same lifecycle boundary as
// Go ServiceApp. Service-specific generated code constructs the graph and
// registers objects; it never starts individual endpoints or pools.
class ServiceLifecycle final {
 public:
  ServiceLifecycle() = default;
  ServiceLifecycle(const ServiceLifecycle&) = delete;
  ServiceLifecycle& operator=(const ServiceLifecycle&) = delete;

  template <typename T>
  void add(ServiceComponentKind kind, std::shared_ptr<T> component,
           metrics::Metrics* telemetryMetrics = nullptr,
           log::Logger* telemetryLogger = nullptr) {
    if (!component) {
      throw std::invalid_argument("registered service component is null");
    }
    if (state_ != State::kCreated) {
      throw std::logic_error(
          "service components must be registered before start");
    }

    auto& entries = entries_[index(kind)];
    const void* identity = component.get();
    for (const auto& entry : entries) {
      if (entry.identity == identity) {
        throw std::logic_error("service component is already registered");
      }
    }
    auto onStopTimeout = makeStopTimeoutCallback(
        kind, *component, entries.size(), telemetryMetrics, telemetryLogger);
    entries.push_back(Entry{
        .name = componentName(kind, *component, entries.size()),
        .identity = identity,
        .owner = component,
        .start = [component](
                     Context context) { component->start(std::move(context)); },
        .stop = [component](
                    Context context) { component->stop(std::move(context)); },
        .onStopTimeout = std::move(onStopTimeout)});
  }

  void start(Context context) {
    if (state_ != State::kCreated) {
      throw std::logic_error("service lifecycle is already started");
    }

    state_ = State::kStarting;
    try {
      for (const auto kind : kStartOrder) {
        for (auto& entry : entries_[index(kind)]) {
          entry.start(context);
          started_.push_back(&entry);
        }
      }
      state_ = State::kRunning;
    } catch (...) {
      const auto error = std::current_exception();
      stopStarted(context);
      state_ = State::kStopped;
      std::rethrow_exception(error);
    }
  }

  void stop(Context context,
            log::Logger& logger = log::NoopLogger::instance()) {
    stopBeforeGraphDrain(context, logger);
    stopAfterGraphDrain(context, logger);
  }

  void stopBeforeGraphDrain(
      Context context, log::Logger& logger = log::NoopLogger::instance()) {
    if (state_ == State::kCreated) {
      state_ = State::kStopped;
      clear();
      return;
    }
    if (state_ == State::kStopped) return;
    if (state_ != State::kRunning) {
      throw std::logic_error("service lifecycle transition is in progress");
    }

    state_ = State::kStopping;
    std::vector<Entry*> firstPhase;
    for (const auto kind : kStopOrder) {
      if (kind == ServiceComponentKind::kDataSink) continue;
      appendReverse(firstPhase, entries_[index(kind)]);
    }
    stopPhase(context, logger, firstPhase);
  }

  void stopAfterGraphDrain(
      Context context, log::Logger& logger = log::NoopLogger::instance()) {
    if (state_ == State::kStopped) return;
    if (state_ != State::kStopping) {
      throw std::logic_error(
          "service lifecycle graph drain phase has not started");
    }
    std::vector<Entry*> sinks;
    appendReverse(sinks, entries_[index(ServiceComponentKind::kDataSink)]);
    stopPhase(context, logger, sinks);

    state_ = State::kStopped;
    clear();
  }

 private:
  enum class State { kCreated, kStarting, kRunning, kStopping, kStopped };

  struct Entry final {
    std::string name;
    const void* identity{};
    std::shared_ptr<void> owner;
    std::function<void(Context)> start;
    std::function<void(Context)> stop;
    std::function<void()> onStopTimeout;
  };

  static constexpr std::size_t index(ServiceComponentKind kind) noexcept {
    return static_cast<std::size_t>(kind);
  }

  static constexpr std::string_view kindName(
      ServiceComponentKind kind) noexcept {
    switch (kind) {
      case ServiceComponentKind::kDataSource:
        return "datasource";
      case ServiceComponentKind::kDataSink:
        return "datasink";
      case ServiceComponentKind::kStorage:
        return "storage";
      case ServiceComponentKind::kDelayPool:
        return "delay_pool";
      case ServiceComponentKind::kTaskPool:
        return "task_pool";
      case ServiceComponentKind::kPriorityTaskPool:
        return "priority_task_pool";
      case ServiceComponentKind::kComponent:
        return "component";
      case ServiceComponentKind::kCount:
        break;
    }
    return "unknown";
  }

  template <typename T>
  static std::string componentName(ServiceComponentKind kind,
                                   const T& component, std::size_t index) {
    const auto prefix = std::string(kindName(kind)) + ":";
    if constexpr (requires { component.getName(); }) {
      return prefix + std::string(component.getName());
    } else if constexpr (requires { component.id(); }) {
      return prefix + std::to_string(component.id());
    } else {
      return prefix + std::to_string(index);
    }
  }

  template <typename T>
  static std::string connectorName(const T& component, std::size_t index) {
    if constexpr (requires { component.getName(); }) {
      return std::string(component.getName());
    } else if constexpr (requires { component.config().name; }) {
      return std::string(component.config().name);
    } else if constexpr (requires { component.id(); }) {
      return std::to_string(component.id());
    } else {
      return std::to_string(index);
    }
  }

  template <typename T>
  static std::function<void()> makeStopTimeoutCallback(
      ServiceComponentKind kind, const T& component, std::size_t index,
      metrics::Metrics* telemetryMetrics, log::Logger* telemetryLogger) {
    if (!telemetryMetrics || !telemetryLogger ||
        (kind != ServiceComponentKind::kDataSource &&
         kind != ServiceComponentKind::kDataSink)) {
      return {};
    }

    const bool isSource = kind == ServiceComponentKind::kDataSource;
    auto connector = connectorName(component, index);
    auto scope = telemetryMetrics->scope(
        isSource ? "datasource_connector" : "datasink_connector",
        {{"connector", connector}});
    auto counter = scope->counter(
        "events_total",
        isSource ? "Total number of events in data source connector"
                 : "Total number of events in data sink connector",
        {{"event", "stop_timeout"}});
    auto sharedCounter =
        std::shared_ptr<metrics::Int64Counter>(std::move(counter));

    return [isSource, connector = std::move(connector), telemetryLogger,
            counter = std::move(sharedCounter)]() noexcept {
      try {
        telemetryLogger->warn(isSource ? "data source stopped by timeout"
                                       : "data sink stopped by timeout",
                              {log::Field::Str("name", connector)});
      } catch (...) {
        // Telemetry must never interrupt lifecycle cleanup.
      }
      try {
        counter->inc();
      } catch (...) {
        // Telemetry must never interrupt lifecycle cleanup.
      }
    };
  }

  static void appendReverse(std::vector<Entry*>& target,
                            std::vector<Entry>& entries) {
    for (auto it = entries.rbegin(); it != entries.rend(); ++it) {
      target.push_back(&*it);
    }
  }

  static void logFailure(log::Logger& logger, std::string_view message,
                         const Entry& entry,
                         std::string_view error = {}) noexcept {
    try {
      if (error.empty()) {
        logger.warn(message, {log::Field::Str("resource", entry.name)});
      } else {
        logger.warn(message, {log::Field::Str("resource", entry.name),
                              log::Field::Err(error)});
      }
    } catch (...) {
      // Telemetry must never interrupt lifecycle cleanup.
    }
  }

  static void stopPhase(Context context, log::Logger& logger,
                        const std::vector<Entry*>& entries) {
    if (entries.empty()) return;

    std::vector<std::future<std::exception_ptr>> tasks;
    tasks.reserve(entries.size());
    for (auto* entry : entries) {
      // stop() may synchronously join the component's own blocking-executor
      // work. A per-component control task preserves canonical parallel stop
      // semantics without occupying a reactor worker or deadlocking the same
      // bounded executor that is being drained.
      auto stop = entry->stop;
      auto owner = entry->owner;
      tasks.push_back(std::async(
          std::launch::async,
          [stop = std::move(stop), owner = std::move(owner),
           context]() mutable -> std::exception_ptr {
            static_cast<void>(owner);
            try {
              stop(std::move(context));
              return {};
            } catch (...) {
              return std::current_exception();
            }
          }));
    }

    bool ready = true;
    if (context.deadline()) {
      for (auto& task : tasks) {
        if (task.wait_until(*context.deadline()) != std::future_status::ready) {
          ready = false;
        }
      }
    } else {
      for (auto& task : tasks) task.wait();
    }

    if (!ready) {
      for (std::size_t i = 0; i < tasks.size(); ++i) {
        if (tasks[i].wait_for(std::chrono::steady_clock::duration::zero()) !=
            std::future_status::ready) {
          if (entries[i]->onStopTimeout) {
            entries[i]->onStopTimeout();
          } else {
            logFailure(logger, "service shutdown operation timed out",
                       *entries[i]);
          }
        }
      }
    }

    // Never release component or graph ownership while stop() is still using
    // it. The deadline is reported above; the process supervisor remains the
    // hard upper bound if foreign or user code refuses to quiesce.
    for (auto& task : tasks) task.wait();

    for (std::size_t i = 0; i < tasks.size(); ++i) {
      const auto error = tasks[i].get();
      if (!error) continue;
      try {
        std::rethrow_exception(error);
      } catch (const std::exception& ex) {
        logFailure(logger, "service shutdown operation failed", *entries[i],
                   ex.what());
      } catch (...) {
        logFailure(logger, "service shutdown operation failed", *entries[i],
                   "unknown exception");
      }
    }
  }

  void stopStarted(Context context) noexcept {
    for (auto it = started_.rbegin(); it != started_.rend(); ++it) {
      try {
        (*it)->stop(context);
      } catch (...) {
      }
    }
    clear();
  }

  void clear() noexcept {
    started_.clear();
    for (auto& entries : entries_) entries.clear();
  }

  inline static constexpr std::array kStartOrder{
      ServiceComponentKind::kStorage,
      ServiceComponentKind::kDelayPool,
      ServiceComponentKind::kTaskPool,
      ServiceComponentKind::kPriorityTaskPool,
      ServiceComponentKind::kComponent,
      ServiceComponentKind::kDataSink,
      ServiceComponentKind::kDataSource};

  // Stop admission first. Sinks stop last so already accepted source work can
  // still flush results, matching Go ServiceApp's two-phase shutdown.
  inline static constexpr std::array kStopOrder{
      ServiceComponentKind::kDataSource,       ServiceComponentKind::kComponent,
      ServiceComponentKind::kPriorityTaskPool, ServiceComponentKind::kTaskPool,
      ServiceComponentKind::kDelayPool,        ServiceComponentKind::kStorage,
      ServiceComponentKind::kDataSink};

  std::array<std::vector<Entry>,
             static_cast<std::size_t>(ServiceComponentKind::kCount)>
      entries_;
  std::vector<Entry*> started_;
  State state_{State::kCreated};
};

// The stream execution environment contains only graph/runtime mechanics.
// ServiceApp adds the service-wide ownership and lifecycle boundary represented
// by runtime.ServiceApp in Go. Generated services derive from this class.
template <typename TService, typename TDataTypeFactory>
class ServiceApp
    : public ServiceExecutionEnvironment<TService, TDataTypeFactory>,
      public status::Provider {
 public:
  using ExecutionEnvironment =
      servicelib::ServiceExecutionEnvironment<TService, TDataTypeFactory>;

  pool::ITaskPool* getTaskPool(const std::string& name) override {
    ensureConfiguredPools();
    const auto found = taskPools_.find(name);
    return found == taskPools_.end() ? nullptr : found->second.get();
  }

  pool::IPriorityTaskPool* getPriorityTaskPool(
      const std::string& name) override {
    ensureConfiguredPools();
    const auto found = priorityTaskPools_.find(name);
    return found == priorityTaskPools_.end() ? nullptr : found->second.get();
  }

  void delay(Context context, pool::IDelayPool::Duration duration,
             std::function<void()> task) override {
    if (!delayPool_) {
      throw std::logic_error("service delay pool is not registered");
    }
    delayPool_->delay(std::move(context), duration, std::move(task));
  }

  template <typename T>
  void registerDataSource(std::shared_ptr<T> source) {
    lifecycle_.add(ServiceComponentKind::kDataSource, std::move(source),
                   &this->getMetrics(), &this->getLogger());
  }

  template <typename T>
  void registerDataSink(std::shared_ptr<T> sink) {
    lifecycle_.add(ServiceComponentKind::kDataSink, std::move(sink),
                   &this->getMetrics(), &this->getLogger());
  }

  template <typename T>
  void registerStorage(std::shared_ptr<T> storage) {
    lifecycle_.add(ServiceComponentKind::kStorage, std::move(storage));
  }

  void registerDelayPool(std::shared_ptr<pool::IDelayPool> pool) {
    if (delayPool_) throw std::logic_error("delay pool is already registered");
    delayPool_ = pool;
    lifecycle_.add(ServiceComponentKind::kDelayPool, std::move(pool));
  }

  void registerTaskPool(std::shared_ptr<pool::ITaskPool> pool) {
    if (!pool) throw std::invalid_argument("task pool is null");
    const auto name = pool->getName();
    if (!taskPools_.emplace(name, pool).second) {
      throw std::logic_error("duplicate task pool: " + name);
    }
    lifecycle_.add(ServiceComponentKind::kTaskPool, std::move(pool));
  }

  void registerPriorityTaskPool(std::shared_ptr<pool::IPriorityTaskPool> pool) {
    if (!pool) throw std::invalid_argument("priority task pool is null");
    const auto name = pool->getName();
    if (!priorityTaskPools_.emplace(name, pool).second) {
      throw std::logic_error("duplicate priority task pool: " + name);
    }
    lifecycle_.add(ServiceComponentKind::kPriorityTaskPool, std::move(pool));
  }

  template <typename T>
  void addComponent(std::shared_ptr<T> component) {
    lifecycle_.add(ServiceComponentKind::kComponent, std::move(component));
  }

  void start(Context context = {}) {
    if (running_) throw std::logic_error("service is already started");

    ensureServiceInfoMetric();
    ensureConfiguredPools();
    this->startExecutionRuntime();
    try {
      status::Registry::Register(*this);
      lifecycle_.start(std::move(context));
      running_ = true;
    } catch (...) {
      status::Registry::Unregister(*this);
      this->stopExecutionRuntime();
      releaseOwnedRuntimeObjects();
      throw;
    }
  }

  void stop(Context context = {}) {
    if (!running_) return;
    running_ = false;
    status::Registry::Unregister(*this);

    std::exception_ptr lifecycleError;
    try {
      if (const auto service = this->getServiceConfigSnapshot();
          service && service->shutdownTimeout > 0) {
        context = context.bounded(
            std::chrono::milliseconds{service->shutdownTimeout});
      }
      lifecycle_.stopBeforeGraphDrain(context, this->getLogger());
    } catch (...) {
      lifecycleError = std::current_exception();
    }
    // First drain ordinary graph work while callers and streams are still
    // alive. Sink completion may itself emit result/error values, so sinks
    // must quiesce before the execution runtime is released.
    if (!this->drainExecutionRuntime(context)) {
      try {
        this->getLogger().warn("service graph drain timed out");
      } catch (...) {
      }
    }
    try {
      lifecycle_.stopAfterGraphDrain(context, this->getLogger());
    } catch (...) {
      if (!lifecycleError) lifecycleError = std::current_exception();
    }
    this->stopExecutionRuntime();
    releaseOwnedRuntimeObjects();
    if (lifecycleError) std::rethrow_exception(lifecycleError);
  }

  bool isRunning() const noexcept { return running_; }

  std::string networkDataJson() const override {
    return this->makeStatusNetworkDataJson();
  }

  std::string graphYaml() const override { return this->makeStatusGraphYaml(); }

 protected:
  ServiceApp() = default;
  ~ServiceApp() = default;

 protected:
  // Generated services build callers before ServiceApp::start(). Pools used by
  // those callers therefore have to be materialized after the immutable
  // runtime configuration is published and before graph construction.
  void ensureConfiguredPools() {
    if (!delayPool_) {
      registerDelayPool(
          std::shared_ptr<pool::IDelayPool>(pool::makeDelayPool(*this)));
    }

    const auto runtimeConfig = this->getRuntimeConfigSnapshot();
    if (!runtimeConfig) {
      throw std::logic_error("runtime config is not published");
    }

    const auto ensureCallSemantics =
        [this](const config::CallSemanticsGroup& semantics) {
          if (semantics.taskPool.has_value()) {
            const auto& name = semantics.taskPool->poolName;
            if (name.empty()) {
              throw std::invalid_argument(
                  "task pool call semantics requires poolName");
            }
            if (!taskPools_.contains(name)) {
              registerTaskPool(std::shared_ptr<pool::ITaskPool>(
                  pool::makeTaskPool(name, *this)));
            }
          }

          if (semantics.priorityTaskPool.has_value()) {
            const auto& name = semantics.priorityTaskPool->poolName;
            if (name.empty()) {
              throw std::invalid_argument(
                  "priority task pool call semantics requires poolName");
            }
            if (!priorityTaskPools_.contains(name)) {
              registerPriorityTaskPool(std::shared_ptr<pool::IPriorityTaskPool>(
                  pool::makePriorityTaskPool(name, *this)));
            }
          }
        };

    if (const auto service = this->getServiceConfigSnapshot();
        service && service->defaultCallSemantics.has_value()) {
      ensureCallSemantics(*service->defaultCallSemantics);
    }
    for (const auto* link : runtimeConfig->GetConfig().GetLinks()) {
      if (link && link->callSemantics.has_value()) {
        ensureCallSemantics(*link->callSemantics);
      }
    }
  }

 private:
  static std::string environmentName(api::Environment environment) {
    switch (environment) {
      case api::Environment::kLocal:
        return "local";
      case api::Environment::kDebug:
        return "debug";
      case api::Environment::kStaging:
        return "staging";
      case api::Environment::kProduction:
        return "production";
      case api::Environment::kUndefined:
        return {};
    }
    return {};
  }

  void ensureServiceInfoMetric() {
    if (serviceInfoGauge_) return;
    const auto service = this->getServiceConfigSnapshot();
    if (!service) return;
    auto scope = this->getMetrics().scope(
        "service", metrics::Labels{{"service", service->name},
                                   {"environment",
                                    environmentName(service->environment)}});
    serviceInfoGauge_ =
        scope->gauge("info", "Service information (value is always 1)");
    try {
      serviceInfoGauge_->set(1);
    } catch (...) {
      // Telemetry must not turn an otherwise valid service startup into a
      // failure after the instrument has been created.
    }
  }

  void releaseOwnedRuntimeObjects() noexcept {
    delayPool_.reset();
    taskPools_.clear();
    priorityTaskPools_.clear();
  }

  ServiceLifecycle lifecycle_;
  std::shared_ptr<pool::IDelayPool> delayPool_;
  std::unordered_map<std::string, std::shared_ptr<pool::ITaskPool>> taskPools_;
  std::unordered_map<std::string, std::shared_ptr<pool::IPriorityTaskPool>>
      priorityTaskPools_;
  std::unique_ptr<metrics::Int64Gauge> serviceInfoGauge_;
  bool running_{false};
};

}  // namespace servicelib

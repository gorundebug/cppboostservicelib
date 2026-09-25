#include <servicelib/runtime/serviceapp.hpp>
#include <servicelib/runtime/testlog/testlog.hpp>
#include <servicelib/runtime/testmetrics/testmetrics.hpp>

#include <algorithm>
#include <atomic>
#include <source_location>
#include <chrono>
#include <concepts>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "test_async.hpp"

namespace {

test_async::AsioRuntime asioRuntime;

void Require(bool condition,
             const std::source_location location = std::source_location::current()) {
  if (!condition) {
    throw std::runtime_error(std::string(location.file_name()) + ":" +
                             std::to_string(location.line()) +
                             ": service lifecycle assertion failed");
  }
}

struct ServiceDataTypes final {
  template <typename>
  struct DataType {};
};
class Service final : public servicelib::ServiceApp<Service, ServiceDataTypes> {};
class RuntimeOwnerProbe final
    : public servicelib::ServiceApp<RuntimeOwnerProbe, ServiceDataTypes> {
 public:
  bool runtimeBelongsToThisInstance() {
    return &getExecutionRuntime<>().getExecutionEnvironment() == this;
  }
};
static_assert(std::derived_from<servicelib::IRuntimeEnvironment,
                                servicelib::IServiceEnvironment>);
static_assert(std::derived_from<Service, servicelib::IRuntimeEnvironment>);
static_assert(std::derived_from<Service, servicelib::IServiceEnvironment>);
static_assert(std::derived_from<
              Service,
              servicelib::ServiceExecutionEnvironment<Service,
                                                       ServiceDataTypes>>);

struct EventLog final {
  void add(std::string event) {
    std::lock_guard lock(mutex);
    events.push_back(std::move(event));
  }
  std::vector<std::string> snapshot() const {
    std::lock_guard lock(mutex);
    return events;
  }
  mutable std::mutex mutex;
  std::vector<std::string> events;
};

class PriorityPoolConfig final : public servicelib::config::IConfig {
 public:
  PriorityPoolConfig()
      : pool_{.name = "Default Pool", .executorsCount = 1},
        link_{.from = 1,
              .to = 2,
              .callSemantics = servicelib::config::MakeCallSemanticsGroup(
                  servicelib::api::CallSemantics::kPriorityTaskPool,
                  "Default Pool", 1)} {}

  std::vector<const servicelib::config::ServiceConfig*> GetServices()
      const override {
    return {};
  }
  std::vector<servicelib::config::StreamConfigRef> GetStreams()
      const override {
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
  std::vector<const servicelib::config::PoolConfig*> GetPools()
      const override {
    return {&pool_};
  }
  std::vector<const servicelib::config::LinkConfig*> GetLinks()
      const override {
    return {&link_};
  }
  std::vector<const servicelib::config::ModuleConfig*> GetModules()
      const override {
    return {};
  }
  std::vector<const servicelib::config::TypeConfig*> GetTypes()
      const override {
    return {};
  }

 private:
  servicelib::config::PoolConfig pool_;
  servicelib::config::LinkConfig link_;
};

class ServiceInfoConfig final : public servicelib::config::IConfig {
 public:
  explicit ServiceInfoConfig(std::string name = "Metrics Service") {
    service_.id = 1;
    service_.name = std::move(name);
    service_.environment = servicelib::api::Environment::kDebug;
  }

  std::vector<const servicelib::config::ServiceConfig*> GetServices()
      const override {
    return {&service_};
  }
  std::vector<servicelib::config::StreamConfigRef> GetStreams()
      const override {
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
  std::vector<const servicelib::config::PoolConfig*> GetPools()
      const override {
    return {};
  }
  std::vector<const servicelib::config::LinkConfig*> GetLinks()
      const override {
    return {};
  }
  std::vector<const servicelib::config::ModuleConfig*> GetModules()
      const override {
    return {};
  }
  std::vector<const servicelib::config::TypeConfig*> GetTypes()
      const override {
    return {};
  }

 private:
  servicelib::config::ServiceConfig service_;
};

struct TelemetryDataTypes final {
  template <typename>
  struct DataType {};
};

class TelemetryService final
    : public servicelib::ServiceApp<TelemetryService, TelemetryDataTypes> {
 public:
  explicit TelemetryService(servicelib::testmetrics::TestMetrics& metrics)
      : metrics_(metrics) {}
  servicelib::metrics::Metrics& getMetrics() override { return metrics_; }

 private:
  servicelib::testmetrics::TestMetrics& metrics_;
};

struct Component final {
  std::string name;
  EventLog* events{};
  bool failStart{};
  bool failStop{};
  std::chrono::milliseconds stopDelay{};
  std::atomic<bool>* stopped{};
  std::shared_future<void> stopGate;

  void start(servicelib::Context) {
    events->add(name + ":start");
    if (failStart) throw std::runtime_error("start failed");
  }
  void stop(servicelib::Context) {
    if (stopGate.valid()) stopGate.wait();
    if (stopDelay > std::chrono::milliseconds::zero()) {
      std::this_thread::sleep_for(stopDelay);
    }
    events->add(name + ":stop");
    if (stopped) stopped->store(true);
    if (failStop) throw std::runtime_error("stop failed");
  }
};

struct RecordingLogger final : servicelib::log::Logger {
  struct Record final {
    std::string message;
    std::string resource;
    std::string error;
  };
  void debug(std::string_view,
             std::initializer_list<servicelib::log::Field>) override {}
  void info(std::string_view,
            std::initializer_list<servicelib::log::Field>) override {}
  void error(std::string_view,
             std::initializer_list<servicelib::log::Field>) override {}
  void warn(std::string_view message,
            std::initializer_list<servicelib::log::Field> fields) override {
    Record record{.message = std::string(message)};
    for (const auto& field : fields) {
      if (field.key() == "resource") record.resource = field.stringValue();
      if (field.key() == "error") record.error = field.stringValue();
    }
    records.push_back(std::move(record));
  }
  std::vector<Record> records;
};

void startsAndStopsInServiceOrder() {
  EventLog events;
  servicelib::ServiceLifecycle lifecycle;
  lifecycle.add(servicelib::ServiceComponentKind::kStorage,
                std::make_shared<Component>("storage", &events));
  lifecycle.add(servicelib::ServiceComponentKind::kTaskPool,
                std::make_shared<Component>("task", &events));
  lifecycle.add(servicelib::ServiceComponentKind::kPriorityTaskPool,
                std::make_shared<Component>("priority", &events));
  lifecycle.add(servicelib::ServiceComponentKind::kComponent,
                std::make_shared<Component>("component", &events));
  lifecycle.add(servicelib::ServiceComponentKind::kDataSink,
                std::make_shared<Component>("sink", &events));
  lifecycle.add(servicelib::ServiceComponentKind::kDelayPool,
                std::make_shared<Component>("delay", &events));
  lifecycle.add(servicelib::ServiceComponentKind::kDataSource,
                std::make_shared<Component>("source", &events));
  lifecycle.start({});
  lifecycle.stopBeforeGraphDrain({});
  const auto beforeGraphDrain = events.snapshot();
  Require(std::find(beforeGraphDrain.begin(), beforeGraphDrain.end(),
                   "sink:stop") == beforeGraphDrain.end());
  lifecycle.stopAfterGraphDrain({});
  const auto recorded = events.snapshot();
  Require(recorded.size() == 14);
  Require((std::vector(recorded.begin(), recorded.begin() + 7) ==
          std::vector<std::string>{"storage:start", "delay:start",
                                   "task:start", "priority:start",
                                   "component:start", "sink:start",
                                   "source:start"}));
  Require(recorded.back() == "sink:stop");
  Require(std::find(recorded.begin() + 7, recorded.end(), "source:stop") !=
         recorded.end());
  Require(std::find(recorded.begin() + 7, recorded.end(), "delay:stop") !=
         recorded.end());
  Require(std::find(recorded.begin() + 7, recorded.end(), "source:stop") <
         std::find(recorded.begin() + 7, recorded.end(), "delay:stop"));
}

void rollsBackStartedComponents() {
  EventLog events;
  servicelib::ServiceLifecycle lifecycle;
  lifecycle.add(servicelib::ServiceComponentKind::kDataSource,
                std::make_shared<Component>("source", &events, true));
  lifecycle.add(servicelib::ServiceComponentKind::kDataSink,
                std::make_shared<Component>("sink", &events));
  lifecycle.add(servicelib::ServiceComponentKind::kDataSink,
                std::make_shared<Component>("failing-sink", &events, false, true));
  lifecycle.add(servicelib::ServiceComponentKind::kStorage,
                std::make_shared<Component>("storage", &events, false, true));
  bool failed = false;
  try {
    lifecycle.start({});
  } catch (const std::runtime_error& error) {
    failed = true;
    Require(std::string_view(error.what()) == "start failed");
  }
  Require(failed);
  Require((events.snapshot() ==
          std::vector<std::string>{"storage:start", "sink:start",
                                   "failing-sink:start", "source:start",
                                   "failing-sink:stop", "sink:stop",
                                   "storage:stop"}));
  const auto afterRollback = events.snapshot();
  lifecycle.stop({});
  Require(events.snapshot() == afterRollback);
}

void stopFailureDoesNotSkipResources() {
  EventLog events;
  RecordingLogger logger;
  servicelib::ServiceLifecycle lifecycle;
  lifecycle.add(servicelib::ServiceComponentKind::kComponent,
                std::make_shared<Component>("healthy", &events));
  lifecycle.add(servicelib::ServiceComponentKind::kComponent,
                std::make_shared<Component>("failing", &events, false, true));
  lifecycle.start({});
  lifecycle.stop({}, logger);
  const auto recorded = events.snapshot();
  Require(std::find(recorded.begin(), recorded.end(), "healthy:stop") !=
         recorded.end());
  Require(std::find(recorded.begin(), recorded.end(), "failing:stop") !=
         recorded.end());
  Require(logger.records.size() == 1);
  Require(logger.records.front().message ==
         "service shutdown operation failed");
  Require(logger.records.front().resource == "component:1");
  Require(logger.records.front().error == "stop failed");
}

void deadlineDiagnosesButDoesNotReleaseLiveResource() {
  EventLog events;
  RecordingLogger logger;
  std::atomic<bool> stopped{false};
  std::promise<void> release;
  servicelib::ServiceLifecycle lifecycle;
  lifecycle.add(servicelib::ServiceComponentKind::kComponent,
                std::make_shared<Component>(
                    "slow", &events, false, false,
                    std::chrono::milliseconds{0}, &stopped,
                    release.get_future().share()));
  lifecycle.start({});
  const auto started = std::chrono::steady_clock::now();
  lifecycle.stop(servicelib::Context{}.bounded(std::chrono::milliseconds{1}),
                 logger);
  Require(std::chrono::steady_clock::now() - started < std::chrono::seconds{2});
  Require(!stopped.load());
  Require(logger.records.size() == 1);
  Require(logger.records.front().message ==
         "service shutdown operation timed out");
  Require(logger.records.front().resource == "component:0");
  release.set_value();
  lifecycle.finishShutdown(logger);
  Require(stopped.load());
}

void connectorTimeoutMatchesTelemetry() {
  EventLog events;
  std::atomic<bool> stopped{false};
  std::promise<void> release;
  servicelib::testmetrics::TestMetrics metrics;
  servicelib::testlog::TestLog logger;
  servicelib::ServiceLifecycle lifecycle;
  lifecycle.add(servicelib::ServiceComponentKind::kDataSource,
                std::make_shared<Component>(
                    "source", &events, false, false,
                    std::chrono::milliseconds{0}, &stopped,
                    release.get_future().share()),
                &metrics, &logger);
  lifecycle.start({});
  lifecycle.stop(servicelib::Context{}.bounded(std::chrono::milliseconds{1}),
                 logger);
  Require(!stopped.load());
  Require(metrics
             .counter("datasource_connector.events_total",
                      {{"connector", "0"}, {"event", "stop_timeout"}})
             .count() == 1);
  const auto entries = logger.entries();
  Require(entries.size() == 1);
  Require(entries.front().message == "data source stopped by timeout");
  release.set_value();
  lifecycle.finishShutdown(logger);
  Require(stopped.load());
}

void preparesConfiguredPoolsBeforeGraphConstruction() {
  PriorityPoolConfig config;
  servicelib::config::RuntimeConfigRegistry::Publish(
      std::make_shared<const servicelib::config::RuntimeConfig>(config));
  {
    Service service;
    Require(service.getPriorityTaskPool("Default Pool") != nullptr);
  }
  servicelib::config::RuntimeConfigRegistry::Publish({});
}

void exposesCanonicalServiceInfoMetric() {
  ServiceInfoConfig config;
  servicelib::config::RuntimeConfigRegistry::Publish(
      std::make_shared<const servicelib::config::RuntimeConfig>(config));
  servicelib::testmetrics::TestMetrics metrics;
  {
    TelemetryService service(metrics);
    service.start();
    Require(metrics
               .gauge("service.info",
                      {{"service", "Metrics Service"},
                       {"environment", "debug"}})
               .value() == 1);
    service.stop();
  }
  servicelib::config::RuntimeConfigRegistry::Publish({});
}

void runtimeConfigSnapshotOwnsConcreteConfigAcrossReload() {
  auto firstConfig = std::make_shared<const ServiceInfoConfig>("first");
  auto first = servicelib::config::detail::MakeRuntimeConfigSnapshot(firstConfig);
  firstConfig.reset();
  servicelib::config::RuntimeConfigRegistry::Publish(first);

  auto second = servicelib::config::detail::MakeRuntimeConfigSnapshot(
      std::make_shared<const ServiceInfoConfig>("second"));
  servicelib::config::RuntimeConfigRegistry::Publish(second);

  Require(first->GetOnlyServiceConfig()->name == "first");
  Require(servicelib::config::RuntimeConfigRegistry::Snapshot()
             ->GetOnlyServiceConfig()
             ->name == "second");
  servicelib::config::RuntimeConfigRegistry::Publish({});
}

void stopWaitsForAcceptedInputInvocation() {
  PriorityPoolConfig config;
  servicelib::config::RuntimeConfigRegistry::Publish(
      std::make_shared<const servicelib::config::RuntimeConfig>(config));
  {
    Service service;
    service.start();
    auto invocation = std::make_unique<
        decltype(service.beginInputInvocation())>(
        service.beginInputInvocation());
    std::atomic<bool> stopReturned{false};
    std::thread stopThread([&] {
      service.stop();
      stopReturned.store(true, std::memory_order_release);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds{10});
    Require(!stopReturned.load(std::memory_order_acquire));
    invocation.reset();
    stopThread.join();
    Require(stopReturned.load(std::memory_order_acquire));
#ifdef NDEBUG
    bool rejected = false;
    try {
      static_cast<void>(service.beginInputInvocation());
    } catch (const servicelib::StreamException&) {
      rejected = true;
    }
    Require(rejected);
#endif
  }
  servicelib::config::RuntimeConfigRegistry::Publish({});
}

void stopDeadlineReturnsBeforeAcceptedInputCompletes() {
  PriorityPoolConfig config;
  servicelib::config::RuntimeConfigRegistry::Publish(
      std::make_shared<const servicelib::config::RuntimeConfig>(config));
  bool returnedByDeadline = false;
  {
    auto destroyed = std::make_shared<std::promise<void>>();
    auto destruction = destroyed->get_future();
    auto service = std::shared_ptr<Service>(new Service, [destroyed](Service* value) {
      delete value;
      destroyed->set_value();
    });
    std::weak_ptr<Service> retained = service;
    service->start();
    auto invocation = std::make_unique<
        decltype(service->beginInputInvocation())>(
        service->beginInputInvocation());
    auto stopped = std::async(std::launch::async, [&] {
      service->stop(servicelib::Context{}.bounded(std::chrono::milliseconds{20}));
    });
    // The generous watchdog is not the requested shutdown deadline. It lets
    // the test report an unbounded wait without leaving a blocked thread.
    returnedByDeadline =
        stopped.wait_for(std::chrono::seconds{2}) == std::future_status::ready;
    if (returnedByDeadline) {
      stopped.get();
      service.reset();
      Require(!retained.expired());
      invocation.reset();
    } else {
      invocation.reset();
      stopped.get();
      service.reset();
    }
    Require(destruction.wait_for(std::chrono::seconds{2}) == std::future_status::ready);
  }
  servicelib::config::RuntimeConfigRegistry::Publish({});
  Require(returnedByDeadline);
}

}  // namespace

int main() {
  {
    std::optional<RuntimeOwnerProbe> first;
    std::optional<RuntimeOwnerProbe> second;
    first.emplace();
    Require(first->runtimeBelongsToThisInstance());
    first.reset();
    second.emplace();
    Require(second->runtimeBelongsToThisInstance());
  }
  startsAndStopsInServiceOrder();
  rollsBackStartedComponents();
  stopFailureDoesNotSkipResources();
  deadlineDiagnosesButDoesNotReleaseLiveResource();
  connectorTimeoutMatchesTelemetry();
  preparesConfiguredPoolsBeforeGraphConstruction();
  exposesCanonicalServiceInfoMetric();
  runtimeConfigSnapshotOwnsConcreteConfigAcrossReload();
  stopWaitsForAcceptedInputInvocation();
  stopDeadlineReturnsBeforeAcceptedInputCompletes();
}

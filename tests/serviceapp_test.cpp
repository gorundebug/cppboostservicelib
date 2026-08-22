#include <servicelib/runtime/serviceapp.hpp>
#include <servicelib/runtime/testlog/testlog.hpp>
#include <servicelib/runtime/testmetrics/testmetrics.hpp>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <concepts>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "test_async.hpp"

namespace {

test_async::AsioRuntime asioRuntime;

struct ServiceDataTypes final {
  template <typename>
  struct DataType {};
};
class Service final : public servicelib::ServiceApp<Service, ServiceDataTypes> {};
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

  void start(servicelib::Context) {
    events->add(name + ":start");
    if (failStart) throw std::runtime_error("start failed");
  }
  void stop(servicelib::Context) {
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
  lifecycle.add(servicelib::ServiceComponentKind::kDataSink,
                std::make_shared<Component>("sink", &events));
  lifecycle.add(servicelib::ServiceComponentKind::kDelayPool,
                std::make_shared<Component>("delay", &events));
  lifecycle.add(servicelib::ServiceComponentKind::kDataSource,
                std::make_shared<Component>("source", &events));
  lifecycle.start({});
  lifecycle.stop({});
  const auto recorded = events.snapshot();
  assert(recorded.size() == 6);
  assert((std::vector(recorded.begin(), recorded.begin() + 3) ==
          std::vector<std::string>{"source:start", "sink:start",
                                   "delay:start"}));
  assert(recorded.back() == "sink:stop");
  assert(std::find(recorded.begin() + 3, recorded.end(), "source:stop") !=
         recorded.end());
  assert(std::find(recorded.begin() + 3, recorded.end(), "delay:stop") !=
         recorded.end());
}

void rollsBackStartedComponents() {
  EventLog events;
  servicelib::ServiceLifecycle lifecycle;
  lifecycle.add(servicelib::ServiceComponentKind::kDataSource,
                std::make_shared<Component>("source", &events));
  lifecycle.add(servicelib::ServiceComponentKind::kDataSink,
                std::make_shared<Component>("sink", &events, true));
  bool failed = false;
  try {
    lifecycle.start({});
  } catch (const std::runtime_error&) {
    failed = true;
  }
  assert(failed);
  assert((events.snapshot() ==
          std::vector<std::string>{"source:start", "sink:start",
                                   "source:stop"}));
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
  assert(std::find(recorded.begin(), recorded.end(), "healthy:stop") !=
         recorded.end());
  assert(std::find(recorded.begin(), recorded.end(), "failing:stop") !=
         recorded.end());
  assert(logger.records.size() == 1);
  assert(logger.records.front().message ==
         "service shutdown operation failed");
  assert(logger.records.front().resource == "component:1");
  assert(logger.records.front().error == "stop failed");
}

void deadlineDiagnosesButJoinsResource() {
  EventLog events;
  RecordingLogger logger;
  std::atomic<bool> stopped{false};
  servicelib::ServiceLifecycle lifecycle;
  lifecycle.add(servicelib::ServiceComponentKind::kComponent,
                std::make_shared<Component>(
                    "slow", &events, false, false,
                    std::chrono::milliseconds{30}, &stopped));
  lifecycle.start({});
  lifecycle.stop(servicelib::Context{}.bounded(std::chrono::milliseconds{1}),
                 logger);
  assert(stopped.load());
  assert(logger.records.size() == 1);
  assert(logger.records.front().message ==
         "service shutdown operation timed out");
  assert(logger.records.front().resource == "component:0");
}

void connectorTimeoutMatchesTelemetry() {
  EventLog events;
  std::atomic<bool> stopped{false};
  servicelib::testmetrics::TestMetrics metrics;
  servicelib::testlog::TestLog logger;
  servicelib::ServiceLifecycle lifecycle;
  lifecycle.add(servicelib::ServiceComponentKind::kDataSource,
                std::make_shared<Component>(
                    "source", &events, false, false,
                    std::chrono::milliseconds{30}, &stopped),
                &metrics, &logger);
  lifecycle.start({});
  lifecycle.stop(servicelib::Context{}.bounded(std::chrono::milliseconds{1}),
                 logger);
  assert(stopped.load());
  assert(metrics
             .counter("datasource_connector.events_total",
                      {{"connector", "0"}, {"event", "stop_timeout"}})
             .count() == 1);
  const auto entries = logger.entries();
  assert(entries.size() == 1);
  assert(entries.front().message == "data source stopped by timeout");
}

void preparesConfiguredPoolsBeforeGraphConstruction() {
  PriorityPoolConfig config;
  servicelib::config::RuntimeConfigRegistry::Publish(
      std::make_shared<const servicelib::config::RuntimeConfig>(config));
  {
    Service service;
    assert(service.getPriorityTaskPool("Default Pool") != nullptr);
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
    assert(metrics
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

  assert(first->GetOnlyServiceConfig()->name == "first");
  assert(servicelib::config::RuntimeConfigRegistry::Snapshot()
             ->GetOnlyServiceConfig()
             ->name == "second");
  servicelib::config::RuntimeConfigRegistry::Publish({});
}

}  // namespace

int main() {
  startsAndStopsInServiceOrder();
  rollsBackStartedComponents();
  stopFailureDoesNotSkipResources();
  deadlineDiagnosesButJoinsResource();
  connectorTimeoutMatchesTelemetry();
  preparesConfiguredPoolsBeforeGraphConstruction();
  exposesCanonicalServiceInfoMetric();
  runtimeConfigSnapshotOwnsConcreteConfigAcrossReload();
}

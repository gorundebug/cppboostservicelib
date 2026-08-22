#include <servicelib/api/serviceapi_parse.hpp>
#include <servicelib/runtime/config/loader.hpp>
#include <servicelib/runtime/testmetrics/testmetrics.hpp>

#include <cassert>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>
#include <thread>

namespace {

struct TestConfig final : servicelib::config::IConfig {
  std::string serviceName{"default"};
  int port{8080};
  int executors{2};
  int queueCapacity{64};
  std::string environment;
  YAML::Node additional;

  std::vector<const servicelib::config::ServiceConfig*> GetServices()
      const override { return {}; }
  std::vector<servicelib::config::StreamConfigRef> GetStreams()
      const override { return {}; }
  std::vector<servicelib::config::DataConnectorConfigRef> GetDataConnectors()
      const override { return {}; }
  std::vector<servicelib::config::EndpointConfigRef> GetEndpoints()
      const override { return {}; }
  std::vector<const servicelib::config::PoolConfig*> GetPools()
      const override { return {}; }
  std::vector<const servicelib::config::LinkConfig*> GetLinks()
      const override { return {}; }
  std::vector<const servicelib::config::ModuleConfig*> GetModules()
      const override { return {}; }
  std::vector<const servicelib::config::TypeConfig*> GetTypes()
      const override { return {}; }
};

TestConfig MakeConfig(servicelib::config::TypeTag<TestConfig>) {
  return {};
}

void ApplyConfig(const YAML::Node& value, TestConfig& config) {
  if (const auto service = value["service"]) {
    if (const auto field = service["name"]) {
      config.serviceName = field.as<std::string>();
    }
    if (const auto field = service["port"]) {
      config.port = field.as<int>();
    }
  }
  if (const auto pool = value["pool"]) {
    if (const auto field = pool["executorsCount"]) {
      config.executors = field.as<int>();
    }
    if (const auto field = pool["queueCapacity"]) {
      config.queueCapacity = field.as<int>();
    }
  }
  config.additional = YAML::Clone(value["additional"]);
}

void ApplyEnvironment(TestConfig& config) {
  config.environment = "production";
  if (config.executors <= 0) {
    throw std::invalid_argument("executorsCount must be positive");
  }
}

class TemporaryDirectory final {
 public:
  TemporaryDirectory() {
    const auto suffix = std::chrono::steady_clock::now().time_since_epoch().count();
    path_ = std::filesystem::temp_directory_path() /
            ("cppboostservicelib-config-" + std::to_string(suffix));
    std::filesystem::create_directories(path_);
  }

  ~TemporaryDirectory() { std::filesystem::remove_all(path_); }

  std::filesystem::path Write(std::string_view name, std::string_view value) {
    const auto path = path_ / name;
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    output << value;
    if (!output) {
      throw std::runtime_error("cannot write test configuration");
    }
    return path;
  }

 private:
  std::filesystem::path path_;
};

using Loader = servicelib::config::ConfigLoader<TestConfig>;

Loader MakeLoader(const std::filesystem::path& base,
                  const std::filesystem::path& values,
                  servicelib::metrics::Metrics& metrics) {
  return Loader({.configPath = base.string(),
                 .overridePath = values.string()},
                {}, servicelib::log::NoopLogger::instance(), metrics,
                "orders");
}

}  // namespace

int main() {
  using servicelib::api::DataConnectorImplementation;
  const auto ParseImplementation = [](std::string_view value) {
    return servicelib::config::YamlValue(YAML::Load(std::string(value)))
        .As<DataConnectorImplementation>();
  };
  assert(ParseImplementation("boost/beast-http") ==
         DataConnectorImplementation::kBoostBeastHTTP);
  assert(ParseImplementation("asio/grpc") ==
         DataConnectorImplementation::kAsioGRPC);
  assert(ParseImplementation("librdkafka") ==
         DataConnectorImplementation::kLibrdkafka);

  TemporaryDirectory directory;
  const auto base = directory.Write("config.yaml", R"(
service:
  name: orders
  port: 8081
pool:
  executorsCount: 4
  queueCapacity: 100
additional:
  nested:
    kept: true
    replaced: base
  list: [one, two]
)");
  const auto values = directory.Write("overrides.yaml", R"(
service:
  port: 9091
pool:
  executorsCount: 8
additional:
  nested:
    replaced: override
  list: [three]
)");

  servicelib::testmetrics::TestMetrics metrics;
  auto loader = MakeLoader(base, values, metrics);
  const auto WaitFor = [](const auto& predicate) {
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (!predicate() && std::chrono::steady_clock::now() < deadline) {
      std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    assert(predicate());
  };
  bool rejectedBeforeLoad = false;
  try {
    (void)loader.GetConfig();
  } catch (const std::logic_error&) {
    rejectedBeforeLoad = true;
  }
  assert(rejectedBeforeLoad);

  const auto initialRuntime = loader.Load();
  assert(initialRuntime == loader.GetRuntimeConfig());
  const auto original = loader.GetConfig();
  assert(original->serviceName == "orders");
  assert(original->port == 9091);
  assert(original->executors == 8);
  assert(original->queueCapacity == 100);
  assert(original->environment == "production");
  assert(original->additional["nested"]["kept"].as<bool>());
  assert(original->additional["nested"]["replaced"].as<std::string>() ==
         "override");
  assert(original->additional["list"].size() == 1);
  assert(original->additional["list"][0].as<std::string>() == "three");

  std::atomic<int> callbackCount{};
  loader.Start(std::chrono::milliseconds{5}, [&](const auto& runtimeConfig) {
    assert(runtimeConfig == loader.GetRuntimeConfig());
    callbackCount.fetch_add(1, std::memory_order_relaxed);
  });
  directory.Write("config.yaml", R"(
service:
  name: orders-reloaded
pool:
  queueCapacity: 200
)");
  directory.Write("overrides.yaml", R"(
service:
  port: 9191
pool:
  executorsCount: 6
)");
  WaitFor([&] { return callbackCount.load(std::memory_order_relaxed) == 1; });
  loader.Stop();
  const auto reloaded = loader.GetConfig();
  assert(reloaded->serviceName == "orders-reloaded");
  assert(reloaded->port == 9191);
  assert(reloaded->executors == 6);
  assert(reloaded->queueCapacity == 200);
  assert(callbackCount.load(std::memory_order_relaxed) == 1);
  assert(original->serviceName == "orders");
  assert(original->port == 9091);

  directory.Write("overrides.yaml", "pool: [malformed\n");
  loader.Start(std::chrono::milliseconds{5}, [&](const auto&) {
    callbackCount.fetch_add(1, std::memory_order_relaxed);
  });
  WaitFor([&] {
    return metrics
               .counter("service.config_reloads_total",
                        {{"service", "orders"}, {"event", "error"}})
               .count() >= 1;
  });
  loader.Stop();
  assert(callbackCount.load(std::memory_order_relaxed) == 1);
  assert(loader.GetConfig()->port == 9191);

  directory.Write("overrides.yaml", "pool:\n  executorsCount: 0\n");
  loader.Start(std::chrono::milliseconds{5}, [&](const auto&) {
    callbackCount.fetch_add(1, std::memory_order_relaxed);
  });
  WaitFor([&] {
    return metrics
               .counter("service.config_reloads_total",
                        {{"service", "orders"}, {"event", "error"}})
               .count() >= 2;
  });
  loader.Stop();
  assert(callbackCount.load(std::memory_order_relaxed) == 1);
  assert(loader.GetConfig()->executors == 6);

  directory.Write("overrides.yaml", R"(
service:
  port: 9292
pool:
  executorsCount: 7
)");
  loader.Start(std::chrono::milliseconds{5}, [&](const auto&) {
    callbackCount.fetch_add(1, std::memory_order_relaxed);
    throw std::runtime_error("callback failure");
  });
  WaitFor([&] { return callbackCount.load(std::memory_order_relaxed) == 2; });
  loader.Stop();
  assert(loader.GetConfig()->port == 9292);
  assert(metrics
             .counter("service.config_reloads_total",
                      {{"service", "orders"}, {"event", "success"}})
             .count() == 2);
  const auto errorsAfterRejectedFiles =
      metrics
          .counter("service.config_reloads_total",
                   {{"service", "orders"}, {"event", "error"}})
          .count();
  // A malformed fingerprint remains uncommitted and is intentionally retried
  // on every poll until a valid file replaces it.
  assert(errorsAfterRejectedFiles >= 2);

  std::atomic<int> asynchronousCallbacks{};
  loader.Start(std::chrono::milliseconds{5}, [&](const auto& next) {
    assert(next == loader.GetRuntimeConfig());
    assert(loader.GetConfig()->port == 9393);
    asynchronousCallbacks.fetch_add(1, std::memory_order_relaxed);
  });
  directory.Write("overrides.yaml", R"(
service:
  port: 9393
pool:
  executorsCount: 9
)");
  WaitFor([&] {
    return asynchronousCallbacks.load(std::memory_order_relaxed) == 1;
  });
  loader.Stop();
  assert(asynchronousCallbacks.load(std::memory_order_relaxed) == 1);
  assert(loader.GetConfig()->port == 9393);
  assert(metrics
             .counter("service.config_reloads_total",
                      {{"service", "orders"}, {"event", "error"}})
             .count() == errorsAfterRejectedFiles);
  assert(metrics
             .counter("service.config_reloads_total",
                      {{"service", "orders"}, {"event", "success"}})
             .count() == 3);
}

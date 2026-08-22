#pragma once

#include <servicelib/runtime/config/config.hpp>
#include <servicelib/runtime/config/substitution.hpp>
#include <servicelib/runtime/config/yaml.hpp>
#include <servicelib/runtime/config/yaml_value.hpp>
#include <servicelib/runtime/environment/log/log.hpp>
#include <servicelib/runtime/environment/metrics/metrics.hpp>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>

namespace servicelib::config {

// Boost/yaml-cpp implementation of the same generated customization contract
// as cppservicelib. The associated ConcreteConfig namespace supplies these
// three functions through ADL.
template <typename ConcreteConfig>
struct GeneratedConfigAdapter {
  static ConcreteConfig Make() {
    return MakeConfig(TypeTag<ConcreteConfig>{});
  }

  static void Apply(const YamlValue& value, ConcreteConfig& config) {
    ApplyConfig(value.node(), config);
  }

  static void Finalize(ConcreteConfig& config) { ApplyEnvironment(config); }
};

template <typename ConcreteConfig,
          typename ConfigAdapter = GeneratedConfigAdapter<ConcreteConfig>>
class ConfigLoader final {
 public:
  struct Paths {
    std::string configPath;
    std::optional<std::string> overridePath;
  };

  using ReloadCallback =
      std::function<void(std::shared_ptr<const RuntimeConfig>)>;

  // `configVars` occupies the canonical substitution boundary. Boost uses the
  // Go/Rust-style environment/file substitution implemented by
  // SubstituteConfiguration, so there is no userver config-vars document to
  // retain or interpret.
  ConfigLoader(Paths paths, YamlValue configVars,
               log::Logger& logger, metrics::Metrics& metrics,
               std::string serviceName)
      : paths_(std::move(paths)),
        logger_(logger),
        metrics_(metrics),
        serviceName_(std::move(serviceName)) {
    static_cast<void>(configVars);
  }

  ~ConfigLoader() { Stop(); }

  ConfigLoader(const ConfigLoader&) = delete;
  ConfigLoader& operator=(const ConfigLoader&) = delete;

  std::shared_ptr<const RuntimeConfig> Load() {
    std::lock_guard lock(reloadMutex_);
    auto loaded = BuildCandidate();
    state_.store(loaded.config, std::memory_order_release);
    fingerprint_ = std::move(loaded.fingerprint);
    InitializeServiceMetrics(loaded.config->runtimeConfig);
    return RuntimeConfigView(loaded.config);
  }

  void Start(std::chrono::milliseconds pollInterval,
             ReloadCallback onReload) {
    if (pollInterval <= std::chrono::milliseconds::zero()) {
      throw std::invalid_argument("config reload interval must be positive");
    }
    Stop();
    {
      std::lock_guard lock(reloadMutex_);
      onReload_ = std::move(onReload);
    }
    if (!paths_.overridePath || paths_.overridePath->empty()) return;
    poller_ = std::jthread([this, pollInterval](std::stop_token stopToken) {
      std::unique_lock lock(pollerMutex_);
      while (!stopToken.stop_requested()) {
        pollerWakeup_.wait_for(lock, stopToken, pollInterval,
                               [] { return false; });
        if (stopToken.stop_requested()) break;
        lock.unlock();
        PollOnce();
        lock.lock();
      }
    });
  }

  void Stop() {
    if (!poller_.joinable()) return;
    poller_.request_stop();
    pollerWakeup_.notify_all();
    poller_.join();
  }

  std::shared_ptr<const RuntimeConfig> GetRuntimeConfig() const {
    const auto loaded = CurrentLoaded();
    return RuntimeConfigView(loaded);
  }

  std::shared_ptr<const ConcreteConfig> GetConfig() const {
    const auto loaded = CurrentLoaded();
    return std::shared_ptr<const ConcreteConfig>(loaded, &loaded->config);
  }

 private:
  struct LoadedConfig final {
    explicit LoadedConfig(ConcreteConfig value)
        : config(std::move(value)), runtimeConfig(config) {}

    ConcreteConfig config;
    RuntimeConfig runtimeConfig;
  };

  struct Fingerprint final {
    std::string config;
    std::string values;
    bool operator==(const Fingerprint&) const = default;
  };

  struct Candidate final {
    std::shared_ptr<const LoadedConfig> config;
    Fingerprint fingerprint;
  };

  static std::shared_ptr<const RuntimeConfig> RuntimeConfigView(
      const std::shared_ptr<const LoadedConfig>& loaded) {
    return std::shared_ptr<const RuntimeConfig>(loaded, &loaded->runtimeConfig);
  }

  std::shared_ptr<const LoadedConfig> CurrentLoaded() const {
    auto loaded = state_.load(std::memory_order_acquire);
    if (!loaded) {
      throw std::logic_error("configuration has not been loaded");
    }
    return loaded;
  }

  Fingerprint ReadFingerprint() const {
    Fingerprint result{ReadConfigFile(paths_.configPath), {}};
    if (paths_.overridePath && !paths_.overridePath->empty()) {
      result.values = ReadConfigFile(*paths_.overridePath);
    }
    return result;
  }

  Candidate BuildCandidate() const {
    auto fingerprint = ReadFingerprint();
    auto config = ConfigAdapter::Make();
    auto merged = ParseYaml(fingerprint.config, paths_.configPath);
    if (paths_.overridePath && !paths_.overridePath->empty()) {
      merged = DeepMerge(
          merged, ParseYaml(fingerprint.values, *paths_.overridePath));
    }
    merged = SubstituteConfiguration(
        merged,
        {.variables = {},
         .baseDirectory = std::filesystem::path(paths_.configPath).parent_path(),
         .environment = {}});
    ConfigAdapter::Apply(YamlValue(std::move(merged)), config);
    ConfigAdapter::Finalize(config);
    return {std::make_shared<const LoadedConfig>(std::move(config)),
            std::move(fingerprint)};
  }

  void PollOnce() noexcept {
    ReloadCallback callback;
    std::shared_ptr<const LoadedConfig> loaded;
    try {
      {
        std::lock_guard lock(reloadMutex_);
        const auto observed = ReadFingerprint();
        if (fingerprint_ && *fingerprint_ == observed) return;
        auto candidate = BuildCandidate();
        loaded = std::move(candidate.config);
        state_.store(loaded, std::memory_order_release);
        fingerprint_ = std::move(candidate.fingerprint);
        callback = onReload_;
      }
      IncBestEffort(reloadSuccessCounter_);
    } catch (const std::exception& error) {
      LogReloadFailure(error.what());
      IncBestEffort(reloadErrorCounter_);
      return;
    } catch (...) {
      LogReloadFailure("unknown reload error");
      IncBestEffort(reloadErrorCounter_);
      return;
    }

    if (!callback) return;
    try {
      callback(RuntimeConfigView(loaded));
    } catch (const std::exception& error) {
      LogCallbackFailure(error.what());
    } catch (...) {
      LogCallbackFailure("unknown callback error");
    }
  }

  static void IncBestEffort(
      const std::unique_ptr<metrics::Int64Counter>& counter) noexcept {
    try {
      if (counter) counter->inc();
    } catch (...) {
    }
  }

  void LogReloadFailure(std::string_view error) noexcept {
    try {
      logger_.warn("config reload failed",
                   {log::Field::Str("service", serviceName_),
                    log::Field::Err(error)});
    } catch (...) {
    }
  }

  void LogCallbackFailure(std::string_view error) noexcept {
    try {
      logger_.warn("config reload callback failed",
                   {log::Field::Str("service", serviceName_),
                    log::Field::Err(error)});
    } catch (...) {
    }
  }

  static std::string EnvironmentName(api::Environment environment) {
    switch (environment) {
      case api::Environment::kLocal: return "local";
      case api::Environment::kDebug: return "debug";
      case api::Environment::kStaging: return "staging";
      case api::Environment::kProduction: return "production";
      case api::Environment::kUndefined: return {};
    }
    return {};
  }

  void InitializeServiceMetrics(const RuntimeConfig& runtimeConfig) {
    const auto* service = runtimeConfig.GetOnlyServiceConfig();
    if (service && !service->name.empty()) serviceName_ = service->name;
    auto scope =
        metrics_.scope("service", metrics::Labels{{"service", serviceName_}});
    reloadSuccessCounter_ = scope->counter(
        "config_reloads_total", "Total number of config reload attempts",
        {{"event", "success"}});
    reloadErrorCounter_ = scope->counter(
        "config_reloads_total", "Total number of config reload attempts",
        {{"event", "error"}});
    if (!service) return;
    auto infoScope = metrics_.scope(
        "service", metrics::Labels{{"service", serviceName_},
                                    {"environment", EnvironmentName(service->environment)}});
    serviceInfoGauge_ =
        infoScope->gauge("info", "Service information (value is always 1)");
    try {
      serviceInfoGauge_->set(1);
    } catch (...) {
    }
  }

  Paths paths_;
  log::Logger& logger_;
  metrics::Metrics& metrics_;
  std::string serviceName_;
  mutable std::mutex reloadMutex_;
  std::atomic<std::shared_ptr<const LoadedConfig>> state_;
  std::optional<Fingerprint> fingerprint_;
  ReloadCallback onReload_;
  std::unique_ptr<metrics::Int64Counter> reloadSuccessCounter_;
  std::unique_ptr<metrics::Int64Counter> reloadErrorCounter_;
  std::unique_ptr<metrics::Int64Gauge> serviceInfoGauge_;
  std::mutex pollerMutex_;
  std::condition_variable_any pollerWakeup_;
  std::jthread poller_;
};

}  // namespace servicelib::config

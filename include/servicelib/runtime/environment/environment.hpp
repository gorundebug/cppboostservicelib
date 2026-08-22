/*
 * environment.hpp
 * C++ streams API — service-wide runtime environment accessor
 *
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the
 * [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */
#pragma once

#include <functional>
#include <memory>
#include <string>

#include <servicelib/runtime/config/config.hpp>
#include <servicelib/runtime/pool/pool.hpp>
#include <servicelib/runtime/environment/log/log.hpp>
#include <servicelib/runtime/environment/metrics/metrics.hpp>
#include <servicelib/runtime/environment/tracing/tracing.hpp>

namespace servicelib {

class StreamBase;

// Service-scoped dependencies shared by runtime components. Go analog:
// environment.ServiceEnvironment. Configs are exposed only as owning
// snapshots so a concurrent reload cannot invalidate a borrowed pointer.
class IServiceEnvironment {
 public:
  virtual ~IServiceEnvironment() = default;

  virtual std::shared_ptr<const config::RuntimeConfig>
  getRuntimeConfigSnapshot() const = 0;
  virtual std::shared_ptr<const config::ServiceConfig>
  getServiceConfigSnapshot() const = 0;

  // Stable graph identity. Concrete service environments cache this value
  // while the graph is initialized so telemetry never reads configuration on
  // the message hot path.
  virtual const std::string& getServiceName() const noexcept {
    static const std::string empty;
    return empty;
  }

  virtual log::Logger& getLogger() = 0;
  virtual metrics::Metrics& getMetrics() = 0;
  // Nullable: no tracing configured for this service.
  virtual tracing::Tracing* getTracing() = 0;
};

// Graph execution facilities layered on top of IServiceEnvironment. Go
// analog: the execution-related subset of runtime.RuntimeEnvironment.
class IRuntimeEnvironment : public IServiceEnvironment {
 public:
  ~IRuntimeEnvironment() override = default;

  virtual pool::ITaskPool* getTaskPool(const std::string& name) = 0;
  virtual pool::IPriorityTaskPool* getPriorityTaskPool(
      const std::string& name) = 0;
  // Go: RuntimeEnvironment.Delay. Delay is service-wide and deliberately
  // exposed as an operation instead of leaking the concrete scheduler into
  // operators.
  virtual void delay(Context context, pool::IDelayPool::Duration duration,
                     std::function<void()> task) {
    static_cast<void>(context);
    static_cast<void>(duration);
    static_cast<void>(task);
    throw std::logic_error("runtime environment has no delay scheduler");
  }
  // Schedule one ParallelCall operation under the service graph lifetime.
  // The concrete execution environment must drain these operations before it
  // releases streams and callers.
  virtual void parallel(std::function<void()> task) {
    static_cast<void>(task);
    throw std::logic_error("runtime environment has no parallel scheduler");
  }
  // Go: RuntimeEnvironment.RegisterStream. The environment owns registered
  // root streams for the complete service lifetime.
  virtual void registerStream(std::shared_ptr<StreamBase> stream) {
    static_cast<void>(stream);
  }
};

}  // namespace servicelib

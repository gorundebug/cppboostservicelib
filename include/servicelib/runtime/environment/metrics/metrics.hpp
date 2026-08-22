/*
 * metrics.hpp
 * C++ streams API — engine-agnostic metrics interface
 *
 * Trimmed mirror of servicelib's Go implementation
 * (runtime/environment/metrics/metrics.go): Metrics/MetricsScope factory
 * interfaces + Int64Counter/Int64Gauge/Float64Histogram instrument
 * interfaces. Vec variants and Float64Counter are intentionally omitted —
 * add them if/when a caller needs them. No concrete backend dependency here —
 * see
 * servicelib/telemetry/userver/metrics.hpp for the userver-backed
 * implementation (pull-based: instruments wrap
 * utils::statistics::RateCounter/Histogram and self-register with
 * utils::statistics::Storage for scraping via server::handlers::ServerMonitor).
 *
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the
 * [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */
#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace servicelib::metrics {

using Labels = std::unordered_map<std::string, std::string>;

// Go analog: metrics.Int64Counter. Monotonically increasing.
class Int64Counter {
 public:
  virtual ~Int64Counter() = default;
  virtual void inc() = 0;
  virtual void add(int64_t v) = 0;
};

// Go analog: metrics.Int64Gauge. Current value of a quantity.
class Int64Gauge {
 public:
  virtual ~Int64Gauge() = default;
  virtual void set(int64_t v) = 0;
  virtual void inc() = 0;
  virtual void dec() = 0;
  virtual void add(int64_t delta) = 0;
  virtual void sub(int64_t delta) = 0;
};

// Go analog: metrics.Float64Histogram.
class Float64Histogram {
 public:
  virtual ~Float64Histogram() = default;
  virtual void observe(double v) = 0;
};

class ObservableFloat64Gauge {
 public:
  virtual ~ObservableFloat64Gauge() = default;
  [[nodiscard]] virtual double value() const = 0;
};

// Go analog: metrics.MetricsScope — factory bound to a fixed name prefix
// and a set of base labels; full metric name is "<prefix>_<name>".
class MetricsScope {
 public:
  virtual ~MetricsScope() = default;

  virtual std::unique_ptr<Int64Counter> counter(std::string_view name,
                                                std::string_view help,
                                                const Labels& labels = {}) = 0;

  virtual std::unique_ptr<Int64Gauge> gauge(std::string_view name,
                                            std::string_view help,
                                            const Labels& labels = {}) = 0;

  virtual std::unique_ptr<Float64Histogram> histogram(
      std::string_view name, std::string_view help, const Labels& labels = {},
      std::vector<double> buckets = {}) = 0;

  virtual std::unique_ptr<ObservableFloat64Gauge> observableFloat64Gauge(
      std::string_view name, std::string_view help,
      std::function<double()> value, const Labels& labels = {}) = 0;
};

// Go analog: metrics.Metrics.
class Metrics {
 public:
  virtual ~Metrics() = default;

  // Hot paths use this capability check to bypass clocks, locks, hashes and
  // virtual no-op instruments when metrics are disabled.
  [[nodiscard]] virtual bool enabled() const noexcept { return true; }

  virtual std::unique_ptr<MetricsScope> scope(std::string_view prefix,
                                              const Labels& labels) = 0;
};

// Discards everything. Returned by IServiceEnvironment implementations that
// have no metrics backend wired up.
class NoopMetrics final : public Metrics {
  class NoopCounter final : public Int64Counter {
   public:
    void inc() override {}
    void add(int64_t) override {}
  };

  class NoopGauge final : public Int64Gauge {
   public:
    void set(int64_t) override {}
    void inc() override {}
    void dec() override {}
    void add(int64_t) override {}
    void sub(int64_t) override {}
  };

  class NoopHistogram final : public Float64Histogram {
   public:
    void observe(double) override {}
  };

  class NoopObservableGauge final : public ObservableFloat64Gauge {
   public:
    [[nodiscard]] double value() const override { return 0.0; }
  };

  class NoopScope final : public MetricsScope {
   public:
    std::unique_ptr<Int64Counter> counter(std::string_view, std::string_view,
                                          const Labels&) override {
      return std::make_unique<NoopCounter>();
    }
    std::unique_ptr<Int64Gauge> gauge(std::string_view, std::string_view,
                                      const Labels&) override {
      return std::make_unique<NoopGauge>();
    }
    std::unique_ptr<Float64Histogram> histogram(std::string_view,
                                                std::string_view, const Labels&,
                                                std::vector<double>) override {
      return std::make_unique<NoopHistogram>();
    }
    std::unique_ptr<ObservableFloat64Gauge> observableFloat64Gauge(
        std::string_view, std::string_view, std::function<double()>,
        const Labels&) override {
      return std::make_unique<NoopObservableGauge>();
    }
  };

 public:
  [[nodiscard]] bool enabled() const noexcept override { return false; }

  std::unique_ptr<MetricsScope> scope(std::string_view,
                                      const Labels&) override {
    return std::make_unique<NoopScope>();
  }

  static Metrics& instance() {
    static NoopMetrics metrics;
    return metrics;
  }
};

}  // namespace servicelib::metrics

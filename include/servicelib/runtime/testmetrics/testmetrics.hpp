/*
 * testmetrics.hpp
 * In-memory Metrics backend for use in automated tests. Every instrument is
 * keyed by name+labels and stays readable afterwards via
 * TestMetrics::counter/gauge/histogram — the same object the code under
 * test wrote to.
 *
 * Go analog: runtime/testmetrics/testmetrics.go (TestMetrics).
 *
 * Usage:
 *   servicelib::testmetrics::TestMetrics metrics;
 *   // wire metrics into IServiceEnvironment::getMetrics()
 *   doWork();
 *   ASSERT_EQ(metrics.counter("task_pool.tasks_total", {{"name",
 * "p"}}).count(), 1);
 *
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the
 * [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */
#pragma once

#include <algorithm>
#include <atomic>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include <mutex>

#include <servicelib/runtime/environment/metrics/metrics.hpp>

namespace servicelib::testmetrics {

class TestInt64Counter final : public metrics::Int64Counter {
 public:
  void inc() override { count_.fetch_add(1, std::memory_order_seq_cst); }
  void add(int64_t v) override {
    count_.fetch_add(v, std::memory_order_seq_cst);
  }
  [[nodiscard]] int64_t count() const noexcept {
    return count_.load(std::memory_order_seq_cst);
  }

 private:
  std::atomic<int64_t> count_{0};
};

class TestInt64Gauge final : public metrics::Int64Gauge {
 public:
  // Test code uses terminal gauge values as lifecycle completion barriers.
  // Sequential consistency makes all preceding instrument writes visible
  // before a reader destroys the in-memory backend after observing that
  // terminal value. Production metric backends provide their own ownership
  // and synchronization; this is the std-thread replacement for userver's
  // coroutine scheduling boundary in the canonical test backend.
  void set(int64_t v) override { val_.store(v, std::memory_order_seq_cst); }
  void inc() override { val_.fetch_add(1, std::memory_order_seq_cst); }
  void dec() override { val_.fetch_sub(1, std::memory_order_seq_cst); }
  void add(int64_t delta) override {
    val_.fetch_add(delta, std::memory_order_seq_cst);
  }
  void sub(int64_t delta) override {
    val_.fetch_sub(delta, std::memory_order_seq_cst);
  }
  [[nodiscard]] int64_t value() const noexcept {
    return val_.load(std::memory_order_seq_cst);
  }

 private:
  std::atomic<int64_t> val_{0};
};

// engine::Mutex (not std::mutex/std::atomic-only): observe() needs to
// append to `values_` atomically-as-a-whole, and everything that would
// exercise this double only runs inside a userver coroutine anyway.
class TestFloat64Histogram final : public metrics::Float64Histogram {
 public:
  void observe(double v) override {
    std::lock_guard<std::mutex> lock(mu_);
    ++count_;
    sum_ += v;
    values_.push_back(v);
  }

  [[nodiscard]] int64_t count() const {
    std::lock_guard<std::mutex> lock(mu_);
    return count_;
  }
  [[nodiscard]] double sum() const {
    std::lock_guard<std::mutex> lock(mu_);
    return sum_;
  }
  [[nodiscard]] std::vector<double> values() const {
    std::lock_guard<std::mutex> lock(mu_);
    return values_;
  }

 private:
  mutable std::mutex mu_;
  int64_t count_{0};
  double sum_{0};
  std::vector<double> values_;
};

class TestObservableFloat64Gauge final
    : public metrics::ObservableFloat64Gauge {
 public:
  explicit TestObservableFloat64Gauge(std::function<double()> value)
      : value_(std::move(value)) {}
  [[nodiscard]] double value() const override { return value_(); }

 private:
  std::function<double()> value_;
};

namespace detail {

// Thin proxies returned by TestMetricsScope::counter/gauge/histogram: they
// forward to the shared Test* instance stored in TestMetrics so that both
// the code under test and the test's own assertions observe the same state.
class CounterRef final : public metrics::Int64Counter {
 public:
  explicit CounterRef(TestInt64Counter& target) : target_(target) {}
  void inc() override { target_.inc(); }
  void add(int64_t v) override { target_.add(v); }

 private:
  TestInt64Counter& target_;
};

class GaugeRef final : public metrics::Int64Gauge {
 public:
  explicit GaugeRef(TestInt64Gauge& target) : target_(target) {}
  void set(int64_t v) override { target_.set(v); }
  void inc() override { target_.inc(); }
  void dec() override { target_.dec(); }
  void add(int64_t delta) override { target_.add(delta); }
  void sub(int64_t delta) override { target_.sub(delta); }

 private:
  TestInt64Gauge& target_;
};

class HistogramRef final : public metrics::Float64Histogram {
 public:
  explicit HistogramRef(TestFloat64Histogram& target) : target_(target) {}
  void observe(double v) override { target_.observe(v); }

 private:
  TestFloat64Histogram& target_;
};

class ObservableGaugeRef final : public metrics::ObservableFloat64Gauge {
 public:
  explicit ObservableGaugeRef(TestObservableFloat64Gauge& target)
      : target_(target) {}
  [[nodiscard]] double value() const override { return target_.value(); }

 private:
  TestObservableFloat64Gauge& target_;
};

}  // namespace detail

class TestMetrics;

class TestMetricsScope final : public metrics::MetricsScope {
 public:
  TestMetricsScope(TestMetrics& m, std::string prefix, metrics::Labels base)
      : m_(m), prefix_(std::move(prefix)), base_(std::move(base)) {}

  // `help` is not recorded — nothing in these tests reads it back.
  std::unique_ptr<metrics::Int64Counter> counter(
      std::string_view name, std::string_view /*help*/,
      const metrics::Labels& labels) override;
  std::unique_ptr<metrics::Int64Gauge> gauge(
      std::string_view name, std::string_view /*help*/,
      const metrics::Labels& labels) override;
  std::unique_ptr<metrics::Float64Histogram> histogram(
      std::string_view name, std::string_view /*help*/,
      const metrics::Labels& labels, std::vector<double> /*buckets*/) override;
  std::unique_ptr<metrics::ObservableFloat64Gauge> observableFloat64Gauge(
      std::string_view name, std::string_view /*help*/,
      std::function<double()> value, const metrics::Labels& labels) override;

 private:
  [[nodiscard]] std::string fullName(std::string_view name) const {
    if (name.empty()) {
      return prefix_;
    }
    if (prefix_.empty()) {
      return std::string(name);
    }
    return prefix_ + "." + std::string(name);
  }

  [[nodiscard]] metrics::Labels mergeLabels(
      const metrics::Labels& extra) const {
    if (extra.empty()) {
      return base_;
    }
    metrics::Labels merged = base_;
    for (const auto& [k, v] : extra) {
      merged[k] = v;
    }
    return merged;
  }

  TestMetrics& m_;
  std::string prefix_;
  metrics::Labels base_;
};

class TestMetrics final : public metrics::Metrics {
 public:
  std::unique_ptr<metrics::MetricsScope> scope(
      std::string_view prefix, const metrics::Labels& labels) override {
    return std::make_unique<TestMetricsScope>(*this, std::string(prefix),
                                              labels);
  }

  TestInt64Counter& counter(const std::string& name,
                            const metrics::Labels& labels) {
    return getOrCreate(counters_, name, labels);
  }
  TestInt64Gauge& gauge(const std::string& name,
                        const metrics::Labels& labels) {
    return getOrCreate(gauges_, name, labels);
  }
  TestFloat64Histogram& histogram(const std::string& name,
                                  const metrics::Labels& labels) {
    return getOrCreate(histograms_, name, labels);
  }
  TestObservableFloat64Gauge& observableGauge(const std::string& name,
                                              const metrics::Labels& labels) {
    std::lock_guard<std::mutex> lock(mu_);
    return *observableGauges_.at(metricKey(name, labels));
  }

  void reset() {
    std::lock_guard<std::mutex> lock(mu_);
    counters_.clear();
    gauges_.clear();
    histograms_.clear();
    observableGauges_.clear();
  }

  // Sorted, deduplicated names of every metric registered (via
  // scope->counter/gauge/histogram) since the last reset(). Use this to
  // verify a component registers all the metrics it's supposed to.
  [[nodiscard]] std::vector<std::string> registeredNames() const {
    std::lock_guard<std::mutex> lock(mu_);
    std::vector<std::string> names;
    for (const auto& [key, unused] : counters_) {
      names.push_back(nameFromKey(key));
    }
    for (const auto& [key, unused] : gauges_) {
      names.push_back(nameFromKey(key));
    }
    for (const auto& [key, unused] : histograms_) {
      names.push_back(nameFromKey(key));
    }
    for (const auto& [key, unused] : observableGauges_) {
      names.push_back(nameFromKey(key));
    }
    std::sort(names.begin(), names.end());
    names.erase(std::unique(names.begin(), names.end()), names.end());
    return names;
  }

 private:
  static std::string labelsKey(const metrics::Labels& labels) {
    std::vector<std::string> keys;
    keys.reserve(labels.size());
    for (const auto& [k, v] : labels) {
      keys.push_back(k);
    }
    std::sort(keys.begin(), keys.end());
    std::string out;
    for (const auto& k : keys) {
      out += k;
      out += '=';
      out += labels.at(k);
      out += '\0';
    }
    return out;
  }

  static std::string metricKey(const std::string& name,
                               const metrics::Labels& labels) {
    return name + '\0' + labelsKey(labels);
  }

  static std::string nameFromKey(const std::string& key) {
    const auto pos = key.find('\0');
    return pos == std::string::npos ? key : key.substr(0, pos);
  }

  template <typename T>
  T& getOrCreate(std::map<std::string, std::unique_ptr<T>>& map,
                 const std::string& name, const metrics::Labels& labels) {
    std::lock_guard<std::mutex> lock(mu_);
    auto key = metricKey(name, labels);
    auto it = map.find(key);
    if (it == map.end()) {
      it = map.emplace(std::move(key), std::make_unique<T>()).first;
    }
    return *it->second;
  }

  mutable std::mutex mu_;
  std::map<std::string, std::unique_ptr<TestInt64Counter>> counters_;
  std::map<std::string, std::unique_ptr<TestInt64Gauge>> gauges_;
  std::map<std::string, std::unique_ptr<TestFloat64Histogram>> histograms_;
  std::map<std::string, std::unique_ptr<TestObservableFloat64Gauge>>
      observableGauges_;

  friend class TestMetricsScope;
};

inline std::unique_ptr<metrics::Int64Counter> TestMetricsScope::counter(
    std::string_view name, std::string_view, const metrics::Labels& labels) {
  return std::make_unique<detail::CounterRef>(
      m_.counter(fullName(name), mergeLabels(labels)));
}

inline std::unique_ptr<metrics::Int64Gauge> TestMetricsScope::gauge(
    std::string_view name, std::string_view, const metrics::Labels& labels) {
  return std::make_unique<detail::GaugeRef>(
      m_.gauge(fullName(name), mergeLabels(labels)));
}

inline std::unique_ptr<metrics::Float64Histogram> TestMetricsScope::histogram(
    std::string_view name, std::string_view, const metrics::Labels& labels,
    std::vector<double>) {
  return std::make_unique<detail::HistogramRef>(
      m_.histogram(fullName(name), mergeLabels(labels)));
}

inline std::unique_ptr<metrics::ObservableFloat64Gauge>
TestMetricsScope::observableFloat64Gauge(std::string_view name,
                                         std::string_view,
                                         std::function<double()> value,
                                         const metrics::Labels& labels) {
  std::lock_guard<std::mutex> lock(m_.mu_);
  auto key = TestMetrics::metricKey(fullName(name), mergeLabels(labels));
  auto [it, inserted] = m_.observableGauges_.emplace(
      std::move(key),
      std::make_unique<TestObservableFloat64Gauge>(std::move(value)));
  if (!inserted) {
    throw std::logic_error("duplicate observable gauge registration");
  }
  return std::make_unique<detail::ObservableGaugeRef>(*it->second);
}

}  // namespace servicelib::testmetrics

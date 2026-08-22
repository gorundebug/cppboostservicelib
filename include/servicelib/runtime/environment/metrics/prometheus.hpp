#pragma once

#include <servicelib/runtime/environment/metrics/metrics.hpp>

#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstdint>
#include <functional>
#include <iomanip>
#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace servicelib::metrics {

class PrometheusMetrics final : public Metrics {
  struct CounterData final {
    std::atomic<std::int64_t> value{};
    std::string help;
    std::string name;
    Labels labels;
  };
  struct GaugeData final {
    std::atomic<std::int64_t> value{};
    std::string help;
    std::string name;
    Labels labels;
  };
  struct HistogramData final {
    mutable std::mutex mutex;
    std::uint64_t count{};
    double sum{};
    std::vector<double> buckets;
    std::vector<std::uint64_t> bucketCounts;
    std::string help;
    std::string name;
    Labels labels;
  };
  struct ObservableData final {
    std::function<double()> value;
    std::string help;
    std::string name;
    Labels labels;
  };

  class CounterHandle final : public Int64Counter {
   public:
    explicit CounterHandle(std::shared_ptr<CounterData> data)
        : data_(std::move(data)) {}
    void inc() override { add(1); }
    void add(std::int64_t value) override {
      if (value < 0) throw std::invalid_argument("counter cannot decrease");
      data_->value.fetch_add(value, std::memory_order_relaxed);
    }
   private:
    std::shared_ptr<CounterData> data_;
  };
  class GaugeHandle final : public Int64Gauge {
   public:
    explicit GaugeHandle(std::shared_ptr<GaugeData> data)
        : data_(std::move(data)) {}
    void set(std::int64_t value) override {
      data_->value.store(value, std::memory_order_relaxed);
    }
    void inc() override { add(1); }
    void dec() override { sub(1); }
    void add(std::int64_t value) override {
      data_->value.fetch_add(value, std::memory_order_relaxed);
    }
    void sub(std::int64_t value) override {
      data_->value.fetch_sub(value, std::memory_order_relaxed);
    }
   private:
    std::shared_ptr<GaugeData> data_;
  };
  class HistogramHandle final : public Float64Histogram {
   public:
    explicit HistogramHandle(std::shared_ptr<HistogramData> data)
        : data_(std::move(data)) {}
    void observe(double value) override {
      std::lock_guard lock(data_->mutex);
      ++data_->count;
      data_->sum += value;
      for (std::size_t index = 0; index < data_->buckets.size(); ++index)
        if (value <= data_->buckets[index]) ++data_->bucketCounts[index];
    }
   private:
    std::shared_ptr<HistogramData> data_;
  };
  class ObservableHandle final : public ObservableFloat64Gauge {
   public:
    explicit ObservableHandle(std::shared_ptr<ObservableData> data)
        : data_(std::move(data)) {}
    double value() const override { return data_->value(); }
   private:
    std::shared_ptr<ObservableData> data_;
  };

  class Scope final : public MetricsScope {
   public:
    Scope(PrometheusMetrics& owner, std::string prefix, Labels labels)
        : owner_(owner), prefix_(std::move(prefix)), labels_(std::move(labels)) {}
    std::unique_ptr<Int64Counter> counter(
        std::string_view name, std::string_view help,
        const Labels& labels) override {
      return owner_.Counter(FullName(name), help, Merge(labels));
    }
    std::unique_ptr<Int64Gauge> gauge(
        std::string_view name, std::string_view help,
        const Labels& labels) override {
      return owner_.Gauge(FullName(name), help, Merge(labels));
    }
    std::unique_ptr<Float64Histogram> histogram(
        std::string_view name, std::string_view help, const Labels& labels,
        std::vector<double> buckets) override {
      return owner_.Histogram(FullName(name), help, Merge(labels),
                              std::move(buckets));
    }
    std::unique_ptr<ObservableFloat64Gauge> observableFloat64Gauge(
        std::string_view name, std::string_view help,
        std::function<double()> value, const Labels& labels) override {
      return owner_.Observable(FullName(name), help, Merge(labels),
                               std::move(value));
    }
   private:
    std::string FullName(std::string_view name) const {
      if (prefix_.empty()) return std::string(name);
      if (name.empty()) return prefix_;
      return prefix_ + "." + std::string(name);
    }
    Labels Merge(const Labels& labels) const {
      auto result = labels_;
      for (const auto& [key, value] : labels) result[key] = value;
      return result;
    }
    PrometheusMetrics& owner_;
    std::string prefix_;
    Labels labels_;
  };

 public:
  std::unique_ptr<MetricsScope> scope(std::string_view prefix,
                                      const Labels& labels) override {
    return std::make_unique<Scope>(*this, std::string(prefix), labels);
  }

  [[nodiscard]] std::string Expose() const {
    std::vector<std::shared_ptr<CounterData>> counters;
    std::vector<std::shared_ptr<GaugeData>> gauges;
    std::vector<std::shared_ptr<HistogramData>> histograms;
    std::vector<std::shared_ptr<ObservableData>> observables;
    {
      std::lock_guard lock(mutex_);
      for (const auto& [unused, value] : counters_) counters.push_back(value);
      for (const auto& [unused, value] : gauges_) gauges.push_back(value);
      for (const auto& [unused, value] : histograms_) histograms.push_back(value);
      for (const auto& [unused, value] : observables_) observables.push_back(value);
    }
    std::ostringstream out;
    out << std::setprecision(17);
    for (const auto& value : counters) {
      Header(out, value->name, value->help, "counter");
      Sample(out, value->name, value->labels,
             value->value.load(std::memory_order_relaxed));
    }
    for (const auto& value : gauges) {
      Header(out, value->name, value->help, "gauge");
      Sample(out, value->name, value->labels,
             value->value.load(std::memory_order_relaxed));
    }
    for (const auto& value : observables) {
      Header(out, value->name, value->help, "gauge");
      Sample(out, value->name, value->labels, value->value());
    }
    for (const auto& value : histograms) {
      Header(out, value->name, value->help, "histogram");
      std::lock_guard lock(value->mutex);
      for (std::size_t index = 0; index < value->buckets.size(); ++index) {
        auto labels = value->labels;
        labels["le"] = Number(value->buckets[index]);
        Sample(out, value->name + "_bucket", labels,
               value->bucketCounts[index]);
      }
      auto infinite = value->labels;
      infinite["le"] = "+Inf";
      Sample(out, value->name + "_bucket", infinite, value->count);
      Sample(out, value->name + "_sum", value->labels, value->sum);
      Sample(out, value->name + "_count", value->labels, value->count);
    }
    return out.str();
  }

 private:
  std::unique_ptr<Int64Counter> Counter(std::string name, std::string_view help,
                                        Labels labels) {
    const auto key = Key(name, labels);
    std::lock_guard lock(mutex_);
    auto& value = counters_[key];
    if (!value) {
      value = std::make_shared<CounterData>();
      value->help = help;
      value->name = std::move(name);
      value->labels = std::move(labels);
    }
    return std::make_unique<CounterHandle>(value);
  }
  std::unique_ptr<Int64Gauge> Gauge(std::string name, std::string_view help,
                                    Labels labels) {
    const auto key = Key(name, labels);
    std::lock_guard lock(mutex_);
    auto& value = gauges_[key];
    if (!value) {
      value = std::make_shared<GaugeData>();
      value->help = help;
      value->name = std::move(name);
      value->labels = std::move(labels);
    }
    return std::make_unique<GaugeHandle>(value);
  }
  std::unique_ptr<Float64Histogram> Histogram(
      std::string name, std::string_view help, Labels labels,
      std::vector<double> buckets) {
    if (buckets.empty()) buckets = {.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10};
    std::sort(buckets.begin(), buckets.end());
    buckets.erase(std::unique(buckets.begin(), buckets.end()), buckets.end());
    const auto key = Key(name, labels);
    std::lock_guard lock(mutex_);
    auto& value = histograms_[key];
    if (!value) {
      value = std::make_shared<HistogramData>();
      value->help = help;
      value->name = std::move(name);
      value->labels = std::move(labels);
      value->buckets = std::move(buckets);
      value->bucketCounts.resize(value->buckets.size());
    } else if (value->buckets != buckets) {
      throw std::invalid_argument("histogram registered with different buckets");
    }
    return std::make_unique<HistogramHandle>(value);
  }
  std::unique_ptr<ObservableFloat64Gauge> Observable(
      std::string name, std::string_view help, Labels labels,
      std::function<double()> callback) {
    if (!callback) throw std::invalid_argument("observable callback is required");
    const auto key = Key(name, labels);
    std::lock_guard lock(mutex_);
    auto& value = observables_[key];
    if (!value)
      value = std::make_shared<ObservableData>(ObservableData{
          std::move(callback), std::string(help), std::move(name),
          std::move(labels)});
    return std::make_unique<ObservableHandle>(value);
  }

  static std::string MetricName(std::string value) {
    std::replace(value.begin(), value.end(), '.', '_');
    std::replace(value.begin(), value.end(), '-', '_');
    return value;
  }
  static std::string Escape(std::string_view value) {
    std::string result;
    for (const char character : value) {
      if (character == '\\' || character == '"') result.push_back('\\');
      if (character == '\n') result += "\\n";
      else result.push_back(character);
    }
    return result;
  }
  static std::string Key(const std::string& name, const Labels& labels) {
    std::map<std::string, std::string> ordered(labels.begin(), labels.end());
    std::string result = name;
    for (const auto& [key, value] : ordered) {
      result.push_back('\0');
      result += key + "=" + value;
    }
    return result;
  }
  static std::string Number(double value) {
    std::ostringstream out;
    out << std::setprecision(17) << value;
    return out.str();
  }
  template <typename NumberType>
  static void Sample(std::ostream& out, const std::string& name,
                     const Labels& labels, NumberType value) {
    out << MetricName(name);
    if (!labels.empty()) {
      std::map<std::string, std::string> ordered(labels.begin(), labels.end());
      out << '{';
      bool first = true;
      for (const auto& [key, label] : ordered) {
        if (!first) out << ',';
        first = false;
        out << MetricName(key) << "=\"" << Escape(label) << '"';
      }
      out << '}';
    }
    out << ' ' << value << '\n';
  }
  static void Header(std::ostream& out, const std::string& name,
                     const std::string& help, const char* type) {
    out << "# HELP " << MetricName(name) << ' ' << Escape(help) << '\n';
    out << "# TYPE " << MetricName(name) << ' ' << type << '\n';
  }

  mutable std::mutex mutex_;
  std::map<std::string, std::shared_ptr<CounterData>> counters_;
  std::map<std::string, std::shared_ptr<GaugeData>> gauges_;
  std::map<std::string, std::shared_ptr<HistogramData>> histograms_;
  std::map<std::string, std::shared_ptr<ObservableData>> observables_;
};

}  // namespace servicelib::metrics

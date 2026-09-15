#include <algorithm>
#include <cstdlib>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>


#include <gtest/gtest.h>

#include <servicelib/runtime/base.hpp>
#include <servicelib/runtime/caller.hpp>
#include <servicelib/runtime/datasink.hpp>
#include <servicelib/runtime/datasource.hpp>
#include <servicelib/runtime/environment_variable.hpp>
#include <servicelib/runtime/testlog/testlog.hpp>
#include <servicelib/runtime/testmetrics/testmetrics.hpp>
#include <servicelib/runtime/testtracing/testtracing.hpp>

namespace {

class ThrowingCounter final : public servicelib::metrics::Int64Counter {
 public:
  void inc() override { throw std::runtime_error("telemetry failure"); }
  void add(int64_t) override { throw std::runtime_error("telemetry failure"); }
};

class TestCallerBase final : public servicelib::CallerBase {
 public:
  TestCallerBase()
      : CallerBase({.sourceName = "source",
                    .consumerName = "consumer",
                    .tracer = {},
                    .metricsEnabled = true,
                    .messagesCounter = std::make_unique<ThrowingCounter>()}) {}

  bool isAsync() const noexcept override { return false; }
  void record() noexcept { recordMessage(); }
};

}  // namespace

TEST(TestTelemetry, EnvironmentFlagsUseStrictBooleanValues) {
  constexpr auto kName = "SERVICELIB_TEST_BOOLEAN_FLAG";
  for (const char* value : {"1", "true", "TRUE", " yes ", "On"}) {
    ASSERT_EQ(setenv(kName, value, 1), 0);
    EXPECT_TRUE(servicelib::EnvironmentFlagEnabled(kName));
  }
  for (const char* value : {"", "0", "false", "no", "off", "anything"}) {
    ASSERT_EQ(setenv(kName, value, 1), 0);
    EXPECT_FALSE(servicelib::EnvironmentFlagEnabled(kName));
  }
  ASSERT_EQ(unsetenv(kName), 0);
  EXPECT_FALSE(servicelib::EnvironmentFlagEnabled(kName));
}

TEST(TestTelemetry, MetricsRecordAllInstrumentKindsAndReset) {
  servicelib::testmetrics::TestMetrics metrics;
  EXPECT_TRUE(metrics.enabled());
  auto scope = metrics.scope("worker", {{"service", "test-service"}});
  auto counter = scope->counter("tasks_total", "Tasks", {{"pool", "default"}});
  auto gauge = scope->gauge("queue_length", "Queue length");
  auto histogram =
      scope->histogram("duration_seconds", "Duration", {}, {0.1, 1.0});
  double observed = 3.5;
  auto observable = scope->observableFloat64Gauge(
      "oldest_age_seconds", "Oldest age", [&observed] { return observed; });

  counter->inc();
  counter->add(2);
  gauge->set(4);
  gauge->dec();
  histogram->observe(0.25);

  EXPECT_EQ(metrics
                .counter("worker.tasks_total",
                         {{"service", "test-service"}, {"pool", "default"}})
                .count(),
            3);
  EXPECT_EQ(metrics.gauge("worker.queue_length", {{"service", "test-service"}})
                .value(),
            3);
  EXPECT_EQ(
      metrics
          .histogram("worker.duration_seconds", {{"service", "test-service"}})
          .count(),
      1);
  EXPECT_DOUBLE_EQ(observable->value(), 3.5);

  const auto names = metrics.registeredNames();
  EXPECT_EQ(names.size(), 4);
  EXPECT_TRUE(std::binary_search(names.begin(), names.end(),
                                 "worker.oldest_age_seconds"));

  metrics.reset();
  EXPECT_TRUE(metrics.registeredNames().empty());
}

TEST(TestTelemetry, NoopMetricsAdvertiseAndUseDisabledFastPath) {
  auto& metrics = servicelib::metrics::NoopMetrics::instance();
  EXPECT_FALSE(metrics.enabled());

  servicelib::testlog::TestLog log;
  servicelib::DataSourceEndpointMetrics source{metrics, log, "connector",
                                               "source"};
  servicelib::DataSinkEndpointMetrics sink{metrics, log, "connector", "sink"};
  EXPECT_EQ(source.requestStart(),
            servicelib::DataSourceEndpointMetrics::Clock::time_point{});
  EXPECT_EQ(sink.requestStart(),
            servicelib::DataSinkEndpointMetrics::Clock::time_point{});

  source.pendingAdd("request");
  source.pendingRemove("request");
  source.requestEnd({}, {});
  sink.requestEnd({}, {});
}

TEST(TestTelemetry, EmptyMetricScopeDoesNotPrefixNamesWithDot) {
  servicelib::testmetrics::TestMetrics metrics;
  auto scope = metrics.scope("", {});
  auto counter = scope->counter("events_total", "Events");
  counter->inc();

  EXPECT_EQ(metrics.counter("events_total", {}).count(), 1);
  EXPECT_EQ(metrics.registeredNames(),
            std::vector<std::string>{"events_total"});
}

TEST(TestTelemetry, LogCapturesStructuredEntriesAndSupportsReset) {
  servicelib::testlog::TestLog log;
  log.debug("debug event", {});
  log.info("info event", {});
  log.warn("request failed",
           {servicelib::log::Field::Str("endpoint", "orders"),
            servicelib::log::Field::Int64("attempt", 2),
            servicelib::log::Field::Float64("ratio", 1.5),
            servicelib::log::Field::Bool("retry", true)});
  log.error("shutdown failed", {servicelib::log::Field::Err("timeout")});

  const auto entries = log.entries();
  ASSERT_EQ(entries.size(), 4);
  EXPECT_EQ(entries[0].level, servicelib::log::Level::kDebug);
  EXPECT_EQ(entries[1].level, servicelib::log::Level::kInfo);
  EXPECT_EQ(entries[2].level, servicelib::log::Level::kWarn);
  EXPECT_EQ(entries[3].level, servicelib::log::Level::kError);
  EXPECT_EQ(entries[2].message, "request failed");
  ASSERT_EQ(entries[2].fields.size(), 4);
  EXPECT_EQ(entries[2].fields[0].key(), "endpoint");
  EXPECT_EQ(std::get<std::string>(entries[2].fields[0].value()), "orders");
  EXPECT_EQ(entries[2].fields[1].key(), "attempt");
  EXPECT_EQ(std::get<int64_t>(entries[2].fields[1].value()), 2);
  EXPECT_EQ(entries[2].fields[2].key(), "ratio");
  EXPECT_DOUBLE_EQ(std::get<double>(entries[2].fields[2].value()), 1.5);
  EXPECT_EQ(entries[2].fields[3].key(), "retry");
  EXPECT_TRUE(std::get<bool>(entries[2].fields[3].value()));
  ASSERT_EQ(entries[3].fields.size(), 1);
  EXPECT_EQ(entries[3].fields[0].key(), "error");
  EXPECT_EQ(std::get<std::string>(entries[3].fields[0].value()), "timeout");
  EXPECT_EQ(log.entriesAtLevel(servicelib::log::Level::kError).size(), 1);

  log.reset();
  EXPECT_TRUE(log.entries().empty());
}

TEST(TestTelemetry, TracingCapturesAttributesEventsErrorsAndIsIdempotent) {
  servicelib::testtracing::TestTracing tracing;
  auto tracer = tracing.tracer("test-service");
  auto span = tracer->start(
      "stream.call", {servicelib::tracing::Attribute::String("from", "input")});
  span->addEvent("accepted",
                 {servicelib::tracing::Attribute::Int64("priority", 10)});
  span->recordError("failed");
  span->setStatus(servicelib::tracing::StatusCode::kError, "failed");
  span->end();
  span->end();

  const auto spans = tracing.spans();
  ASSERT_EQ(spans.size(), 1);
  EXPECT_EQ(spans[0].name, "stream.call");
  ASSERT_EQ(spans[0].attributes.size(), 1);
  ASSERT_EQ(spans[0].events.size(), 1);
  EXPECT_EQ(spans[0].events[0].name, "accepted");
  EXPECT_EQ(spans[0].error, "failed");
  EXPECT_EQ(spans[0].statusCode, servicelib::tracing::StatusCode::kError);
  EXPECT_EQ(spans[0].statusDescription, "failed");

  tracing.reset();
  EXPECT_TRUE(tracing.spans().empty());
}

TEST(TestTelemetry, InstrumentFailureDoesNotChangeDeliveryStatistics) {
  TestCallerBase caller;

  caller.record();

  EXPECT_EQ(caller.statistics().count(), 1);
}

TEST(TestTelemetry, SourceEndpointRecordsTheGoEventAndPendingMetricSet) {
  servicelib::testmetrics::TestMetrics metrics;
  servicelib::testlog::TestLog log;
  servicelib::DataSourceEndpointMetrics endpoint{metrics, log, "connector",
                                                 "endpoint"};
  const servicelib::metrics::Labels base{{"connector", "connector"},
                                         {"endpoint", "endpoint"}};

  endpoint.missingStreamId();
  endpoint.lateResult("stream");
  endpoint.unknownMessageId("stream", "message");
  endpoint.duplicateMessageId("stream", "message");
  endpoint.invalidHttpMethod("PUT", "/messages");
  endpoint.beginRequestFailed("failed");
  endpoint.pendingAdd("stream");

  const auto eventCount = [&metrics, &base](std::string event) {
    auto labels = base;
    labels.emplace("event", std::move(event));
    return metrics.counter("datasource_endpoint.events_total", labels).count();
  };
  EXPECT_EQ(eventCount("missing_stream_id"), 1);
  EXPECT_EQ(eventCount("late_result"), 1);
  EXPECT_EQ(eventCount("unknown_message_id"), 1);
  EXPECT_EQ(eventCount("duplicate_message_id"), 1);
  EXPECT_EQ(eventCount("invalid_http_method"), 1);
  EXPECT_EQ(eventCount("begin_request_failed"), 1);
  EXPECT_EQ(log.entries().size(), 6);
  EXPECT_EQ(metrics.gauge("datasource_endpoint.pending_requests", base).value(),
            1);

  endpoint.pendingRemove("stream");
  EXPECT_EQ(metrics.gauge("datasource_endpoint.pending_requests", base).value(),
            0);
  EXPECT_DOUBLE_EQ(
      metrics
          .observableGauge("datasource_endpoint.pending_oldest_age_seconds",
                           base)
          .value(),
      0.0);
}

TEST(TestTelemetry, SinkEndpointRecordsLateResults) {
  servicelib::testmetrics::TestMetrics metrics;
  servicelib::testlog::TestLog log;
  servicelib::DataSinkEndpointMetrics endpoint{metrics, log, "connector",
                                               "endpoint"};
  endpoint.lateResult("stream");

  EXPECT_EQ(metrics
                .counter("datasink_endpoint.events_total",
                         {{"connector", "connector"},
                          {"endpoint", "endpoint"},
                          {"event", "late_result"}})
                .count(),
            1);
  ASSERT_EQ(log.entries().size(), 1);
  EXPECT_EQ(log.entries()[0].level, servicelib::log::Level::kWarn);
}

// Component grouping is bounded definition metadata, not an execution wrapper.
namespace {
class GroupedCaller final : public servicelib::CallerBase {
 public:
  using CallerBase::CallerBase;
  bool isAsync() const noexcept override { return false; }
  void call(servicelib::MessageContext context, std::string_view type = {}, std::string_view pool = {}) {
    recordMessage();
    [[maybe_unused]] auto span = startCallSpan(context, type, pool);
  }
};
class GroupedStream final : public servicelib::StreamBase {
 public:
  template<typename Config> explicit GroupedStream(const Config& config) { setConfigIdentity(config); }
  void copy(const GroupedStream& other) { copySettings(other); }
 private:
  size_t buildTopology(servicelib::StreamBuilderContext&, size_t, std::vector<size_t>*, bool) override { return 1; }
  void verifyTopology(servicelib::StreamVerifyContext&) const override {}
  void printTopology(servicelib::TopologyPrinter&, std::unordered_set<size_t>&) const override {}
};
}  // namespace

#include <filesystem>
#include <fstream>
#include <iterator>

TEST(ComponentObservability, AllSinkSpanPathsIncludeCachedDefinitionLabels) {
  const auto root = std::filesystem::path{__FILE__}.parent_path().parent_path();
  for (const std::string relative : {"http/beast.hpp", "grpc/common.hpp",
                                    "kafka/librdkafka.hpp", "localsink/custom.hpp"}) {
    std::ifstream input{root / "include/servicelib/datasink" / relative};
    ASSERT_TRUE(input.good()) << relative;
    const std::string source{std::istreambuf_iterator<char>{input},
                             std::istreambuf_iterator<char>{}};
    const auto count = [&](std::string_view needle) {
      std::size_t found = 0;
      for (auto position = source.find(needle); position != std::string::npos;
           position = source.find(needle, position + needle.size())) ++found;
      return found;
    };
    const std::size_t expected = relative == "grpc/common.hpp" ? 2 : 1;
    EXPECT_EQ(count("tracing::Attribute::String(\"pipeline\", streamIdentity_.pipeline)"), expected) << relative;
    EXPECT_EQ(count("tracing::Attribute::String(\"component\", streamIdentity_.component)"), expected) << relative;
    EXPECT_EQ(count("tracing::Attribute::String(\"stream\", streamIdentity_.name)"), expected) << relative;
    EXPECT_NE(source.find("StreamTraceIdentity streamIdentity_;"), std::string::npos) << relative;
    EXPECT_NE(source.find("streamIdentity_(resolveStreamIdentity("), std::string::npos) << relative;
    EXPECT_EQ(source.find("component_instance"), std::string::npos) << relative;
  }
}

TEST(ComponentObservability, AllStreamConfigsParseOptionalGroupingAsTypedFields) {
  const auto parse = [](std::string text) {
    return servicelib::config::YamlValue(YAML::Load(text));
  };
  const auto check = [&]<typename Config>() {
    const auto empty = parse("id: 1\nname: Stage\n").template As<Config>();
    EXPECT_TRUE(servicelib::config::StreamConfigRef(empty).GetComponent().empty());
    const auto config = parse("id: 1\nname: Stage\npipeline: pricing\ncomponent: Customer Pricing\ncustomField: preserved\n").template As<Config>();
    const servicelib::config::StreamConfigRef ref(config);
    EXPECT_EQ(ref.GetPipeline(), "pricing"); EXPECT_EQ(ref.GetComponent(), "Customer Pricing");
    EXPECT_EQ(config.GetProperty("component"), nullptr);
    EXPECT_NE(config.GetProperty("customField"), nullptr);
    const GroupedStream stream(config);
    EXPECT_EQ(stream.getPipeline(), "pricing"); EXPECT_EQ(stream.getComponent(), "Customer Pricing");
    GroupedStream copy(empty); copy.copy(stream);
    EXPECT_EQ(copy.getPipeline(), "pricing"); EXPECT_EQ(copy.getComponent(), "Customer Pricing");
  };
  check.template operator()<servicelib::config::InputStreamConfig>();
  check.template operator()<servicelib::config::MapStreamConfig>();
  check.template operator()<servicelib::config::FilterStreamConfig>();
  check.template operator()<servicelib::config::JoinStreamConfig>();
  check.template operator()<servicelib::config::MultiJoinStreamConfig>();
  check.template operator()<servicelib::config::ProcessStreamConfig>();
  check.template operator()<servicelib::config::FlatMapStreamConfig>();
  check.template operator()<servicelib::config::FlatMapIterableStreamConfig>();
  check.template operator()<servicelib::config::KeyByStreamConfig>();
  check.template operator()<servicelib::config::MergeStreamConfig>();
  check.template operator()<servicelib::config::SplitStreamConfig>();
  check.template operator()<servicelib::config::CaseStreamConfig>();
  check.template operator()<servicelib::config::WhenStreamConfig>();
  check.template operator()<servicelib::config::SinkStreamConfig>();
  check.template operator()<servicelib::config::CycleLinkStreamConfig>();
  check.template operator()<servicelib::config::DelayStreamConfig>();
}

TEST(ComponentObservability, CallSpansAndExistingCountersKeepDefinitionLabelsAndSampling) {
  servicelib::testtracing::TestTracing tracing;
  servicelib::testmetrics::TestMetrics metrics;
  const servicelib::metrics::Labels labels{{"service", "Booking"}, {"from", "Request"}, {"to", "Price"}, {"pipeline", "pricing"}, {"component", "Customer Pricing"}};
  auto scope=metrics.scope("stream", labels);
  GroupedCaller caller({.sourceName="Request", .consumerName="Price", .pipeline="pricing", .component="Customer Pricing",
      .tracer=tracing.tracer("Booking"), .messagesCounter=scope->counter("messages_total", "Messages")});
  caller.call({}); EXPECT_TRUE(tracing.spans().empty());
  const auto sampled=servicelib::tracing::EnableSampling(servicelib::MessageContext{});
  caller.call(sampled); caller.call(sampled, "parallel"); caller.call(sampled, "taskpool", "Workers"); caller.call(sampled, "prioritytaskpool", "Priority Workers");
  EXPECT_EQ(caller.statistics().count(),5);
  EXPECT_EQ(metrics.counter("stream.messages_total",labels).count(),5);
  const auto spans=tracing.spans(); ASSERT_EQ(spans.size(),4);
  for(const auto& span:spans) {
    EXPECT_EQ(span.name,"stream.call");
    const auto attribute=[&](const std::string& key) {
      const auto it=std::find_if(span.attributes.begin(),span.attributes.end(),[&](const auto& value){return value.key()==key;});
      return it==span.attributes.end()?std::string():std::get<std::string>(it->value());
    };
    EXPECT_EQ(attribute("pipeline"),"pricing"); EXPECT_EQ(attribute("component"),"Customer Pricing");
    EXPECT_EQ(attribute("to"),"Price"); EXPECT_TRUE(attribute("component_instance").empty());
  }
}

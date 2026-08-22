#include <algorithm>
#include <array>
#include <cstdlib>
#include <memory>
#include <sstream>
#include <string>

#include <gtest/gtest.h>

#include <opentelemetry/exporters/memory/in_memory_span_exporter.h>
#include <opentelemetry/exporters/ostream/log_record_exporter.h>
#include <opentelemetry/sdk/logs/logger_provider_factory.h>
#include <opentelemetry/sdk/logs/simple_log_record_processor_factory.h>
#include <opentelemetry/sdk/trace/simple_processor_factory.h>
#include <opentelemetry/sdk/trace/tracer_provider_factory.h>

#include <servicelib/runtime/telemetry/opentelemetry/opentelemetry.hpp>

namespace {

namespace otel = ::opentelemetry;
namespace adapter = servicelib::telemetry::opentelemetry_adapter;

std::string SpanId(const otel::trace::SpanId& span_id) {
  std::array<char, otel::trace::SpanId::kSize * 2> value{};
  span_id.ToLowerBase16(
      otel::nostd::span<char, otel::trace::SpanId::kSize * 2>{value});
  return {value.data(), value.size()};
}

TEST(OpenTelemetry, PreservesTypedSpanDataExplicitParentageAndLifecycle) {
  auto exporter =
      std::make_unique<otel::exporter::memory::InMemorySpanExporter>();
  auto data = exporter->GetData();
  auto processor = otel::sdk::trace::SimpleSpanProcessorFactory::Create(
      std::move(exporter));
  auto provider = otel::sdk::trace::TracerProviderFactory::Create(
      std::move(processor), adapter::ServiceResource("orders"));
  adapter::OpenTelemetryTracing tracing(std::move(provider));
  auto tracer = tracing.tracer("orders");

  auto root = tracer->start(
      "stream.call",
      {servicelib::tracing::Attribute::String("from", "input"),
       servicelib::tracing::Attribute::Int64("priority", 7),
       servicelib::tracing::Attribute::Float64("ratio", 0.5),
       servicelib::tracing::Attribute::Bool("sampled", true)});
  const auto root_context = root->spanContext();
  ASSERT_TRUE(root_context.isValid());
  EXPECT_EQ(root_context.traceId.size(), 32);
  EXPECT_EQ(root_context.spanId.size(), 16);

  auto child = tracer->startDetachedChildOf(
      "stream.delay", root_context,
      {servicelib::tracing::Attribute::String("stream", "Soft Deadline")});
  child->addEvent(
      "accepted",
      {servicelib::tracing::Attribute::Int64("attempt", 2)});
  child->recordError("deadline exceeded");
  child->setStatus(servicelib::tracing::StatusCode::kError,
                   "deadline exceeded");
  child->end();
  child->end();
  root->end();

  ASSERT_TRUE(tracing.forceFlush());
  auto spans = data->GetSpans();
  ASSERT_EQ(spans.size(), 2);
  const auto child_it = std::find_if(
      spans.begin(), spans.end(), [](const auto& span) {
        return std::string(span->GetName()) == "stream.delay";
      });
  ASSERT_NE(child_it, spans.end());
  const auto& child_data = **child_it;
  EXPECT_EQ(SpanId(child_data.GetParentSpanId()), root_context.spanId);
  EXPECT_EQ(child_data.GetStatus(), otel::trace::StatusCode::kError);
  EXPECT_EQ(std::string(child_data.GetDescription()), "deadline exceeded");
  EXPECT_TRUE(child_data.GetAttributes().contains("stream"));
  EXPECT_TRUE(child_data.GetAttributes().contains("error"));
  ASSERT_EQ(child_data.GetEvents().size(), 2);
  EXPECT_EQ(child_data.GetEvents()[0].GetName(), "accepted");
  EXPECT_EQ(child_data.GetEvents()[1].GetName(), "exception");

  const auto& resource = child_data.GetResource().GetAttributes();
  EXPECT_TRUE(resource.contains("service.name"));
}

TEST(OpenTelemetry, RejectsMalformedExplicitParentInsteadOfForgingTraceIds) {
  auto exporter =
      std::make_unique<otel::exporter::memory::InMemorySpanExporter>();
  auto data = exporter->GetData();
  auto processor = otel::sdk::trace::SimpleSpanProcessorFactory::Create(
      std::move(exporter));
  auto provider = otel::sdk::trace::TracerProviderFactory::Create(
      std::move(processor));
  adapter::OpenTelemetryTracing tracing(std::move(provider));
  auto tracer = tracing.tracer("orders");

  auto span = tracer->startChildOf(
      "invalid-parent",
      {.traceId = "not-a-trace-id",
       .spanId = "bad",
       .valid = true,
       .traceState = {},
       .baggage = {}});
  const auto context = span->spanContext();
  EXPECT_TRUE(context.isValid());
  EXPECT_NE(context.traceId, "not-a-trace-id");
  span->end();
  ASSERT_TRUE(tracing.forceFlush());
  const auto spans = data->GetSpans();
  ASSERT_EQ(spans.size(), 1);
  EXPECT_FALSE(spans[0]->GetParentSpanId().IsValid());
}

TEST(OpenTelemetry, EmitsTypedStructuredLogsAndFlushesBeforeShutdown) {
  std::ostringstream output;
  auto exporter =
      std::make_unique<otel::exporter::logs::OStreamLogRecordExporter>(output);
  auto processor =
      otel::sdk::logs::SimpleLogRecordProcessorFactory::Create(
          std::move(exporter));
  auto provider = otel::sdk::logs::LoggerProviderFactory::Create(
      std::move(processor), adapter::ServiceResource("orders"));
  adapter::OpenTelemetryLogger logger(std::move(provider), "orders");

  logger.warn("request failed",
              {servicelib::log::Field::Str("endpoint", "orders"),
               servicelib::log::Field::Int64("attempt", 2),
               servicelib::log::Field::Float64("delay", 1.5),
               servicelib::log::Field::Bool("retry", true)});
  ASSERT_TRUE(logger.forceFlush());
  EXPECT_NE(output.str().find("request failed"), std::string::npos);
  EXPECT_NE(output.str().find("endpoint"), std::string::npos);
  EXPECT_NE(output.str().find("orders"), std::string::npos);
  EXPECT_NE(output.str().find("attempt"), std::string::npos);
  EXPECT_TRUE(logger.shutdown());
  EXPECT_TRUE(logger.shutdown());
}

TEST(OpenTelemetry, ExportsStructuredLogsThroughRealOtlpGrpcCollector) {
  if (std::getenv("CPPBOOSTSERVICELIB_RUN_OTLP_LOG_INTEGRATION") == nullptr) {
    GTEST_SKIP() << "real OTLP collector integration is opt-in";
  }

  auto logger = adapter::CreateOTLPLogsEngine("cppboost-otel-log-probe");
  logger->info("cppboost real otlp log probe",
               {servicelib::log::Field::Str("endpoint", "collector"),
                servicelib::log::Field::Int64("attempt", 7),
                servicelib::log::Field::Float64("ratio", 0.25),
                servicelib::log::Field::Bool("verified", true)});
  ASSERT_TRUE(logger->forceFlush());
  EXPECT_TRUE(logger->shutdown());
}

}  // namespace

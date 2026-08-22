/*
 * opentelemetry.hpp
 * OTLP factories and the single include point for Boost ServiceLib telemetry.
 */
#pragma once

#include <memory>
#include <iostream>
#include <string>
#include <string_view>
#include <utility>

// The pinned OTel release documents that the gRPC exporter must precede other
// OTel API headers when gRPC and OTel use different Abseil configurations.
#include <opentelemetry/exporters/otlp/otlp_grpc_exporter_factory.h>
#include <opentelemetry/exporters/otlp/otlp_grpc_exporter_options.h>
#include <opentelemetry/exporters/otlp/otlp_grpc_log_record_exporter_factory.h>
#include <opentelemetry/exporters/otlp/otlp_grpc_log_record_exporter_options.h>
#include <opentelemetry/exporters/ostream/span_exporter.h>
#include <opentelemetry/sdk/common/attribute_utils.h>
#include <opentelemetry/sdk/logs/batch_log_record_processor_factory.h>
#include <opentelemetry/sdk/logs/batch_log_record_processor_options.h>
#include <opentelemetry/sdk/logs/logger_provider_factory.h>
#include <opentelemetry/sdk/resource/resource.h>
#include <opentelemetry/sdk/trace/batch_span_processor_factory.h>
#include <opentelemetry/sdk/trace/batch_span_processor_options.h>
#include <opentelemetry/sdk/trace/simple_processor_factory.h>
#include <opentelemetry/sdk/trace/tracer_provider_factory.h>

#include <servicelib/runtime/telemetry/opentelemetry/log.hpp>
#include <servicelib/runtime/telemetry/opentelemetry/tracing.hpp>

namespace servicelib::telemetry::opentelemetry_adapter {

inline otel::sdk::resource::Resource ServiceResource(
    std::string_view service_name) {
  otel::sdk::resource::ResourceAttributes attributes;
  attributes["service.name"] = std::string(service_name);
  return otel::sdk::resource::Resource::Create(attributes);
}

inline std::unique_ptr<OpenTelemetryTracing> CreateOTLPTracingEngine(
    std::string_view service_name,
    const otel::exporter::otlp::OtlpGrpcExporterOptions& options = {}) {
  auto exporter =
      otel::exporter::otlp::OtlpGrpcExporterFactory::Create(options);
  auto processor = otel::sdk::trace::BatchSpanProcessorFactory::Create(
      std::move(exporter), otel::sdk::trace::BatchSpanProcessorOptions{});
  auto provider = otel::sdk::trace::TracerProviderFactory::Create(
      std::move(processor), ServiceResource(service_name));
  return std::make_unique<OpenTelemetryTracing>(std::move(provider));
}

inline std::unique_ptr<OpenTelemetryTracing> CreateStdoutTracingEngine(
    std::string_view service_name, std::ostream& output = std::clog) {
  auto exporter =
      std::make_unique<otel::exporter::trace::OStreamSpanExporter>(output);
  auto processor = otel::sdk::trace::SimpleSpanProcessorFactory::Create(
      std::move(exporter));
  auto provider = otel::sdk::trace::TracerProviderFactory::Create(
      std::move(processor), ServiceResource(service_name));
  return std::make_unique<OpenTelemetryTracing>(std::move(provider));
}

inline std::unique_ptr<OpenTelemetryLogger> CreateOTLPLogsEngine(
    std::string_view service_name,
    const otel::exporter::otlp::OtlpGrpcLogRecordExporterOptions& options = {}) {
  auto exporter =
      otel::exporter::otlp::OtlpGrpcLogRecordExporterFactory::Create(options);
  auto processor =
      otel::sdk::logs::BatchLogRecordProcessorFactory::Create(
          std::move(exporter),
          otel::sdk::logs::BatchLogRecordProcessorOptions{});
  auto provider = otel::sdk::logs::LoggerProviderFactory::Create(
      std::move(processor), ServiceResource(service_name));
  return std::make_unique<OpenTelemetryLogger>(std::move(provider),
                                               service_name);
}

}  // namespace servicelib::telemetry::opentelemetry_adapter

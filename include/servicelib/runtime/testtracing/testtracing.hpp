/*
 * testtracing.hpp
 * In-memory Tracer for use in automated tests. Every span, once ended, is
 * recorded and accessible via TestTracing::spans() for assertions.
 *
 * Go analog: runtime/testtracing/testtracing.go (TestTracing).
 *
 * Usage:
 *   servicelib::testtracing::TestTracing tracing;
 *   // wire tracing into IServiceEnvironment::getTracing()
 *   doWork();
 *   auto spans = tracing.spans();
 *   ASSERT_EQ(spans[0].name, "stream.call");
 *
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the
 * [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */
#pragma once

#include <memory>
#include <string>
#include <vector>

#include <mutex>

#include <servicelib/runtime/environment/tracing/tracing.hpp>

namespace servicelib::testtracing {

struct RecordedEvent {
  std::string name;
  std::vector<tracing::Attribute> attributes;
};

struct RecordedSpan {
  std::string name;
  std::vector<tracing::Attribute> attributes;
  std::vector<RecordedEvent> events;
  tracing::StatusCode statusCode = tracing::StatusCode::kUnset;
  std::string statusDescription;
  std::string error;
};

class TestTracing;

class TestSpan final : public tracing::Span {
 public:
  TestSpan(const TestTracing& engine, std::string name,
           std::initializer_list<tracing::Attribute> attrs)
      : engine_(engine), name_(std::move(name)), attrs_(attrs) {}

  // Defined below, once TestTracing is a complete type.
  void end() override;

  void setAttributes(std::initializer_list<tracing::Attribute> attrs) override {
    attrs_.insert(attrs_.end(), attrs.begin(), attrs.end());
  }

  void recordError(std::string_view message) override {
    error_ = std::string(message);
  }

  void setStatus(tracing::StatusCode code,
                 std::string_view description) override {
    status_ = code;
    desc_ = std::string(description);
  }

  void addEvent(std::string_view name,
                std::initializer_list<tracing::Attribute> attrs) override {
    events_.push_back(RecordedEvent{std::string(name),
                                    std::vector<tracing::Attribute>(attrs)});
  }

  [[nodiscard]] tracing::SpanContext spanContext() const override { return {}; }

 private:
  const TestTracing& engine_;
  std::string name_;
  std::vector<tracing::Attribute> attrs_;
  std::vector<RecordedEvent> events_;
  tracing::StatusCode status_ = tracing::StatusCode::kUnset;
  std::string desc_;
  std::string error_;
  bool ended_ = false;
};

class TestTracer final : public tracing::Tracer {
 public:
  explicit TestTracer(const TestTracing& engine) : engine_(engine) {}

  std::shared_ptr<tracing::Span> start(
      std::string_view spanName,
      std::initializer_list<tracing::Attribute> attrs) const override {
    return std::make_shared<TestSpan>(engine_, std::string(spanName), attrs);
  }

  // No real span nesting is tracked by this double — every span is a root.
  [[nodiscard]] tracing::SpanContext currentSpanContext() const override {
    return {};
  }

  std::shared_ptr<tracing::Span> startChildOf(
      std::string_view spanName, const tracing::SpanContext& /*parent*/,
      std::initializer_list<tracing::Attribute> attrs) const override {
    return start(spanName, attrs);
  }

  std::shared_ptr<tracing::Span> startDetachedChildOf(
      std::string_view spanName, const tracing::SpanContext& parent,
      std::initializer_list<tracing::Attribute> attrs) const override {
    return startChildOf(spanName, parent, attrs);
  }

 private:
  const TestTracing& engine_;
};

// engine::Mutex (not std::mutex): everything that would exercise this
// double only runs inside a userver coroutine.
class TestTracing final : public tracing::Tracing {
 public:
  std::shared_ptr<tracing::Tracer> tracer(
      std::string_view /*name*/) const override {
    return std::make_shared<TestTracer>(*this);
  }

  // Snapshot of all completed (ended) spans, in end() order.
  [[nodiscard]] std::vector<RecordedSpan> spans() const {
    std::lock_guard<std::mutex> lock(mu_);
    return spans_;
  }

  void reset() {
    std::lock_guard<std::mutex> lock(mu_);
    spans_.clear();
  }

 private:
  friend class TestSpan;

  void record(RecordedSpan span) const {
    std::lock_guard<std::mutex> lock(mu_);
    spans_.push_back(std::move(span));
  }

  mutable std::mutex mu_;
  mutable std::vector<RecordedSpan> spans_;
};

inline void TestSpan::end() {
  if (ended_) {
    return;
  }
  ended_ = true;
  engine_.record(RecordedSpan{name_, attrs_, events_, status_, desc_, error_});
}

}  // namespace servicelib::testtracing

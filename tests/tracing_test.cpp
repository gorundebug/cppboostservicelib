#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>


#include <gtest/gtest.h>

#include <servicelib/runtime/context.hpp>
#include <servicelib/runtime/environment/tracing/tracing.hpp>

namespace {

class RecordingSpan final : public servicelib::tracing::Span {
 public:
  explicit RecordingSpan(servicelib::tracing::SpanContext context)
      : context_(std::move(context)) {}

  void end() override { ++endCount; }

  void setAttributes(
      std::initializer_list<servicelib::tracing::Attribute> attrs) override {
    attributes.insert(attributes.end(), attrs.begin(), attrs.end());
  }

  void recordError(std::string_view message) override {
    error = std::string(message);
  }

  void setStatus(servicelib::tracing::StatusCode code,
                 std::string_view description) override {
    status = code;
    statusDescription = std::string(description);
  }

  void addEvent(
      std::string_view name,
      std::initializer_list<servicelib::tracing::Attribute> attrs) override {
    eventName = std::string(name);
    eventAttributes.assign(attrs.begin(), attrs.end());
  }

  [[nodiscard]] servicelib::tracing::SpanContext spanContext() const override {
    return context_;
  }

  int endCount{};
  std::vector<servicelib::tracing::Attribute> attributes;
  std::vector<servicelib::tracing::Attribute> eventAttributes;
  servicelib::tracing::StatusCode status{
      servicelib::tracing::StatusCode::kUnset};
  std::string statusDescription;
  std::string error;
  std::string eventName;

 private:
  servicelib::tracing::SpanContext context_;
};

class RecordingTracer final : public servicelib::tracing::Tracer {
 public:
  std::shared_ptr<servicelib::tracing::Span> start(
      std::string_view spanName,
      std::initializer_list<servicelib::tracing::Attribute> attrs) const override {
    ++rootStarts;
    name = std::string(spanName);
    startedAttributes.assign(attrs.begin(), attrs.end());
    lastSpan = std::make_shared<RecordingSpan>(
        servicelib::tracing::SpanContext{"trace-1", "span-1", true, {}, {}});
    return lastSpan;
  }

  [[nodiscard]] servicelib::tracing::SpanContext currentSpanContext()
      const override {
    return {};
  }

  std::shared_ptr<servicelib::tracing::Span> startChildOf(
      std::string_view spanName, const servicelib::tracing::SpanContext& parent,
      std::initializer_list<servicelib::tracing::Attribute> attrs) const override {
    ++childStarts;
    parentContext = parent;
    name = std::string(spanName);
    startedAttributes.assign(attrs.begin(), attrs.end());
    lastSpan = std::make_shared<RecordingSpan>(
        servicelib::tracing::SpanContext{parent.traceId, "span-child", true,
                                        {}, {}});
    return lastSpan;
  }

  std::shared_ptr<servicelib::tracing::Span> startDetachedChildOf(
      std::string_view spanName, const servicelib::tracing::SpanContext& parent,
      std::initializer_list<servicelib::tracing::Attribute> attrs) const override {
    ++detachedStarts;
    parentContext = parent;
    name = std::string(spanName);
    startedAttributes.assign(attrs.begin(), attrs.end());
    lastSpan = std::make_shared<RecordingSpan>(servicelib::tracing::SpanContext{
        parent.traceId, "span-detached", true, {}, {}});
    return lastSpan;
  }

  mutable int rootStarts{};
  mutable int childStarts{};
  mutable int detachedStarts{};
  mutable std::string name;
  mutable servicelib::tracing::SpanContext parentContext;
  mutable std::vector<servicelib::tracing::Attribute> startedAttributes;
  mutable std::shared_ptr<RecordingSpan> lastSpan;
};

}  // namespace

TEST(Tracing, SamplingIsExplicitAndPreservedByMessageContextClones) {
  const servicelib::MessageContext original;
  EXPECT_FALSE(servicelib::tracing::SamplingEnabled(original));

  const auto sampled = servicelib::tracing::EnableSampling(original)
                           .withStreamId("message-1")
                           .withPriority(7);
  EXPECT_TRUE(servicelib::tracing::SamplingEnabled(sampled));
  EXPECT_EQ(sampled.streamId(), "message-1");
  EXPECT_EQ(sampled.priority(), 7);
  EXPECT_FALSE(servicelib::tracing::SamplingEnabled(original));
}

TEST(Tracing, RvalueContextUpdatesPreserveSharedCopies) {
  auto context = servicelib::MessageContext{}.withStreamId("shared-stream");
  const auto shared = context;

  const auto updated = std::move(context).withPriority(11).withSampling(true);

  EXPECT_EQ(updated.streamId(), "shared-stream");
  EXPECT_EQ(updated.priority(), 11);
  EXPECT_TRUE(updated.samplingEnabled());
  EXPECT_EQ(shared.streamId(), "shared-stream");
  EXPECT_FALSE(shared.hasPriority());
  EXPECT_FALSE(shared.samplingEnabled());
}

TEST(Tracing, StartSpanIsNoopWithoutSamplingOrTracer) {
  RecordingTracer tracer;

  {
    auto started = servicelib::tracing::StartSpan(servicelib::MessageContext{},
                                                  &tracer, "disabled");
    EXPECT_FALSE(started.context().trace().isValid());
    EXPECT_EQ(started.span(), nullptr);
  }
  {
    auto started = servicelib::tracing::StartSpan(
        servicelib::tracing::EnableSampling(servicelib::MessageContext{}),
        nullptr, "missing-tracer");
    EXPECT_FALSE(started.context().trace().isValid());
    EXPECT_EQ(started.span(), nullptr);
  }

  EXPECT_EQ(tracer.rootStarts, 0);
  EXPECT_EQ(tracer.childStarts, 0);
}

TEST(Tracing, StartSpanInPlaceHasEmptyDisabledFastPath) {
  RecordingTracer tracer;
  auto context = servicelib::MessageContext{}.withStreamId("message-1");

  {
    auto active = servicelib::tracing::StartSpanInPlace(
        context, &tracer, "disabled");
    EXPECT_EQ(active.span(), nullptr);
    EXPECT_EQ(context.streamId(), "message-1");
    EXPECT_FALSE(context.trace().isValid());
  }

  EXPECT_EQ(tracer.rootStarts, 0);
  EXPECT_EQ(tracer.childStarts, 0);
}

TEST(Tracing, StartSpanInPlacePublishesContextAndEndsExactlyOnce) {
  RecordingTracer tracer;
  auto context = servicelib::tracing::EnableSampling(
      servicelib::MessageContext{}.withStreamId("message-1"));
  std::shared_ptr<RecordingSpan> span;

  {
    auto active = servicelib::tracing::StartSpanInPlace(
        context, &tracer, "stream.call");
    span = tracer.lastSpan;
    EXPECT_EQ(active.span(), span.get());
    EXPECT_EQ(context.streamId(), "message-1");
    EXPECT_EQ(context.trace().traceId, "trace-1");
    EXPECT_EQ(context.trace().spanId, "span-1");
    EXPECT_EQ(span->endCount, 0);
  }

  ASSERT_TRUE(span);
  EXPECT_EQ(span->endCount, 1);
}

TEST(Tracing, RootSpanPublishesContextAndEndsExactlyOnce) {
  RecordingTracer tracer;
  std::shared_ptr<RecordingSpan> span;

  {
    auto started = servicelib::tracing::StartSpan(
        servicelib::tracing::EnableSampling(servicelib::MessageContext{}),
        &tracer, "stream.call",
        {servicelib::tracing::Attribute::String("from", "input")});
    span = tracer.lastSpan;

    EXPECT_EQ(tracer.rootStarts, 1);
    EXPECT_EQ(tracer.childStarts, 0);
    EXPECT_EQ(tracer.name, "stream.call");
    EXPECT_EQ(tracer.startedAttributes.size(), 1);
    EXPECT_EQ(started.context().trace().traceId, "trace-1");
    EXPECT_EQ(started.context().trace().spanId, "span-1");
    EXPECT_TRUE(started.context().trace().isValid());
    EXPECT_EQ(span->endCount, 0);
  }

  ASSERT_TRUE(span);
  EXPECT_EQ(span->endCount, 1);
}

TEST(Tracing, ExistingContextStartsChildAndHelpersAnnotateSpan) {
  RecordingTracer tracer;
  const auto context =
      servicelib::tracing::EnableSampling(servicelib::MessageContext{})
          .withTrace({"trace-parent", "span-parent", true, "vendor=value",
                      "tenant=acme"});

  std::shared_ptr<RecordingSpan> span;
  {
    auto started =
        servicelib::tracing::StartSpan(context, &tracer, "stream.call");
    span = tracer.lastSpan;

    EXPECT_EQ(tracer.rootStarts, 0);
    EXPECT_EQ(tracer.childStarts, 1);
    EXPECT_EQ(tracer.parentContext.traceId, "trace-parent");
    EXPECT_EQ(tracer.parentContext.spanId, "span-parent");
    EXPECT_EQ(started.context().trace().traceId, "trace-parent");
    EXPECT_EQ(started.context().trace().spanId, "span-child");
    EXPECT_EQ(started.context().trace().traceState, "vendor=value");
    EXPECT_EQ(started.context().trace().baggage, "tenant=acme");

    servicelib::tracing::SpanAttrs(
        started.span(),
        {servicelib::tracing::Attribute::Bool("expedited", true)});
    servicelib::tracing::SpanEvent(
        started.span(), "accepted",
        {servicelib::tracing::Attribute::Int64("priority", 3)});
    servicelib::tracing::SpanError(started.span(), "failed");
  }

  ASSERT_TRUE(span);
  EXPECT_EQ(span->attributes.size(), 1);
  EXPECT_EQ(span->eventName, "accepted");
  EXPECT_EQ(span->eventAttributes.size(), 1);
  EXPECT_EQ(span->error, "failed");
  EXPECT_EQ(span->status, servicelib::tracing::StatusCode::kError);
  EXPECT_EQ(span->statusDescription, "failed");
  EXPECT_EQ(span->endCount, 1);
}

TEST(Tracing, DetachedChildPreservesExplicitParentAndCanEndLater) {
  RecordingTracer tracer;
  const servicelib::tracing::SpanContext parent{
      "trace-parent", "span-parent", true, {}, {}};

  auto span = tracer.startDetachedChildOf(
      "stream.delay", parent,
      {servicelib::tracing::Attribute::String("stream", "Soft Deadline")});

  ASSERT_TRUE(span);
  EXPECT_EQ(tracer.detachedStarts, 1);
  EXPECT_EQ(tracer.parentContext.traceId, "trace-parent");
  EXPECT_EQ(tracer.parentContext.spanId, "span-parent");
  EXPECT_EQ(tracer.name, "stream.delay");
  EXPECT_EQ(tracer.startedAttributes.size(), 1);
  EXPECT_EQ(span->spanContext().traceId, "trace-parent");
  EXPECT_EQ(span->spanContext().spanId, "span-detached");

  span->end();
  ASSERT_TRUE(tracer.lastSpan);
  EXPECT_EQ(tracer.lastSpan->endCount, 1);
}

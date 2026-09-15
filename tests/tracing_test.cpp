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
      servicelib::tracing::AttributeView attrs) override {
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
      servicelib::tracing::AttributeView attrs) override {
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
      servicelib::tracing::AttributeView attrs) const override {
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
      servicelib::tracing::AttributeView attrs) const override {
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
      servicelib::tracing::AttributeView attrs) const override {
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

#include <array>
#include <type_traits>
#include <servicelib/runtime/caller.hpp>

namespace {
class CachedAttributeTracer final : public servicelib::tracing::Tracer {
 public:
  std::shared_ptr<servicelib::tracing::Span> start(
      std::string_view, servicelib::tracing::AttributeView attrs) const override {
    ++starts;
    stable = stable && attrs.data() == expected.data() && attrs.size() == expected.size();
    return span_;
  }
  servicelib::tracing::SpanContext currentSpanContext() const override { return {}; }
  std::shared_ptr<servicelib::tracing::Span> startChildOf(
      std::string_view name, const servicelib::tracing::SpanContext&,
      servicelib::tracing::AttributeView attrs) const override { return start(name, attrs); }
  std::shared_ptr<servicelib::tracing::Span> startDetachedChildOf(
      std::string_view name, const servicelib::tracing::SpanContext&,
      servicelib::tracing::AttributeView attrs) const override { return start(name, attrs); }
  std::span<const servicelib::tracing::Attribute> expected;
  mutable std::size_t starts{};
  mutable bool stable{true};
 private:
  std::shared_ptr<servicelib::tracing::Span> span_ = std::make_shared<servicelib::tracing::NoopSpan>();
};
class CachedAttributeCaller final : public servicelib::CallerBase {
 public:
  CachedAttributeCaller(Params params, std::string_view type = {}, std::string_view pool = {})
      : CallerBase(std::move(params)) { configureCallAttributes(type, pool); }
  bool isAsync() const noexcept override { return false; }
  void fire(servicelib::MessageContext& context) {
    recordMessage();
    if (samplingEnabled(context)) {
      [[maybe_unused]] auto span = startCallSpan(context);
    }
  }
  std::span<const servicelib::tracing::Attribute> cached() const { return callAttributes_; }
};
}

TEST(Tracing, CachedCallAttributesReuseOwnedStorageAndKeepSamplingLive) {
  static_assert(std::is_trivially_copyable_v<servicelib::tracing::AttributeView>);
  const std::array<std::pair<std::string_view, std::string_view>, 4> variants{{
      {"", ""}, {"parallel", ""}, {"taskpool", "Inventory Workers"},
      {"prioritytaskpool", "Inventory Priority Workers"}}};
  for (const auto& [type, pool] : variants) {
    auto tracer = std::make_shared<CachedAttributeTracer>();
    servicelib::CallerBase::Params params;
    params.sourceName = "Request source with a long non-SSO name";
    params.consumerName = "Receiving stage with a long non-SSO name";
    params.pipeline = "customerPricingPipeline";
    params.component = "Customer Pricing Component";
    params.tracer = tracer;
    params.metricsEnabled = false;
    CachedAttributeCaller caller(std::move(params), type, pool);
    tracer->expected = caller.cached();
    ASSERT_EQ(caller.cached().size(), 4 + !type.empty() + !pool.empty());
    EXPECT_EQ(std::get<std::string>(caller.cached()[1].value()), "customerPricingPipeline");
    EXPECT_EQ(std::get<std::string>(caller.cached()[2].value()), "Customer Pricing Component");
    servicelib::MessageContext unsampled;
    caller.fire(unsampled);
    EXPECT_EQ(tracer->starts, 0);
    auto sampled = servicelib::tracing::EnableSampling(servicelib::MessageContext{});
    for (int i = 0; i < 100; ++i) caller.fire(sampled);
    EXPECT_EQ(tracer->starts, 100);
    EXPECT_TRUE(tracer->stable);
    caller.fire(unsampled);
    EXPECT_EQ(tracer->starts, 100);
    EXPECT_EQ(caller.statistics().count(), 102);
  }
}

TEST(Tracing, DisabledCallerDoesNotAllocateAnAttributeCache) {
  servicelib::CallerBase::Params params;
  params.metricsEnabled = false;
  CachedAttributeCaller caller(std::move(params), "taskpool", "Workers");
  EXPECT_TRUE(caller.cached().empty());
  auto context = servicelib::tracing::EnableSampling(servicelib::MessageContext{});
  for (int i = 0; i < 100; ++i) caller.fire(context);
  EXPECT_EQ(caller.statistics().count(), 100);
  EXPECT_TRUE(caller.cached().empty());
}

TEST(Tracing, BorrowedAttributeViewKeepsRecorderOwnershipAndBracedCalls) {
  using servicelib::tracing::Attribute;
  std::array<Attribute, 2> attributes{{Attribute::String("pipeline", "owned pipeline value"), Attribute::Bool("enabled", true)}};
  const servicelib::tracing::AttributeView view{std::span<const Attribute>{attributes}};
  RecordingTracer tracer;
  auto span = tracer.start("cached", view);
  EXPECT_EQ(view.data(), attributes.data());
  attributes[0] = Attribute::String("pipeline", "changed after synchronous start");
  ASSERT_EQ(tracer.startedAttributes.size(), 2);
  EXPECT_EQ(std::get<std::string>(tracer.startedAttributes[0].value()), "owned pipeline value");
  span->setAttributes({Attribute::Int64("count", 2)});
  span->end();
}

#pragma once

namespace {
struct PreparedGraphTypes {
  template <typename>
  struct DataType {};
};

class PreparedGraphEnvironment final
    : public servicelib::StreamExecutionEnvironment<
          PreparedGraphEnvironment, PreparedGraphTypes> {
 public:
  void prepare() { static_cast<void>(getExecutionRuntime<>()); }
};

template <typename Config>
Config preparedConfig(int id, const char* name) {
  Config config;
  config.id = id;
  config.name = name;
  return config;
}

struct PreparedRecord {
  std::vector<int>* values;
  void operator()(servicelib::MessageContext context, const int& value) const {
    EXPECT_EQ(context.streamId(), "typed-graph");
    values->push_back(value);
  }
};

struct PreparedMap {
  template <typename Out>
  void operator()(servicelib::MessageContext context, servicelib::StreamBase&,
                  const int& value, Out&& out) const {
    out.out(std::move(context), value * 2);
  }
};

template <typename Input, typename Output>
struct PreparedConvert {
  template <typename Out>
  void operator()(servicelib::MessageContext context, servicelib::StreamBase&,
                  const Input& value, Out&& out) const {
    out.out(std::move(context), static_cast<Output>(value));
  }
};

struct PreparedFilter {
  bool operator()(servicelib::MessageContext, servicelib::StreamBase&,
                  const int& value) const { return value > 2; }
};

struct PreparedCase {
  int offset;
  std::size_t operator()(servicelib::MessageContext, servicelib::StreamBase&,
                         const int& value) const {
    return static_cast<std::size_t>((value + offset) % 2);
  }
};

using PreparedStream = servicelib::Stream<
    int, servicelib::StreamConsumer<int>, PreparedGraphEnvironment>;
using PreparedSink = PreparedStream::SinkImpl<int, PreparedRecord>;
}  // namespace

TEST(Operators, PreparedTypedChainMatchesFluentGraphAndInheritsSerde) {
  std::vector<int> actual;
  {
    PreparedGraphEnvironment app;
    auto input = servicelib::makeInputStream<
        int, std::monostate, int, PreparedGraphEnvironment>(
        preparedConfig<servicelib::config::InputStreamConfig>(30100, "typed-input"),
        nullptr, app);
    using Filter = PreparedStream::FilterImpl<PreparedFilter, PreparedSink>;
    using Map = PreparedStream::MapImpl<int, PreparedMap, Filter>;
    // Runtime construction is forward for serde propagation; ownership is
    // connected backwards. There is no disposable prototype graph.
    auto map = app.makeStream<Map>(
        preparedConfig<servicelib::config::MapStreamConfig>(30101, "typed-map"),
        input->getSerde(), &app, servicelib::StreamFunction(PreparedMap{}));
    auto filter = app.makeStream<Filter>(
        preparedConfig<servicelib::config::FilterStreamConfig>(30102, "typed-filter"),
        map->getSerde(), &app, servicelib::StreamFunction(PreparedFilter{}));
    auto sink = app.makeStream<PreparedSink>(
        preparedConfig<servicelib::config::SinkStreamConfig>(30103, "typed-sink"),
        filter->getSerde(), &app, servicelib::StreamFunction(PreparedRecord{&actual}));
    EXPECT_NE(map->getSerde(), input->getSerde());
    EXPECT_EQ(filter->getSerde(), map->getSerde());
    EXPECT_EQ(sink->getSerde(), filter->getSerde());
    auto* mapPointer = map.get();
    auto* filterPointer = filter.get();
    auto* sinkPointer = sink.get();
    filter->connect(std::move(sink));
    map->connect(std::move(filter));
    input->connect(std::move(map));
    // These succeed only if the registered operator edges retain their types.
    auto mapEdge = app.prepareTypedCaller<int>(*mapPointer, *filterPointer);
    auto filterEdge = app.prepareTypedCaller<int>(*filterPointer, *sinkPointer);
    app.prepare();
    for (int value : {1, 2, 3}) {
      input->consume(servicelib::MessageContext{}.withStreamId("typed-graph"),
                     servicelib::Payload<int>::make(value));
    }
    EXPECT_EQ(mapEdge.statistics().count(), 3);
    EXPECT_EQ(filterEdge.statistics().count(), 2);
  }
  std::vector<int> expected;
  {
    PreparedGraphEnvironment app;
    auto input = servicelib::makeInputStream<
        int, std::monostate, int, PreparedGraphEnvironment>(
        preparedConfig<servicelib::config::InputStreamConfig>(30200, "fluent-input"),
        nullptr, app);
    auto& map = input->map(
        preparedConfig<servicelib::config::MapStreamConfig>(30201, "fluent-map"),
        servicelib::StreamType<int>{}, servicelib::StreamFunction(PreparedMap{}));
    auto& filter = map.filter(
        preparedConfig<servicelib::config::FilterStreamConfig>(30202, "fluent-filter"),
        servicelib::StreamFunction(PreparedFilter{}));
    filter.sink(
        preparedConfig<servicelib::config::SinkStreamConfig>(30203, "fluent-sink"),
        servicelib::StreamType<int>{}, servicelib::StreamFunction(PreparedRecord{&expected}));
    app.prepare();
    for (int value : {1, 2, 3}) {
      input->consume(servicelib::MessageContext{}.withStreamId("typed-graph"),
                     servicelib::Payload<int>::make(value));
    }
  }
  EXPECT_EQ(actual, expected);
  EXPECT_EQ(actual, (std::vector<int>{4, 6}));
}

TEST(Operators, PreparedTypedTypeChangingMapPreservesOutputTopology) {
  std::vector<int> values;
  PreparedGraphEnvironment app;
  auto input = servicelib::makeInputStream<
      int, std::monostate, int, PreparedGraphEnvironment>(
      preparedConfig<servicelib::config::InputStreamConfig>(30500, "convert-input"),
      nullptr, app);
  using LongStream = servicelib::Stream<
      long, servicelib::StreamConsumer<long>, PreparedGraphEnvironment>;
  using Narrow = LongStream::MapImpl<int, PreparedConvert<long, int>, PreparedSink>;
  using Widen = PreparedStream::MapImpl<long, PreparedConvert<int, long>, Narrow>;
  static_assert(std::is_same_v<typename Widen::topology_value_type, long>);
  static_assert(std::is_same_v<typename Narrow::topology_value_type, int>);
  auto widen = app.makeStream<Widen>(
      preparedConfig<servicelib::config::MapStreamConfig>(30501, "widen"),
      input->getSerde(), &app,
      servicelib::StreamFunction((PreparedConvert<int, long>{})));
  auto narrow = app.makeStream<Narrow>(
      preparedConfig<servicelib::config::MapStreamConfig>(30502, "narrow"),
      static_cast<servicelib::StreamConsumer<long>&>(*widen).getSerde(), &app,
      servicelib::StreamFunction((PreparedConvert<long, int>{})));
  auto sink = app.makeStream<PreparedSink>(
      preparedConfig<servicelib::config::SinkStreamConfig>(30503, "convert-sink"),
      static_cast<servicelib::StreamConsumer<int>&>(*narrow).getSerde(), &app,
      servicelib::StreamFunction(PreparedRecord{&values}));
  servicelib::StatusTopologyPrinter topology;
  EXPECT_EQ(topology.makeNode(*widen).typeName,
            topology.makeNode(static_cast<servicelib::StreamConsumer<long>&>(*widen)).typeName);
  EXPECT_EQ(topology.makeNode(*narrow).typeName,
            topology.makeNode(static_cast<servicelib::StreamConsumer<int>&>(*narrow)).typeName);
  EXPECT_NE(topology.makeNode(*widen).typeName, topology.makeNode(*narrow).typeName);
  narrow->connect(std::move(sink));
  widen->connect(std::move(narrow));
  input->connect(std::move(widen));
  app.prepare();
  input->consume(servicelib::MessageContext{}.withStreamId("typed-graph"),
                 servicelib::Payload<int>::make(42));
  EXPECT_EQ(values, (std::vector<int>{42}));
}

TEST(Operators, PreparedTypedSplitMergeSharesOneMergeAndSink) {
  std::vector<int> values;
  PreparedGraphEnvironment app;
  auto input = servicelib::makeInputStream<
      int, std::monostate, int, PreparedGraphEnvironment>(
      preparedConfig<servicelib::config::InputStreamConfig>(30300, "shared-input"),
      nullptr, app);
  using Merge = PreparedStream::MergeImpl<2, PreparedSink>;
  using Owner = PreparedStream::MergeLinkImpl<2, true, Merge>;
  using Reference = PreparedStream::MergeLinkImpl<2, false, Merge>;
  using Left = PreparedStream::SplitLinkImpl<int, Owner, PreparedGraphEnvironment>;
  using Right = PreparedStream::SplitLinkImpl<int, Reference, PreparedGraphEnvironment>;
  using Split = PreparedStream::SplitImpl<std::tuple<Left&, Right&>>;
  auto split = app.makeStream<Split>(
      preparedConfig<servicelib::config::SplitStreamConfig>(30301, "shared-split"),
      input->getSerde(), &app, app.makeStream<Left>(), app.makeStream<Right>());
  auto merge = app.makeStream<Merge>(
      preparedConfig<servicelib::config::MergeStreamConfig>(30302, "shared-merge"),
      split->getSerde(), &app);
  EXPECT_EQ(merge->getSerde(), input->getSerde());
  auto sink = app.makeStream<PreparedSink>(
      preparedConfig<servicelib::config::SinkStreamConfig>(30303, "shared-sink"),
      merge->getSerde(), &app, servicelib::StreamFunction(PreparedRecord{&values}));
  auto* splitPointer = split.get();
  auto* mergePointer = merge.get();
  auto* sinkPointer = sink.get();
  merge->connect(std::move(sink));
  auto owner = app.makeStream<Owner>(std::move(merge));
  auto reference = app.makeStream<Reference>(*mergePointer);
  split->get<0>().connect(std::move(owner));
  split->get<1>().connect(std::move(reference));
  input->connect(std::move(split));
  app.prepare();
  input->consume(servicelib::MessageContext{}.withStreamId("typed-graph"),
                 servicelib::Payload<int>::make(7));
  EXPECT_EQ(values, (std::vector<int>{7, 7}));
  auto incoming = app.prepareTypedCaller<int>(*splitPointer, *mergePointer);
  auto* erasedIncoming = app.prepareCaller<int>(*splitPointer, *mergePointer);
  EXPECT_EQ(incoming.statistics().count(), 2);
  EXPECT_EQ(&incoming.statistics(), &erasedIncoming->statistics());
  auto output = app.prepareTypedCaller<int>(*mergePointer, *sinkPointer);
  EXPECT_EQ(output.statistics().count(), 2);
}

TEST(Operators, PreparedTypedCaseKeepsFunctionStateAndSelectsOneBranch) {
  std::vector<int> leftValues;
  std::vector<int> rightValues;
  PreparedGraphEnvironment app;
  auto input = servicelib::makeInputStream<
      int, std::monostate, int, PreparedGraphEnvironment>(
      preparedConfig<servicelib::config::InputStreamConfig>(30400, "case-input"),
      nullptr, app);
  using Branch = PreparedStream::WhenLinkImpl<PreparedSink, PreparedGraphEnvironment>;
  using Case = PreparedStream::CaseImpl<std::tuple<Branch&, Branch&>, PreparedCase>;
  auto choice = app.makeStream<Case>(
      preparedConfig<servicelib::config::CaseStreamConfig>(30401, "typed-case"),
      input->getSerde(), &app, servicelib::StreamFunction(PreparedCase{1}),
      app.makeStream<Branch>(), app.makeStream<Branch>());
  auto& left = choice->get<0>();
  auto& right = choice->get<1>();
  left.configure(preparedConfig<servicelib::config::WhenStreamConfig>(30402, "left"),
                 nullptr, &app);
  right.configure(preparedConfig<servicelib::config::WhenStreamConfig>(30403, "right"),
                  nullptr, &app);
  EXPECT_NE(left.getSerde(), input->getSerde());
  EXPECT_NE(left.getSerde(), right.getSerde());
  left.connect(app.makeStream<PreparedSink>(
      preparedConfig<servicelib::config::SinkStreamConfig>(30404, "left-sink"),
      left.getSerde(), &app, servicelib::StreamFunction(PreparedRecord{&leftValues})));
  right.connect(app.makeStream<PreparedSink>(
      preparedConfig<servicelib::config::SinkStreamConfig>(30405, "right-sink"),
      right.getSerde(), &app, servicelib::StreamFunction(PreparedRecord{&rightValues})));
  input->connect(std::move(choice));
  app.prepare();
  for (int value : {0, 1, 2, 3}) {
    input->consume(servicelib::MessageContext{}.withStreamId("typed-graph"),
                   servicelib::Payload<int>::make(value));
  }
  EXPECT_EQ(leftValues, (std::vector<int>{1, 3}));
  EXPECT_EQ(rightValues, (std::vector<int>{0, 2}));
}

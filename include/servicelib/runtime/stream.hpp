/*
 * streams.hpp
 * C++ streams API
 *
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the
 * [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */
#pragma once

#include <functional>

#include <servicelib/runtime/config/stream_types.hpp>
#include <servicelib/runtime/datasink.hpp>
#include <servicelib/runtime/detail/storage.hpp>
#include <servicelib/runtime/serde/serde.hpp>
#include <servicelib/runtime/store/hashmap.hpp>
#include <servicelib/runtime/stream_tracing.hpp>
#include <servicelib/runtime/topology.hpp>

namespace servicelib {

template <typename T, typename Context>
class CycleLinkStream;

template <typename _Tp, typename _Cp, typename _Context>
class Stream : public StreamBase, public virtual StreamConsumer<_Tp> {
  template <typename, typename>
  friend class StreamExecutionEnvironment;
  template <typename, typename, typename>
  friend class Stream;
  template <typename, typename, typename, typename>
  friend class InputStream;
  template <typename, typename>
  friend class CycleLinkStream;
  template <typename>
  friend class ErrorStream;
  friend class StreamVerifyContext;
  using ValueType = _Tp;
  using ConsumerType = _Cp;

  template <typename _T>
  using unique_ptr = StreamBase::unique_ptr<_T>;

#include <servicelib/operators/streamlink.inl>

#include <servicelib/operators/case.inl>
#include <servicelib/operators/delay.inl>
#include <servicelib/operators/error.inl>
#include <servicelib/operators/filter.inl>
#include <servicelib/operators/flatmap.inl>
#include <servicelib/operators/flatmapiterable.inl>
#include <servicelib/operators/join.inl>
#include <servicelib/operators/join_common.inl>
#include <servicelib/operators/keyby.inl>
#include <servicelib/operators/link.inl>
#include <servicelib/operators/map.inl>
#include <servicelib/operators/merge.inl>
#include <servicelib/operators/multijoin.inl>
#include <servicelib/operators/process.inl>
#include <servicelib/operators/sink.inl>
#include <servicelib/operators/split.inl>

  template <typename T = _Tp>
  struct is_base_stream_consumer : std::false_type {};

  template <typename T>
  struct is_base_stream_consumer<unique_ptr<StreamConsumer<T>>>
      : std::true_type {};

 private:
  unique_ptr<_Cp> consumer_;
  std::function<void(MessageContext, Payload<_Tp>)> preparedCaller_;
  decltype(_Context::getExecutionEnvironment())& context_{
      _Context::getExecutionEnvironment()};

 public:
  void consume(MessageContext ctx, Payload<_Tp> payload) override {
    if (consumer_) {
      consumer_->consume(std::move(ctx), std::move(payload));
    }
  }

  size_t getId() const noexcept override { return StreamBase::getId(); }

  size_t getConfigId() const noexcept override {
    if (StreamBase::getConfigId() != 0) {
      return StreamBase::getConfigId();
    }
    return consumer_ ? consumer_->getBase().getConfigId() : 0;
  }

  const std::string& getName() const noexcept override {
    return StreamBase::getName();
  }

 protected:
  Stream() noexcept = default;
  explicit Stream(unique_ptr<_Cp> stream) noexcept
      : consumer_(std::move(stream)) {}

  ~Stream() override = default;

  auto& consumer() const noexcept { return consumer_; }
  auto& consumer() noexcept { return consumer_; }

  auto& context() const noexcept { return context_; }
  auto& context() noexcept { return context_; }

  bool hasPreparedCaller() const noexcept {
    return static_cast<bool>(preparedCaller_);
  }

  void dispatchPrepared(MessageContext context, Payload<_Tp> payload) {
    preparedCaller_(std::move(context), std::move(payload));
  }

  // Topology construction is single-threaded. Resolve the edge once and keep
  // a direct dispatcher on the producer; request processing must never look
  // the Caller up in Environment's registry.
  void prepareConsumerCaller() {
    if (!consumer_) return;
    if constexpr (requires {
                    context_.template prepareCaller<_Tp>(*this, *consumer_);
                  }) {
      auto* caller =
          context_.template prepareCaller<_Tp>(*this, *consumer_);
      preparedCaller_ =
          [caller](MessageContext context, Payload<_Tp> payload) {
            caller->consume(std::move(context), std::move(payload));
          };
    }
  }

  [[nodiscard]] const StreamBase& getConsumer() const override {
    if (consumer_) {
      return consumer_->getBase();
    }
    throw StreamException("Split branch must not have an empty consumer.");
  }

  StreamBase& getConsumer() override {
    if (consumer_) {
      return consumer_->getBase();
    }
    throw StreamException("Split branch must not have an empty consumer.");
  }

  bool hasConsumer() const noexcept override {
    return consumer_.operator bool();
  }

  const StreamBase& getBase() const noexcept override { return *this; }

  StreamBase& getBase() noexcept override { return *this; }

  std::string getCode() const override { return std::string(); }

  const std::string_view& getType() const override {
    return StreamBuilderContext::getType<decltype(*this)>();
  }

  size_t buildTopologyCommon(StreamBuilderContext& ctx, size_t id,
                             StreamBuilderContext::TIdsList* splitConsumerIds,
                             bool skip) {
    if (this->getId() == 0) {
      this->setId(id);
    }
    auto& os = ctx.getOut();

    if (consumer_ && consumer_->getId() != 0) {
      os << "[[maybe_unused]] ";
    }
    os << "auto &stream" << id << "t = static_cast<" << getType() << "&>(stream"
       << id << ");" << std::endl;
    if (consumer_) {
      size_t cid = consumer_->getId();
      size_t nextid = id;
      if (cid == 0) {
        os << "auto &stream" << (id + 1) << " = stream" << id
           << "t.getConsumer();" << std::endl;
        nextid = consumer_->buildTopology(ctx, id + 1, splitConsumerIds, false);
        cid = id + 1;
      } else if (splitConsumerIds != nullptr) {
        splitConsumerIds->emplace_back(cid);
      }
      if (!skip) {
        os << "auto stream" << id << "c = stream" << id << "t.build(";
        os << "stream" << id << "t";
        os << ", std::move(stream" << cid << "c)";
        std::string code = this->getCode();
        if (code.length() > 0) {
          os << ", " << code;
        }
        os << ");" << std::endl;
        os << "auto &stream" << id << "r = *stream" << id << "c;" << std::endl;
        os << "stream" << id << "r.setId(" << id << ");" << std::endl;
      }
      // Call semantics and their telemetry are fixed by the topology.  Build
      // the caller here, while a userver component is still being
      // constructed; userver intentionally forbids registering new metric
      // writers later from the request path.
      prepareConsumerCaller();
      id = nextid;
    } else {
      os << "auto stream" << id << "c = stream" << id << "t.build(";
      os << "stream" << id << "t";
      std::string code = this->getCode();
      if (code.length() > 0) {
        os << ", " << code;
      }
      os << ");" << std::endl;
      os << "auto &stream" << id << "r = *stream" << id << "c;" << std::endl;
      os << "stream" << id << "r.setId(" << id << ");" << std::endl;
    }
    return id;
  }

  void verifyTopology(StreamVerifyContext& ctx) const override {
    ctx.verify(*this);
    if (consumer_) {
      consumer_->verifyTopology(ctx);
    }
  }

  void printTopology(TopologyPrinter& tp,
                     std::unordered_set<size_t>& visited) const override {
    if (visited.find(getId()) == visited.end()) {
      visited.emplace(getId());
      if (hasConsumer()) {
        tp.printLink(tp.makeNode(*this), tp.makeNode(*consumer_));
        consumer_->printTopology(tp, visited);
      }
    }
  }

 public:
  // Go: MakeCaseStream — routes each value to exactly one of N branches via
  // SwitchFunction: (MessageContext, StreamBase&, _Tp&) -> size_t.
  template <size_t N = 2, typename SwitchFunction, typename _FCtx>
  auto case_(const servicelib::config::CaseStreamConfig& cfg,
             StreamFunction<SwitchFunction, _FCtx> f)
      -> std::enable_if_t<
          N >= 2 && std::is_convertible_v<
                        std::invoke_result_t<SwitchFunction, MessageContext,
                                             StreamBase&, _Tp&>,
                        size_t>,
          CaseBase<detail::tuple_of<
              N, WhenLinkImpl<StreamConsumer<_Tp>, _Context>&>>>& {
    return setConsumer(
        CaseImpl<
            detail::tuple_of<N, WhenLinkImpl<StreamConsumer<_Tp>, _Context>&>,
            SwitchFunction>::make(cfg, this->serde_, this->env_, std::move(f)));
  }

  // Go: MakeFilterStream — config id + upstream serde propagated, no explicit
  // serde param
  template <typename _FilterFunction, typename _FCtx>
  auto filter(const servicelib::config::FilterStreamConfig& cfg,
              StreamFunction<_FilterFunction, _FCtx> f)
      -> std::enable_if_t<
          std::is_same_v<std::invoke_result_t<_FilterFunction, MessageContext,
                                              StreamBase&, _Tp&>,
                         bool>,
          Filter<>&> {
    return setConsumer(FilterImpl<_FilterFunction>::make(
        cfg, this->serde_, this->env_, std::move(f)));
  }

  // Go: MakeMapStream — f emits 0, 1, or many R values via Collector
  template <typename R, typename MapFunction, typename _FCtx>
  auto map(const servicelib::config::MapStreamConfig& cfg, const StreamType<R>&,
           StreamFunction<MapFunction, _FCtx> f)
      -> std::enable_if_t<
          std::is_same_v<std::invoke_result_t<MapFunction, MessageContext,
                                              StreamBase&, _Tp&,
                                              Collector<R, void>&&>,
                         void>,
          Map<R>&> {
    return setConsumer(MapImpl<R, MapFunction>::make(cfg, this->serde_,
                                                     this->env_, std::move(f)));
  }

  // Go: MakeKeyByStream — f emits KeyValue<K,V> pairs via Collector; K and V
  // are explicit
  template <typename K, typename V, typename KeyByFunction, typename _FCtx>
  auto keyBy(const servicelib::config::KeyByStreamConfig& cfg,
             StreamFunction<KeyByFunction, _FCtx> f)
      -> std::enable_if_t<
          std::is_same_v<
              std::invoke_result_t<KeyByFunction, MessageContext, StreamBase&,
                                   _Tp&,
                                   Collector<KeyValueType<K, V>, void>&&>,
              void>,
          KeyBy<K, V>&> {
    return setConsumer(KeyByImpl<K, V, KeyByFunction>::make(
        cfg, this->serde_, this->env_, std::move(f)));
  }

  // Go: MakeFlatMapStream
  template <typename T, typename FlatMapFunction, typename _FCtx>
  auto flatMap(const servicelib::config::FlatMapStreamConfig& cfg,
               const StreamType<T>&, StreamFunction<FlatMapFunction, _FCtx> f)
      -> std::enable_if_t<
          std::is_same_v<std::invoke_result_t<FlatMapFunction, MessageContext,
                                              StreamBase&, _Tp&,
                                              Collector<T, void>&&>,
                         void>,
          FlatMap<T, StreamConsumer<T>>&> {
    return setConsumer(FlatMapImpl<T, FlatMapFunction, StreamConsumer<T>>::make(
        cfg, this->serde_, this->env_, std::move(f)));
  }

  // Go: MakeFlatMapIterableStream
  template <typename T = _Tp>
  auto flatMapIterate(const servicelib::config::FlatMapIterableStreamConfig& cfg)
      -> Stream<typename flatmap_iterable_type<T>::type,
                StreamConsumer<typename flatmap_iterable_type<T>::type>,
                _Context>& {
    return setConsumer(
        FlatMapIterableImpl<StreamConsumer<
            typename flatmap_iterable_type<T>::type>>::make(cfg, this->serde_,
                                                            this->env_));
  }

  // Go: MakeSplitStream
  template <size_t N = 2>
  auto split(const servicelib::config::SplitStreamConfig& cfg) -> std::enable_if_t<
      N >= 2, SplitBase<detail::tuple_of<
                  N, SplitLinkImpl<_Tp, StreamConsumer<_Tp>, _Context>&>>>& {
    return setConsumer(
        SplitImpl<detail::tuple_of<
            N, SplitLinkImpl<_Tp, StreamConsumer<_Tp>, _Context>&>>::
            make(cfg, this->serde_, this->env_));
  }

  // Go: MakeDelayStream
  template <typename DelayFunction, typename _FCtx>
  auto delay(const servicelib::config::DelayStreamConfig& cfg,
             StreamFunction<DelayFunction, _FCtx> f) -> Delay<>& {
    return setConsumer(DelayImpl<DelayFunction>::make(
        cfg, this->serde_, this->env_, std::move(f)));
  }

  // Go: MakeProcessStream — T→R with error output E
  template <typename _R, typename _E, typename ProcessFunction, typename _FCtx>
  auto process(const servicelib::config::ProcessStreamConfig& cfg,
               const StreamType<_R>&, const StreamType<_E>&,
               StreamFunction<ProcessFunction, _FCtx> f) -> Process<_R, _E>& {
    return setConsumer(ProcessImpl<_R, _E, ProcessFunction>::make(
        cfg, this->serde_, this->env_, std::move(f)));
  }

  // Go: MakeSinkStream. A sink is terminal: it consumes the value and has
  // no downstream stream. Codegen normally binds this callback to a data
  // sink endpoint's consume() method. E is the sink's error output type
  // (Go: SinkStream[T,E].errorConsumer).
  template <typename E, typename SinkFunction, typename _FCtx>
  auto sink(const servicelib::config::SinkStreamConfig& cfg,
            const StreamType<E>&, StreamFunction<SinkFunction, _FCtx> f)
      -> Sink<E, SinkFunction>& {
    return setConsumer(SinkImpl<E, SinkFunction>::make(cfg, this->serde_,
                                                       this->env_, std::move(f)));
  }

  // Go: MakeSinkStreamWithResult. Input T is consumed by the endpoint-bound
  // callback; endpoint results R re-enter the graph through consumeResult().
  // E is the sink's error output type (Go: SinkStreamWithResult[T,R,E]).
  template <typename R, typename E, typename SinkFunction, typename _FCtx>
  auto sinkWithResult(const servicelib::config::SinkStreamConfig& cfg,
                      const StreamType<R>&, const StreamType<E>&,
                      StreamFunction<SinkFunction, _FCtx> f)
      -> SinkWithResult<R, E, SinkFunction>& {
    return setConsumer(SinkWithResult<R, E, SinkFunction>::make(
        cfg, this->serde_, this->env_, std::move(f)));
  }

  // Go: MakeJoinStream (with config)
  template <typename _Type, typename _JoinFunction, typename _Kp, typename _Vp,
            JoinTypeEnum _JoinType, JoinStrategyEnum _JoinStrategy,
            typename _CCp, typename _Ctx, typename _FCtx, typename T = _Tp>
  auto join(const servicelib::config::JoinStreamConfig& cfg,
            Stream<KeyValueType<_Kp, _Vp>, _CCp, _Ctx>& stream,
            const StreamType<_Type>&, const JoinType<_JoinType>&,
            const JoinStrategy<_JoinStrategy>&,
            StreamFunction<_JoinFunction, _FCtx> jf)
      -> std::enable_if_t<
          std::is_same_v<T, _Tp> &&
              detail::can_join<KeyValueType<_Kp, _Vp>, T, _Ctx,
                               _JoinStrategy>::value &&
              std::is_same_v<
                  std::invoke_result_t<
                      _JoinFunction, MessageContext, StreamBase&,
                      typename detail::key_value_args<T>::key_type&,
                      std::pair<std::vector<typename detail::key_value_args<
                                    T>::value_type>,
                                std::vector<_Vp>>&,
                      Collector<_Type, void>&&>,
                  void>,
          Join<KeyValueType<_Kp, _Vp>, _Type, _JoinType, _JoinStrategy>&> {
    using TJoinImpl = JoinImpl<KeyValueType<_Kp, _Vp>, _Type, _JoinFunction,
                               _JoinType, _JoinStrategy>;
    auto ret = TJoinImpl::make(cfg, this->serde_, this->env_, std::move(jf));
    stream.setConsumer(
        JoinLinkImpl<KeyValueType<_Kp, _Vp>, TJoinImpl>::make(*ret));
    return setConsumer(std::move(ret));
  }

  // Go: MakeMultiJoinStream (with config)
  template <typename _Type, JoinStrategyEnum _JoinStrategy,
            typename _JoinFunction, typename... ValueType, typename... _CCp,
            typename _Ctx, typename T = _Tp>
  auto multiJoin(
      const servicelib::config::MultiJoinStreamConfig& cfg,
      const StreamType<_Type>&, const JoinStrategy<_JoinStrategy>&,
      StreamFunction<_JoinFunction> f,
      Stream<
          KeyValueType<typename detail::key_value_args<T>::key_type, ValueType>,
          _CCp, _Ctx>&... stream)
      -> std::enable_if_t<
          std::conjunction_v<
              std::is_same<T, _Tp>,
              std::is_same<
                  std::invoke_result_t<
                      _JoinFunction, MessageContext, StreamBase&,
                      typename detail::key_value_args<T>::key_type&,
                      std::tuple<std::vector<typename detail::key_value_args<
                                     T>::value_type>,
                                 std::vector<ValueType>...>&,
                      Collector<_Type, void>&&>,
                  void>,
              std::conjunction<detail::can_join<
                  KeyValueType<typename detail::key_value_args<T>::key_type,
                               ValueType>,
                  T, _Ctx, JoinStrategyEnum::InMemory>...>>,
          MultiJoin<std::tuple<std::vector<typename detail::key_value_args<
                                   T>::value_type>,
                               std::vector<ValueType>...>,
                    _Type, _JoinStrategy>&> {
    using TKey = typename detail::key_value_args<T>::key_type;
    using TValues =
        std::tuple<std::vector<typename detail::key_value_args<T>::value_type>,
                   std::vector<ValueType>...>;
    using TMultiJoinImpl =
        MultiJoinImpl<TValues, _Type, _JoinFunction, _JoinStrategy>;

    auto ret =
        TMultiJoinImpl::make(cfg, this->serde_, this->env_, std::move(f));
    multiJoinHelper<TKey, TMultiJoinImpl, ValueType...>(
        std::index_sequence_for<ValueType...>(), *ret, stream...);
    return setConsumer(std::move(ret));
  }

  // Go: MakeMergeStream. In C++ the first parent owns the construction call,
  // so generated code uses `first.merge(config, other...)`.
  template <typename... C>
  Merge<sizeof...(C) + 1>& merge(
      const servicelib::config::MergeStreamConfig& cfg, C&... streams) {
    return MergeImpl<sizeof...(C) + 1>::build(cfg, this->serde_, this->env_,
                                               *this, streams...);
  }

 private:
  template <typename _Consumer>
  _Consumer& setConsumer(unique_ptr<_Consumer> stream_ptr) {
    _Consumer& r = *stream_ptr;
    if (consumer_) {
      throw StreamException(
          "stream already has a consumer; add an explicit Split operator");
    }
    consumer_ = std::move(stream_ptr);
    // Config-driven graph wiring is single-threaded and both logical edge ids
    // are already known here. Prepare immediately so even callers that invoke
    // a freshly constructed graph before the final topology pass cannot fall
    // back to runtime Caller creation. Synthetic split links have no own
    // config id and are prepared by SplitBase during buildTopology().
    if (StreamBase::getConfigId() != 0) {
      prepareConsumerCaller();
    }
    return r;
  }

  template <typename Key, typename Join, typename... ValueType,
            size_t... Indices, typename RetValue, typename... StreamType>
  void multiJoinHelper(std::index_sequence<Indices...>, RetValue& ret,
                       StreamType&... stream) {
    [[maybe_unused]] auto _ = {
        0, (stream.setConsumer(
                JoinLinkImpl<KeyValueType<Key, ValueType>, Join, Indices>::make(
                    ret)),
            void(), 0)...};
  }
};

}  // namespace servicelib

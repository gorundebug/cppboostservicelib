// Part of class Stream<_Tp, _Cp, _Context> — included by stream.hpp.
// Go analog: operators/multijoin.go.
// Link/storage primitives shared with binary Join are declared in join.inl.

template <typename _TTp, typename _JTp, JoinStrategyEnum _JoinStrategy,
          typename _CCp>
class MultiJoinBuilder;

template <typename _TTp, typename _JTp, JoinStrategyEnum _JoinStrategy,
          typename _CCp = StreamConsumer<_JTp>>
class MultiJoin : public TransformStream<_JTp, _CCp> {
  template <typename, typename>
  friend class StreamExecutionEnvironment;

 protected:
  MultiJoin() = default;
  ~MultiJoin() override = default;

  template <typename T>
  explicit MultiJoin(unique_ptr<T> consumer)
      : TransformStream<_JTp, T>(std::move(consumer)) {}

  const std::string_view& getType() const override {
    return StreamBuilderContext::getType<decltype(*this)>();
  }

  template <typename JoinFunction, typename Ctx>
  static auto build(MultiJoin& stream,
                    StreamFunction<JoinFunction, Ctx>&& function) {
    auto result = MultiJoinBuilder<_TTp, _JTp, _JoinStrategy, _CCp>::build(
        std::move(function));
    result->copySettings(stream);
    return result;
  }

  template <typename T, typename JoinFunction, typename Ctx>
  static auto build(MultiJoin& stream, unique_ptr<T> consumer,
                    StreamFunction<JoinFunction, Ctx>&& function) {
    auto result = MultiJoinBuilder<_TTp, _JTp, _JoinStrategy, T>::build(
        std::move(consumer), std::move(function));
    result->copySettings(stream);
    return result;
  }
};

// Concrete multi-input join implementation.

template <typename _TTp, typename _JTp, typename _JoinFunction,
          JoinStrategyEnum _JoinStrategy, typename _CCp = StreamConsumer<_JTp>>
class MultiJoinImpl final : public MultiJoin<_TTp, _JTp, _JoinStrategy, _CCp>,
                            public virtual StreamConsumer<_Tp> {
  template <typename, typename>
  friend class StreamExecutionEnvironment;
  template <typename, typename, typename>
  friend class Stream;
  template <typename, typename, JoinStrategyEnum, typename>
  friend class MultiBuilder;
  template <typename, typename, size_t>
  friend class JoinLinkImpl;
  template <typename, typename>
  friend class Collector;
  friend class StreamBuilderContext;

  StreamFunction<_JoinFunction, MultiJoinImpl> f_;
  using LeftArgs = detail::key_value_args<_Tp>;
  using Key = typename LeftArgs::key_type;
  std::shared_ptr<store::IJoinStorage<Key>> joinStorage_;

 public:
  using topology_value_type = _JTp;
  using MultiJoin<_TTp, _JTp, _JoinStrategy, _CCp>::consume;

  void consume(MessageContext ctx, Payload<_Tp> payload) override {
    [[maybe_unused]] auto activeSpan =
        tracing::StartStreamSpan(ctx, *this, "stream.join");
    consumeValue(ctx, payload.get().first, 0,
                 payload.get().second);
  }

  size_t getId() const noexcept override {
    return MultiJoin<_TTp, _JTp, _JoinStrategy, _CCp>::getId();
  }

  const std::string& getName() const noexcept override {
    return MultiJoin<_TTp, _JTp, _JoinStrategy, _CCp>::getName();
  }

 protected:
  template <typename T, typename Ctx = MultiJoinImpl>
  MultiJoinImpl(StreamFunction<_JoinFunction, Ctx>&& f, unique_ptr<T> consumer)
      : MultiJoin<_TTp, _JTp, _JoinStrategy, T>(std::move(consumer)),
        f_(std::move(f), *this) {}

  template <typename Ctx = MultiJoinImpl>
  explicit MultiJoinImpl(StreamFunction<_JoinFunction, Ctx>&& f)
      : MultiJoin<_TTp, _JTp, _JoinStrategy, _CCp>(), f_(std::move(f), *this) {}

  // Go-aligned: config id + env; output serde (_JTp) is freshly resolved
  // (Go: runtime.MakeSerde[R](env)). The _Tp serde parameter is just
  // propagated as-is — Go has no serde slot for the input type of a
  // type-changing operator.
  template <typename Ctx = MultiJoinImpl>
  MultiJoinImpl(const servicelib::config::MultiJoinStreamConfig& cfg,
                serde::StreamSerde<_Tp>* serde, IRuntimeEnvironment* env,
                StreamFunction<_JoinFunction, Ctx>&& f)
      : MultiJoin<_TTp, _JTp, _JoinStrategy, _CCp>(), f_(std::move(f), *this) {
    this->setConfigIdentity(cfg);
    StreamConsumer<_Tp>::serde_ = serde;
    MultiJoin<_TTp, _JTp, _JoinStrategy, _CCp>::resolveDefaultSerde();
    this->env_ = env;
    initializeStorage(cfg, env);
  }

  template <typename T, typename F = _JoinFunction,
            typename Ctx = MultiJoinImpl>
  static unique_ptr<MultiJoinImpl<_TTp, _JTp, F, _JoinStrategy, T>> make(
      StreamFunction<F, Ctx>&& f, unique_ptr<T> consumer) {
    return unique_ptr<MultiJoinImpl<_TTp, _JTp, F, _JoinStrategy, T>>(
        new MultiJoinImpl<_TTp, _JTp, F, _JoinStrategy, T>(
            std::move(f), std::move(consumer)));
  }

  template <typename F = _JoinFunction, typename Ctx = MultiJoinImpl>
  static unique_ptr<MultiJoinImpl<_TTp, _JTp, F, _JoinStrategy>> make(
      StreamFunction<F, Ctx>&& f) {
    return unique_ptr<MultiJoinImpl<_TTp, _JTp, F, _JoinStrategy>>(
        new MultiJoinImpl<_TTp, _JTp, F, _JoinStrategy>(std::move(f)));
  }

  template <typename F = _JoinFunction, typename Ctx = MultiJoinImpl>
  static unique_ptr<MultiJoinImpl<_TTp, _JTp, F, _JoinStrategy>> make(
      const servicelib::config::MultiJoinStreamConfig& cfg,
      serde::StreamSerde<_Tp>* serde, IRuntimeEnvironment* env,
      StreamFunction<F, Ctx>&& f) {
    return unique_ptr<MultiJoinImpl<_TTp, _JTp, F, _JoinStrategy>>(
        new MultiJoinImpl<_TTp, _JTp, F, _JoinStrategy>(cfg, serde, env,
                                                        std::move(f)));
  }

  template <typename _PTp, size_t N = 0>
  void consumeRight(MessageContext ctx, Payload<_PTp> payload) {
    static_assert(std::is_same_v<
                  typename detail::key_value_args<_PTp>::key_type, Key>);
    consumeValue(std::move(ctx), payload.get().first, N + 1,
                 payload.get().second);
  }

  size_t buildTopology(StreamBuilderContext& ctx, size_t id,
                       StreamBuilderContext::TIdsList* splitConsumerIds,
                       bool skip) override {
    ctx.buildTopology(*this, id);
    if (splitConsumerIds != nullptr) {
      splitConsumerIds->emplace_back(id);
    }
    return this->buildTopologyCommon(ctx, id, nullptr, skip);
  }

  std::string getCode() const override {
    if (f_.isInternalType()) {
      return f_.getFunctionCode();
    }
    return std::string();
  }

  const StreamBase& getConsumer() const override {
    return MultiJoin<_TTp, _JTp, _JoinStrategy, _CCp>::getConsumer();
  }

  StreamBase& getConsumer() override {
    return MultiJoin<_TTp, _JTp, _JoinStrategy, _CCp>::getConsumer();
  }

  bool hasConsumer() const noexcept override {
    return MultiJoin<_TTp, _JTp, _JoinStrategy, _CCp>::hasConsumer();
  }

  const StreamBase& getBase() const noexcept override {
    return MultiJoin<_TTp, _JTp, _JoinStrategy, _CCp>::getBase();
  }

  StreamBase& getBase() noexcept override {
    return MultiJoin<_TTp, _JTp, _JoinStrategy, _CCp>::getBase();
  }

  void verifyTopology(StreamVerifyContext& ctx) const override {
    MultiJoin<_TTp, _JTp, _JoinStrategy, _CCp>::verifyTopology(ctx);
  }

  void printTopology(TopologyPrinter& tp,
                     std::unordered_set<size_t>& visited) const override {
    MultiJoin<_TTp, _JTp, _JoinStrategy, _CCp>::printTopology(tp, visited);
  }

  const std::string_view& getType() const override {
    if (f_.isInternalType()) {
      return MultiJoin<_TTp, _JTp, _JoinStrategy, _CCp>::getType();
    }
    return StreamBuilderContext::getType<decltype(*this)>();
  }

  static auto build(MultiJoinImpl& stream) {
    auto r = make(std::move(stream.f_));
    r->copySettings(stream);
    static_cast<StreamConsumer<_Tp>&>(*r).copyConsumerSettings(
        static_cast<const StreamConsumer<_Tp>&>(stream));
    return r;
  }

  template <typename T>
  static auto build(MultiJoinImpl& stream, unique_ptr<T> consumer) {
    auto r = make(std::move(stream.f_), std::move(consumer));
    r->copySettings(stream);
    static_cast<StreamConsumer<_Tp>&>(*r).copyConsumerSettings(
        static_cast<const StreamConsumer<_Tp>&>(stream));
    return r;
  }

  template <typename F, typename Ctx>
  static auto build(StreamFunction<F, Ctx>&& f) {
    return make(std::move(f));
  }

  template <typename T, typename F, typename Ctx>
  static auto build(unique_ptr<T> consumer, StreamFunction<F, Ctx>&& f) {
    return make(std::move(f), std::move(consumer));
  }

 private:
  void initializeStorage(const servicelib::config::MultiJoinStreamConfig& cfg,
                         IRuntimeEnvironment* env) {
    if (!env) {
      throw std::invalid_argument("multi-join runtime environment is null");
    }
    auto storage = store::makeJoinStorage<Key>(
        cfg.joinStorage, *env, store::makeJoinStorageConfig(cfg));
    joinStorage_ = std::shared_ptr<store::IJoinStorage<Key>>(std::move(storage));
    auto& app = _Context::getExecutionEnvironment().getApp();
    if constexpr (requires { app.registerStorage(joinStorage_); }) {
      app.registerStorage(joinStorage_);
    } else {
      throw std::logic_error(
          "multi-join runtime environment cannot register storage lifecycle");
    }
  }

  template <std::size_t Index>
  static void copyValues(_TTp& result, const store::JoinValues& values) {
    if (values.size() <= Index) return;
    auto& destination = std::get<Index>(result);
    using Value = typename std::remove_reference_t<decltype(destination)>::value_type;
    destination.reserve(values[Index].size());
    for (const auto& item : values[Index]) {
      destination.push_back(std::any_cast<Value>(item));
    }
  }

  template <std::size_t... Indices>
  static _TTp makeTypedValues(const store::JoinValues& values,
                              std::index_sequence<Indices...>) {
    _TTp result;
    (copyValues<Indices>(result, values), ...);
    return result;
  }

  template <typename Value>
  void consumeValue(MessageContext context, const Key& key, std::size_t index,
                    const Value& value) {
    if (!joinStorage_) {
      throw std::logic_error("multi-join storage is not initialized");
    }
    joinStorage_->joinValue(
        context, key, index, value,
        [this, context, key](store::JoinValues& values) mutable {
          if (values.empty() || values[0].empty()) return false;
          auto typed = makeTypedValues(
              values, std::make_index_sequence<std::tuple_size_v<_TTp>>{});
          auto mutableKey = key;
          bool emitted = false;
          f_(context, *this, mutableKey, typed,
             Collector<_JTp, MultiJoinImpl>(*this, &emitted));
          return emitted;
        });
  }

 protected:
  void produce(MessageContext context, _JTp value) {
    if (this->hasConsumer()) {
      this->context().template consume<_JTp>(
          std::move(context), *this, *this->consumer(),
          Payload<_JTp>::make(std::move(value)));
    }
  }
};

template <typename _TTp, typename _JTp, JoinStrategyEnum _JoinStrategy,
          typename _CCp>
class MultiJoinBuilder final {
  template <typename, typename, JoinStrategyEnum, typename>
  friend class MultiJoin;

 protected:
  template <typename _JoinFunction, typename Ctx>
  static auto build(StreamFunction<_JoinFunction, Ctx>&& f) {
    return MultiJoinImpl<_TTp, _JTp, _JoinFunction, _JoinStrategy, _CCp>::build(
        std::move(f));
  }

  template <typename _JoinFunction, typename T, typename Ctx>
  static auto build(unique_ptr<T> consumer,
                    StreamFunction<_JoinFunction, Ctx>&& f) {
    return MultiJoinImpl<_TTp, _JTp, _JoinFunction, _JoinStrategy, T>::build(
        std::move(consumer), std::move(f));
  }
};

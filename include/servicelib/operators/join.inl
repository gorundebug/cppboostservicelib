// Part of class Stream<_Tp, _Cp, _Context> — included by stream.hpp
// Go analog: operators/join.go — MakeJoinStream / JoinStream

template <typename _TTp, typename _JTp, JoinTypeEnum _JoinType,
          JoinStrategyEnum _JoinStrategy, typename _CCp>
class JoinBuilder;

template <JoinTypeEnum _JoinType, typename _TLeft, typename _TRight>
class InMemoryJoinContainer final : public NotCopyableOrMovable {
  template <typename _TKeyValue>
  using container_type = std::unordered_map<
      typename detail::memory_storage_key_type_helper<
          typename detail::key_value_args<_TKeyValue>::key_type,
          _Context>::ktype,
      typename detail::memory_storage_value_type_helper<
          typename detail::key_value_args<_TKeyValue>::value_type,
          _Context>::vtype,
      typename detail::memory_storage_key_type_helper<
          typename detail::key_value_args<_TKeyValue>::key_type,
          _Context>::hash_type,
      typename detail::memory_storage_key_type_helper<
          typename detail::key_value_args<_TKeyValue>::key_type,
          _Context>::equal_to_type>;

  using TLeftMap = container_type<_TLeft>;
  using TRightMap = container_type<_TRight>;

  class InMemoryJoinEngine {
    std::chrono::microseconds _ttl;

   public:
    explicit InMemoryJoinEngine(std::chrono::microseconds ttl) : _ttl(ttl) {
      if (_ttl.count() == 0) {
      }
    }
  };

  Lazy<std::function<std::unique_ptr<InMemoryJoinEngine>()>> engine_;

 public:
  InMemoryJoinContainer()
      : engine_([]() {
          return std::make_unique<InMemoryJoinEngine>(
              std::chrono::microseconds(0));
        }) {}
};

template <JoinTypeEnum _JoinType, typename _TLeft, typename _TRight>
class RocksDbJoinContainer final : public NotCopyableOrMovable {
 public:
  RocksDbJoinContainer() {}
};

template <JoinTypeEnum _JoinType, typename _TLeft, typename _TRight,
          JoinStrategyEnum _JoinStrategy>
struct JoinContainerFactory;

template <JoinTypeEnum _JoinType, typename _TLeft, typename _TRight>
struct JoinContainerFactory<_JoinType, _TLeft, _TRight,
                            JoinStrategyEnum::InMemory> {
  using type = InMemoryJoinContainer<_JoinType, _TLeft, _TRight>;
};

template <JoinTypeEnum _JoinType, typename _TLeft, typename _TRight>
struct JoinContainerFactory<_JoinType, _TLeft, _TRight,
                            JoinStrategyEnum::RocksDB> {
  using type = RocksDbJoinContainer<_JoinType, _TLeft, _TRight>;
};

template <typename _TTp, typename _JTp, JoinTypeEnum _JoinType,
          JoinStrategyEnum _JoinStrategy, typename _CCp = StreamConsumer<_JTp>>
class Join : public TransformStream<_JTp, _CCp> {
  template <typename, typename>
  friend class StreamExecutionEnvironment;

 protected:
  Join() = default;
  ~Join() override = default;

  template <typename T>
  explicit Join(unique_ptr<T> consumer)
      : TransformStream<_TTp, T>(std::move(consumer)) {}

  const std::string_view& getType() const override {
    return StreamBuilderContext::getType<decltype(*this)>();
  }

  template <typename _JoinFunction, typename Ctx>
  static auto build(Join& stream, StreamFunction<_JoinFunction, Ctx>&& f) {
    auto r = JoinBuilder<_TTp, _JTp, _JoinType, _JoinStrategy, _CCp>::build(
        std::move(f));
    r->copySettings(stream);
    return r;
  }

  template <typename T, typename _JoinFunction, typename Ctx>
  static auto build(Join& stream, unique_ptr<T> consumer,
                    StreamFunction<_JoinFunction, Ctx>&& f) {
    auto r = JoinBuilder<_TTp, _JTp, _JoinType, _JoinStrategy, T>::build(
        std::move(consumer), std::move(f));
    r->copySettings(stream);
    return r;
  }
};

template <typename _TTp, typename _JTp, typename _JoinFunction,
          JoinTypeEnum _JoinType, JoinStrategyEnum _JoinStrategy,
          typename _CCp = StreamConsumer<_JTp>>
class JoinImpl final : public Join<_TTp, _JTp, _JoinType, _JoinStrategy, _CCp>,
                       public virtual StreamConsumer<_Tp> {
  template <typename, typename>
  friend class StreamExecutionEnvironment;
  template <typename, typename, typename>
  friend class Stream;
  template <typename, typename, JoinTypeEnum, JoinStrategyEnum, typename>
  friend class JoinBuilder;
  template <typename, typename, size_t>
  friend class JoinLinkImpl;
  template <typename, typename>
  friend class Collector;
  friend class StreamBuilderContext;

  StreamFunction<_JoinFunction, JoinImpl> f_;
  using LeftArgs = detail::key_value_args<_Tp>;
  using RightArgs = detail::key_value_args<_TTp>;
  using Key = typename LeftArgs::key_type;
  using LeftValue = typename LeftArgs::value_type;
  using RightValue = typename RightArgs::value_type;
  std::shared_ptr<store::IJoinStorage<Key>> joinStorage_;

 public:
  using topology_value_type = _JTp;
  using Join<_TTp, _JTp, _JoinType, _JoinStrategy, _CCp>::consume;

  void consume(MessageContext ctx, Payload<_Tp> payload) override {
    [[maybe_unused]] auto activeSpan =
        tracing::StartStreamSpan(ctx, *this, "stream.join");
    consumeValue(ctx, payload.get().first, 0,
                 payload.get().second);
  }

  size_t getId() const noexcept override {
    return Join<_TTp, _JTp, _JoinType, _JoinStrategy, _CCp>::getId();
  }

  const std::string& getName() const noexcept override {
    return Join<_TTp, _JTp, _JoinType, _JoinStrategy, _CCp>::getName();
  }

 protected:
  template <typename T, typename Ctx = JoinImpl>
  JoinImpl(StreamFunction<_JoinFunction, Ctx>&& f, unique_ptr<T> consumer)
      : Join<_TTp, _JTp, _JoinType, _JoinStrategy, T>(std::move(consumer)),
        f_(std::move(f), *this) {}

  template <typename Ctx = JoinImpl>
  explicit JoinImpl(StreamFunction<_JoinFunction, Ctx>&& f)
      : Join<_TTp, _JTp, _JoinType, _JoinStrategy, _CCp>(),
        f_(std::move(f), *this) {}

  // Go-aligned: config id + env; output serde (_JTp) is freshly resolved
  // (Go: runtime.MakeSerde[R](env)). The _Tp serde parameter is just
  // propagated as-is — Go has no serde slot for the input type of a
  // type-changing operator.
  template <typename Ctx = JoinImpl>
  JoinImpl(const servicelib::config::JoinStreamConfig& cfg,
           serde::StreamSerde<_Tp>* serde, IRuntimeEnvironment* env,
           StreamFunction<_JoinFunction, Ctx>&& f)
      : Join<_TTp, _JTp, _JoinType, _JoinStrategy, _CCp>(),
        f_(std::move(f), *this) {
    this->setConfigIdentity(cfg);
    StreamConsumer<_Tp>::serde_ = serde;
    Join<_TTp, _JTp, _JoinType, _JoinStrategy, _CCp>::resolveDefaultSerde();
    this->env_ = env;
    initializeStorage(cfg, env);
  }

  template <typename T, typename Ctx = JoinImpl>
  JoinImpl(const servicelib::config::JoinStreamConfig& cfg,
           serde::StreamSerde<_Tp>* serde, IRuntimeEnvironment* env,
           StreamFunction<_JoinFunction, Ctx>&& f, unique_ptr<T> consumer)
      : Join<_TTp, _JTp, _JoinType, _JoinStrategy, T>(std::move(consumer)),
        f_(std::move(f), *this) {
    this->setConfigIdentity(cfg);
    StreamConsumer<_Tp>::serde_ = serde;
    Join<_TTp, _JTp, _JoinType, _JoinStrategy, _CCp>::resolveDefaultSerde();
    this->env_ = env;
    initializeStorage(cfg, env);
  }

  template <typename T, typename F = _JoinFunction, typename Ctx = JoinImpl>
  static unique_ptr<JoinImpl<_TTp, _JTp, F, _JoinType, _JoinStrategy, T>> make(
      StreamFunction<F, Ctx>&& f, unique_ptr<T> consumer) {
    return unique_ptr<JoinImpl<_TTp, _JTp, F, _JoinType, _JoinStrategy, T>>(
        new JoinImpl<_TTp, _JTp, F, _JoinType, _JoinStrategy, T>(
            std::move(f), std::move(consumer)));
  }

  template <typename F = _JoinFunction, typename Ctx = JoinImpl>
  static unique_ptr<JoinImpl<_TTp, _JTp, F, _JoinType, _JoinStrategy>> make(
      StreamFunction<F, Ctx>&& f) {
    return unique_ptr<JoinImpl<_TTp, _JTp, F, _JoinType, _JoinStrategy>>(
        new JoinImpl<_TTp, _JTp, F, _JoinType, _JoinStrategy>(std::move(f)));
  }

  template <typename F = _JoinFunction, typename Ctx = JoinImpl>
  static unique_ptr<JoinImpl<_TTp, _JTp, F, _JoinType, _JoinStrategy>> make(
      const servicelib::config::JoinStreamConfig& cfg,
      serde::StreamSerde<_Tp>* serde, IRuntimeEnvironment* env,
      StreamFunction<F, Ctx>&& f) {
    return unique_ptr<JoinImpl<_TTp, _JTp, F, _JoinType, _JoinStrategy>>(
        new JoinImpl<_TTp, _JTp, F, _JoinType, _JoinStrategy>(cfg, serde, env,
                                                              std::move(f)));
  }

  template <typename _PTp, size_t N = 0>
  void consumeRight(MessageContext ctx, Payload<_PTp> payload) {
    static_assert(N == 0);
    static_assert(std::is_same_v<_PTp, _TTp>);
    consumeValue(std::move(ctx), payload.get().first, 1,
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
    return Join<_TTp, _JTp, _JoinType, _JoinStrategy, _CCp>::getConsumer();
  }

  StreamBase& getConsumer() override {
    return Join<_TTp, _JTp, _JoinType, _JoinStrategy, _CCp>::getConsumer();
  }

  bool hasConsumer() const noexcept override {
    return Join<_TTp, _JTp, _JoinType, _JoinStrategy, _CCp>::hasConsumer();
  }

  const StreamBase& getBase() const noexcept override {
    return Join<_TTp, _JTp, _JoinType, _JoinStrategy, _CCp>::getBase();
  }

  StreamBase& getBase() noexcept override {
    return Join<_TTp, _JTp, _JoinType, _JoinStrategy, _CCp>::getBase();
  }

  void verifyTopology(StreamVerifyContext& ctx) const override {
    Join<_TTp, _JTp, _JoinType, _JoinStrategy, _CCp>::verifyTopology(ctx);
  }

  void printTopology(TopologyPrinter& tp,
                     std::unordered_set<size_t>& visited) const override {
    Join<_TTp, _JTp, _JoinType, _JoinStrategy, _CCp>::printTopology(tp,
                                                                    visited);
  }

  const std::string_view& getType() const override {
    if (f_.isInternalType()) {
      return Join<_TTp, _JTp, _JoinType, _JoinStrategy, _CCp>::getType();
    }
    return StreamBuilderContext::getType<decltype(*this)>();
  }

  static auto build(JoinImpl& stream) {
    auto r = make(std::move(stream.f_));
    r->copySettings(stream);
    static_cast<StreamConsumer<_Tp>&>(*r).copyConsumerSettings(
        static_cast<const StreamConsumer<_Tp>&>(stream));
    return r;
  }

 private:
  void initializeStorage(const servicelib::config::JoinStreamConfig& cfg,
                         IRuntimeEnvironment* env) {
    if (!env) throw std::invalid_argument("join runtime environment is null");
    auto storage = store::makeJoinStorage<Key>(
        cfg.joinStorage, *env, store::makeJoinStorageConfig(cfg));
    joinStorage_ = std::shared_ptr<store::IJoinStorage<Key>>(std::move(storage));
    auto& app = _Context::getExecutionEnvironment().getApp();
    if constexpr (requires { app.registerStorage(joinStorage_); }) {
      app.registerStorage(joinStorage_);
    } else {
      throw std::logic_error(
          "join runtime environment cannot register storage lifecycle");
    }
  }

  template <typename Value>
  void consumeValue(MessageContext context, const Key& key, std::size_t index,
                    const Value& value) {
    if (!joinStorage_) throw std::logic_error("join storage is not initialized");
    joinStorage_->joinValue(
        context, key, index, value,
        [this, context, key](store::JoinValues& values) mutable {
          const bool hasLeft = !values.empty() && !values[0].empty();
          const bool hasRight = values.size() > 1 && !values[1].empty();
          bool canCall = false;
          if constexpr (_JoinType == JoinTypeEnum::Inner) {
            canCall = hasLeft && hasRight;
          } else if constexpr (_JoinType == JoinTypeEnum::Left) {
            canCall = hasLeft;
          } else if constexpr (_JoinType == JoinTypeEnum::Right) {
            canCall = hasRight;
          } else if constexpr (_JoinType == JoinTypeEnum::Outer) {
            canCall = true;
          }
          if (!canCall) return false;

          std::pair<std::vector<LeftValue>, std::vector<RightValue>> typed;
          if (!values.empty()) {
            for (const auto& item : values[0]) {
              typed.first.push_back(std::any_cast<LeftValue>(item));
            }
          }
          if (values.size() > 1) {
            for (const auto& item : values[1]) {
              typed.second.push_back(std::any_cast<RightValue>(item));
            }
          }
          auto mutableKey = key;
          bool emitted = false;
          f_(context, *this, mutableKey, typed,
             Collector<_JTp, JoinImpl>(*this, &emitted));
          return emitted;
        });
  }

  void produce(MessageContext context, _JTp value) {
    if (this->hasConsumer()) {
      this->context().template consume<_JTp>(
          std::move(context), *this, *this->consumer(),
          Payload<_JTp>::make(std::move(value)));
    }
  }

  template <typename T>
  static auto build(JoinImpl& stream, unique_ptr<T> consumer) {
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
};

template <typename _TTp, typename _JTp, JoinTypeEnum _JoinType,
          JoinStrategyEnum _JoinStrategy, typename _CCp>
class JoinBuilder final {
  template <typename, typename, JoinTypeEnum, JoinStrategyEnum, typename>
  friend class Join;

 protected:
  template <typename _JoinFunction, typename Ctx>
  static auto build(StreamFunction<_JoinFunction, Ctx>&& f) {
    return JoinImpl<_TTp, _JTp, _JoinFunction, _JoinType, _JoinStrategy,
                    _CCp>::build(std::move(f));
  }

  template <typename _JoinFunction, typename T, typename Ctx>
  static auto build(unique_ptr<T> consumer,
                    StreamFunction<_JoinFunction, Ctx>&& f) {
    return JoinImpl<_TTp, _JTp, _JoinFunction, _JoinType, _JoinStrategy,
                    T>::build(std::move(consumer), std::move(f));
  }
};

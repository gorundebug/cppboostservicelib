// Part of class Stream<_Tp, _Cp, _Context> — included by stream.hpp
// Go analog: operators/keyby.go — MakeKeyByStream / KeyByStream
// Go: KeyByFunction[T, K, V] — (ctx, stream, T, Collect[KeyValue[K,V]]) -> void
// Can emit 0, 1, or multiple KeyValue<K,V> values.
// _Kp = key type, _VTp = value type (need not equal _Tp).

  template <typename _Kp, typename _VTp, typename _CCp>
  class KeyByBuilder;

  template <typename _Kp, typename _VTp, typename _CCp = StreamConsumer<KeyValueType<_Kp, _VTp>>>
  class KeyBy : public TransformStream<KeyValueType<_Kp, _VTp>, _CCp> {
    template <typename, typename>
    friend class StreamExecutionEnvironment;

    using _KVType = KeyValueType<_Kp, _VTp>;

   protected:
    KeyBy() = default;
    ~KeyBy() override = default;

    template <typename T>
    explicit KeyBy(unique_ptr<T> consumer) : TransformStream<_KVType, T>(std::move(consumer)) {}

    const std::string_view& getType() const override { return StreamBuilderContext::getType<decltype(*this)>(); }

    template <typename KeyByFunction, typename Ctx>
    static auto build(KeyBy& stream, StreamFunction<KeyByFunction, Ctx>&& f) {
      auto r = KeyByBuilder<_Kp, _VTp, _CCp>::build(std::move(f));
      r->copySettings(stream);
      return r;
    }

    template <typename T, typename KeyByFunction, typename Ctx>
    static auto build(KeyBy& stream, unique_ptr<T> consumer, StreamFunction<KeyByFunction, Ctx>&& f) {
      auto r = KeyByBuilder<_Kp, _VTp, T>::build(std::move(consumer), std::move(f));
      r->copySettings(stream);
      return r;
    }
  };

  template <typename _Kp, typename _VTp, typename KeyByFunction,
            typename _CCp = StreamConsumer<KeyValueType<_Kp, _VTp>>>
  class KeyByImpl final : public KeyBy<_Kp, _VTp, _CCp>,
                          public virtual StreamConsumer<_Tp> {
    template <typename, typename>
    friend class StreamExecutionEnvironment;
    template <typename, typename, typename>
    friend class Stream;
    template <typename, typename, typename>
    friend class KeyByBuilder;
    template <typename, typename>
    friend class Collector;
    friend class StreamBuilderContext;

    using _KVType = KeyValueType<_Kp, _VTp>;

    StreamFunction<KeyByFunction, KeyByImpl> f_;

   public:
    using KeyBy<_Kp, _VTp, _CCp>::consume;

    // Go: KeyByStream.Consume — calls f with collector; f emits KeyValue<K,V> pairs
    void consume(MessageContext ctx, Payload<_Tp> payload) override {
      [[maybe_unused]] auto activeSpan =
          tracing::StartStreamSpan(ctx, *this, "stream.keyby");
      f_(ctx, *this, payload.get(),
         Collector<_KVType, KeyByImpl>(*this));
    }

    size_t getId() const noexcept override { return KeyBy<_Kp, _VTp, _CCp>::getId(); }

    const std::string& getName() const noexcept override { return KeyBy<_Kp, _VTp, _CCp>::getName(); }

   protected:
    void produce(MessageContext ctx, _KVType v) {
      if (this->hasConsumer()) {
        this->context().template consume<_KVType>(
            std::move(ctx), *this, *this->consumer(),
            Payload<_KVType>::make(std::move(v)));
      }
    }

    template <typename Ctx = KeyByImpl>
    explicit KeyByImpl(StreamFunction<KeyByFunction, Ctx>&& f)
        : KeyBy<_Kp, _VTp, _CCp>(), f_(std::move(f), *this) {}

    template <typename T, typename Ctx = KeyByImpl>
    KeyByImpl(StreamFunction<KeyByFunction, Ctx>&& f, unique_ptr<T> consumer)
        : KeyBy<_Kp, _VTp, T>(std::move(consumer)), f_(std::move(f), *this) {}

    // Go-aligned: config id + env; output serde (_KVType) is freshly resolved
    // (Go: runtime.MakeKeyValueSerde[K, V](env)). The _Tp serde parameter is
    // just propagated as-is — Go has no serde slot for the input type of a
    // type-changing operator.
    template <typename Ctx = KeyByImpl>
    KeyByImpl(const servicelib::config::KeyByStreamConfig& cfg,
              serde::StreamSerde<_Tp>* serde,
              IRuntimeEnvironment* env,
              StreamFunction<KeyByFunction, Ctx>&& f)
        : KeyBy<_Kp, _VTp, _CCp>(), f_(std::move(f), *this) {
      this->setConfigIdentity(cfg);
      StreamConsumer<_Tp>::serde_     = serde;
      (KeyBy<_Kp, _VTp, _CCp>::template resolveDefaultKeyValueSerde<_Kp, _VTp>());
      this->env_                      = env;
    }

    template <typename T, typename Ctx = KeyByImpl>
    KeyByImpl(const servicelib::config::KeyByStreamConfig& cfg,
              serde::StreamSerde<_Tp>* serde,
              IRuntimeEnvironment* env,
              StreamFunction<KeyByFunction, Ctx>&& f,
              unique_ptr<T> consumer)
        : KeyBy<_Kp, _VTp, T>(std::move(consumer)), f_(std::move(f), *this) {
      this->setConfigIdentity(cfg);
      StreamConsumer<_Tp>::serde_     = serde;
      (KeyBy<_Kp, _VTp, _CCp>::template resolveDefaultKeyValueSerde<_Kp, _VTp>());
      this->env_                      = env;
    }

    template <typename F = KeyByFunction, typename Ctx = KeyByImpl>
    static unique_ptr<KeyByImpl<_Kp, _VTp, F>> make(StreamFunction<F, Ctx>&& f) {
      return unique_ptr<KeyByImpl<_Kp, _VTp, F>>(new KeyByImpl<_Kp, _VTp, F>(std::move(f)));
    }

    template <typename T, typename F = KeyByFunction, typename Ctx = KeyByImpl>
    static unique_ptr<KeyByImpl<_Kp, _VTp, F, T>> make(StreamFunction<F, Ctx>&& f, unique_ptr<T> consumer) {
      return unique_ptr<KeyByImpl<_Kp, _VTp, F, T>>(
          new KeyByImpl<_Kp, _VTp, F, T>(std::move(f), std::move(consumer)));
    }

    template <typename F = KeyByFunction, typename Ctx = KeyByImpl>
    static unique_ptr<KeyByImpl<_Kp, _VTp, F>> make(const servicelib::config::KeyByStreamConfig& cfg,
                                                      serde::StreamSerde<_Tp>* serde,
                                                      IRuntimeEnvironment* env,
                                                      StreamFunction<F, Ctx>&& f) {
      return unique_ptr<KeyByImpl<_Kp, _VTp, F>>(new KeyByImpl<_Kp, _VTp, F>(cfg, serde, env, std::move(f)));
    }

    template <typename T, typename F = KeyByFunction, typename Ctx = KeyByImpl>
    static unique_ptr<KeyByImpl<_Kp, _VTp, F, T>> make(const servicelib::config::KeyByStreamConfig& cfg,
                                                          serde::StreamSerde<_Tp>* serde,
                                                          IRuntimeEnvironment* env,
                                                          StreamFunction<F, Ctx>&& f,
                                                          unique_ptr<T> consumer) {
      return unique_ptr<KeyByImpl<_Kp, _VTp, F, T>>(
          new KeyByImpl<_Kp, _VTp, F, T>(cfg, serde, env, std::move(f), std::move(consumer)));
    }

    size_t buildTopology(StreamBuilderContext& ctx, size_t id, StreamBuilderContext::TIdsList* splitConsumerIds,
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

    const StreamBase& getConsumer() const override { return KeyBy<_Kp, _VTp, _CCp>::getConsumer(); }

    StreamBase& getConsumer() override { return KeyBy<_Kp, _VTp, _CCp>::getConsumer(); }

    bool hasConsumer() const noexcept override { return KeyBy<_Kp, _VTp, _CCp>::hasConsumer(); }

    const StreamBase& getBase() const noexcept override { return KeyBy<_Kp, _VTp, _CCp>::getBase(); }

    StreamBase& getBase() noexcept override { return KeyBy<_Kp, _VTp, _CCp>::getBase(); }

    void verifyTopology(StreamVerifyContext& ctx) const override { KeyBy<_Kp, _VTp, _CCp>::verifyTopology(ctx); }

    void printTopology(TopologyPrinter& tp, std::unordered_set<size_t>& visited) const override {
      KeyBy<_Kp, _VTp, _CCp>::printTopology(tp, visited);
    }

    const std::string_view& getType() const override {
      if (f_.isInternalType()) {
        return KeyBy<_Kp, _VTp, _CCp>::getType();
      }
      return StreamBuilderContext::getType<decltype(*this)>();
    }

    static auto build(KeyByImpl& stream) {
      auto r = make(std::move(stream.f_));
      r->copySettings(stream);
      static_cast<StreamConsumer<_Tp>&>(*r).copyConsumerSettings(
          static_cast<const StreamConsumer<_Tp>&>(stream));
      return r;
    }

    template <typename T>
    static auto build(KeyByImpl& stream, unique_ptr<T> consumer) {
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

  template <typename _Kp, typename _VTp, typename _CCp>
  class KeyByBuilder final {
    template <typename, typename, typename>
    friend class KeyBy;

   protected:
    template <typename KeyByFunction, typename Ctx>
    static auto build(StreamFunction<KeyByFunction, Ctx>&& f) {
      return KeyByImpl<_Kp, _VTp, KeyByFunction, _CCp>::build(std::move(f));
    }

    template <typename KeyByFunction, typename Ctx>
    static auto build(unique_ptr<_CCp> consumer, StreamFunction<KeyByFunction, Ctx>&& f) {
      return KeyByImpl<_Kp, _VTp, KeyByFunction, _CCp>::build(std::move(consumer), std::move(f));
    }
  };

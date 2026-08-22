// Part of class Stream<_Tp, _Cp, _Context> — included by stream.hpp
// Go analog: operators/flatmap.go — MakeFlatMapStream / FlatMapStream

  template <typename _TTp, typename _CCp>
  class FlatMapBuilder;

  template <typename _TTp, typename _CCp>
  class FlatMap : public TransformStream<_TTp, _CCp> {
    template <typename, typename>
    friend class StreamExecutionEnvironment;
    friend class StreamBuilderContext;

   protected:
    FlatMap() = default;
    ~FlatMap() override = default;

    template <typename T>
    explicit FlatMap(unique_ptr<T> consumer) : TransformStream<_TTp, T>(std::move(consumer)) {}

    const std::string_view& getType() const override { return StreamBuilderContext::getType<decltype(*this)>(); }

    template <typename FlatMapFunction, typename Ctx>
    static auto build(FlatMap& stream, StreamFunction<FlatMapFunction, Ctx>&& f) {
      auto r = FlatMapBuilder<_TTp, _CCp>::build(std::move(f));
      r->copySettings(stream);
      return r;
    }

    template <typename T, typename FlatMapFunction, typename Ctx>
    static auto build(FlatMap& stream, unique_ptr<T> consumer, StreamFunction<FlatMapFunction, Ctx>&& f) {
      auto r = FlatMapBuilder<_TTp, T>::build(std::move(consumer), std::move(f));
      r->copySettings(stream);
      return r;
    }
  };

  template <typename _TTp, typename FlatMapFunction, typename _CCp = StreamConsumer<_TTp>>
  class FlatMapImpl final : public FlatMap<_TTp, _CCp>, public virtual StreamConsumer<_Tp> {
    template <typename, typename>
    friend class StreamExecutionEnvironment;
    template <typename, typename, typename>
    friend class Stream;
    template <typename, typename>
    friend class FlatMapBuilder;
    template <typename, typename>
    friend class Collector;
    friend class StreamBuilderContext;

    StreamFunction<FlatMapFunction, FlatMapImpl> f_;

   public:
    using FlatMap<_TTp, _CCp>::consume;

    void consume(MessageContext ctx, Payload<_Tp> payload) override {
      [[maybe_unused]] auto activeSpan =
          tracing::StartStreamSpan(ctx, *this, "stream.flatmap");
      f_(ctx, *this, payload.get(),
         Collector<_TTp, decltype(*this)>(*this));
    }

    size_t getId() const noexcept override { return FlatMap<_TTp, _CCp>::getId(); }

    const std::string& getName() const noexcept override { return FlatMap<_TTp, _CCp>::getName(); }

   protected:
    void produce(MessageContext ctx, _TTp v) {
      if (this->hasConsumer()) {
        this->context().template consume<_TTp>(
            std::move(ctx), *this, *this->consumer(),
            Payload<_TTp>::make(std::move(v)));
      }
    }

    template <typename Ctx = FlatMapImpl>
    explicit FlatMapImpl(StreamFunction<FlatMapFunction, Ctx>&& f) : FlatMap<_TTp, _CCp>(), f_(std::move(f), *this) {}

    template <typename T, typename Ctx = FlatMapImpl>
    FlatMapImpl(StreamFunction<FlatMapFunction, Ctx>&& f, unique_ptr<T> consumer)
        : FlatMap<_TTp, T>(std::move(consumer)), f_(std::move(f), *this) {}

    // Go-aligned: config id + env; output serde (_TTp) is freshly resolved
    // (Go: runtime.MakeSerde[R](env)). The _Tp serde parameter is just
    // propagated as-is — Go has no serde slot for the input type of a
    // type-changing operator.
    template <typename Ctx = FlatMapImpl>
    FlatMapImpl(const servicelib::config::FlatMapStreamConfig& cfg,
                serde::StreamSerde<_Tp>* serde,
                IRuntimeEnvironment* env,
                StreamFunction<FlatMapFunction, Ctx>&& f)
        : FlatMap<_TTp, _CCp>(), f_(std::move(f), *this) {
      this->setConfigIdentity(cfg);
      StreamConsumer<_Tp>::serde_     = serde;
      FlatMap<_TTp, _CCp>::resolveDefaultSerde();
      this->env_                      = env;
    }

    template <typename T, typename Ctx = FlatMapImpl>
    FlatMapImpl(const servicelib::config::FlatMapStreamConfig& cfg,
                serde::StreamSerde<_Tp>* serde,
                IRuntimeEnvironment* env,
                StreamFunction<FlatMapFunction, Ctx>&& f,
                unique_ptr<T> consumer)
        : FlatMap<_TTp, T>(std::move(consumer)), f_(std::move(f), *this) {
      this->setConfigIdentity(cfg);
      StreamConsumer<_Tp>::serde_     = serde;
      FlatMap<_TTp, _CCp>::resolveDefaultSerde();
      this->env_                      = env;
    }

    template <typename T, typename F = FlatMapFunction, typename Ctx = FlatMapImpl>
    static unique_ptr<FlatMapImpl<_TTp, F, T>> make(StreamFunction<F, Ctx>&& f, unique_ptr<T> consumer) {
      return unique_ptr<FlatMapImpl<_TTp, F, T>>(new FlatMapImpl<_TTp, F, T>(std::move(f), std::move(consumer)));
    }

    template <typename F = FlatMapFunction, typename Ctx = FlatMapImpl>
    static unique_ptr<FlatMapImpl<_TTp, F>> make(StreamFunction<F, Ctx>&& f) {
      return unique_ptr<FlatMapImpl<_TTp, F>>(new FlatMapImpl<_TTp, F>(std::move(f)));
    }

    template <typename F = FlatMapFunction, typename Ctx = FlatMapImpl>
    static unique_ptr<FlatMapImpl<_TTp, F>> make(const servicelib::config::FlatMapStreamConfig& cfg,
                                                   serde::StreamSerde<_Tp>* serde,
                                                   IRuntimeEnvironment* env,
                                                   StreamFunction<F, Ctx>&& f) {
      return unique_ptr<FlatMapImpl<_TTp, F>>(new FlatMapImpl<_TTp, F>(cfg, serde, env, std::move(f)));
    }

    template <typename T, typename F = FlatMapFunction, typename Ctx = FlatMapImpl>
    static unique_ptr<FlatMapImpl<_TTp, F, T>> make(const servicelib::config::FlatMapStreamConfig& cfg,
                                                      serde::StreamSerde<_Tp>* serde,
                                                      IRuntimeEnvironment* env,
                                                      StreamFunction<F, Ctx>&& f,
                                                      unique_ptr<T> consumer) {
      return unique_ptr<FlatMapImpl<_TTp, F, T>>(
          new FlatMapImpl<_TTp, F, T>(cfg, serde, env, std::move(f), std::move(consumer)));
    }

    const std::string_view& getType() const override {
      if (f_.isInternalType()) {
        return FlatMap<_TTp, _CCp>::getType();
      }
      return StreamBuilderContext::getType<decltype(*this)>();
    }

    std::string getCode() const override {
      if (f_.isInternalType()) {
        return f_.getFunctionCode();
      }
      return std::string();
    }

    bool hasConsumer() const noexcept override { return FlatMap<_TTp, _CCp>::hasConsumer(); }

    size_t buildTopology(StreamBuilderContext& ctx, size_t id, StreamBuilderContext::TIdsList* splitConsumerIds,
                         bool skip) override {
      ctx.buildTopology(*this, id);
      if (splitConsumerIds != nullptr) {
        splitConsumerIds->emplace_back(id);
      }
      return this->buildTopologyCommon(ctx, id, nullptr, skip);
    }

    void verifyTopology(StreamVerifyContext& ctx) const override { FlatMap<_TTp, _CCp>::verifyTopology(ctx); }

    void printTopology(TopologyPrinter& tp, std::unordered_set<size_t>& visited) const override {
      FlatMap<_TTp, _CCp>::printTopology(tp, visited);
    }

    const StreamBase& getConsumer() const override { return FlatMap<_TTp, _CCp>::getConsumer(); }

    StreamBase& getConsumer() override { return FlatMap<_TTp, _CCp>::getConsumer(); }

    const StreamBase& getBase() const noexcept override { return FlatMap<_TTp, _CCp>::getBase(); }

    StreamBase& getBase() noexcept override { return FlatMap<_TTp, _CCp>::getBase(); }

    static auto build(FlatMapImpl& stream) {
      auto r = make(std::move(stream.f_));
      r->copySettings(stream);
      static_cast<StreamConsumer<_Tp>&>(*r).copyConsumerSettings(
          static_cast<const StreamConsumer<_Tp>&>(stream));
      return r;
    }

    template <typename T>
    static auto build(FlatMapImpl& stream, unique_ptr<T> consumer) {
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

  template <typename _TTp, typename _CCp>
  class FlatMapBuilder final {
    template <typename, typename>
    friend class FlatMap;

   protected:
    template <typename FlatMapFunction, typename Ctx>
    static auto build(StreamFunction<FlatMapFunction, Ctx>&& f) {
      return FlatMapImpl<_TTp, FlatMapFunction, _CCp>::build(std::move(f));
    }

    template <typename FlatMapFunction, typename Ctx>
    static auto build(unique_ptr<_CCp> consumer, StreamFunction<FlatMapFunction, Ctx>&& f) {
      return FlatMapImpl<_TTp, FlatMapFunction, _CCp>::build(std::move(consumer), std::move(f));
    }
  };

// Part of class Stream<_Tp, _Cp, _Context> — included by stream.hpp
// Go analog: operators/map.go — MakeMapStream / MapStream
// MapFunction: (MessageContext, const _Tp&, Collector<_TTp>) -> void
// Can emit 0, 1, or multiple _TTp values (unlike C++ return-based map).

  template <typename _TTp, typename _CCp>
  class MapBuilder;

  template <typename _TTp, typename _CCp = StreamConsumer<_TTp>>
  class Map : public TransformStream<_TTp, _CCp> {
    template <typename, typename>
    friend class StreamExecutionEnvironment;

   protected:
    Map() = default;
    ~Map() override = default;

    template <typename T>
    explicit Map(unique_ptr<T> consumer) : TransformStream<_TTp, T>(std::move(consumer)) {}

    const std::string_view& getType() const override { return StreamBuilderContext::getType<decltype(*this)>(); }

    template <typename MapFunction, typename Ctx>
    static auto build(Map& stream, StreamFunction<MapFunction, Ctx>&& f) {
      auto r = MapBuilder<_TTp, _CCp>::build(std::move(f));
      r->copySettings(stream);
      return r;
    }

    template <typename T, typename MapFunction, typename Ctx>
    static auto build(Map& stream, unique_ptr<T> consumer, StreamFunction<MapFunction, Ctx>&& f) {
      auto r = MapBuilder<_TTp, T>::build(std::move(consumer), std::move(f));
      r->copySettings(stream);
      return r;
    }
  };

  template <typename _TTp, typename MapFunction, typename _CCp = StreamConsumer<_TTp>>
  class MapImpl final : public Map<_TTp, _CCp>, public virtual StreamConsumer<_Tp> {
    template <typename, typename>
    friend class StreamExecutionEnvironment;
    template <typename, typename, typename>
    friend class Stream;
    template <typename, typename>
    friend class MapBuilder;
    template <typename, typename>
    friend class Collector;
    friend class StreamBuilderContext;

    StreamFunction<MapFunction, MapImpl> f_;

   public:
    using Map<_TTp, _CCp>::consume;

    // Go: MapStream.Consume — calls f with collector; f may emit 0, 1, or many _TTp values
    void consume(MessageContext ctx, Payload<_Tp> payload) override {
      [[maybe_unused]] auto activeSpan =
          tracing::StartStreamSpan(ctx, *this, "stream.map");
      f_(ctx, *this, payload.get(), Collector<_TTp, MapImpl>(*this));
    }

    size_t getId() const noexcept override { return Map<_TTp, _CCp>::getId(); }

    [[nodiscard]] const std::string& getName() const noexcept override { return Map<_TTp, _CCp>::getName(); }

   protected:
    void produce(MessageContext ctx, _TTp v) {
      if (this->hasConsumer()) {
        this->context().template consume<_TTp>(
            std::move(ctx), *this, *this->consumer(),
            Payload<_TTp>::make(std::move(v)));
      }
    }

    template <typename Ctx = MapImpl>
    explicit MapImpl(StreamFunction<MapFunction, Ctx>&& f) : Map<_TTp, _CCp>(), f_(std::move(f), *this) {}

    template <typename T, typename Ctx = MapImpl>
    MapImpl(StreamFunction<MapFunction, Ctx>&& f, unique_ptr<T> consumer)
        : Map<_TTp, T>(std::move(consumer)), f_(std::move(f), *this) {}

    // Go-aligned: config id + env; output serde (_TTp) is freshly resolved
    // (Go: runtime.MakeSerde[R](env)). The _Tp serde parameter is just
    // propagated as-is — Go has no serde slot for the input type of a
    // type-changing operator.
    template <typename Ctx = MapImpl>
    MapImpl(const servicelib::config::MapStreamConfig& cfg,
            serde::StreamSerde<_Tp>* serde,
            IRuntimeEnvironment* env,
            StreamFunction<MapFunction, Ctx>&& f)
        : Map<_TTp, _CCp>(), f_(std::move(f), *this) {
      this->setConfigIdentity(cfg);
      StreamConsumer<_Tp>::serde_     = serde;
      Map<_TTp, _CCp>::resolveDefaultSerde();
      this->env_                      = env;
    }

    template <typename T, typename Ctx = MapImpl>
    MapImpl(const servicelib::config::MapStreamConfig& cfg,
            serde::StreamSerde<_Tp>* serde,
            IRuntimeEnvironment* env,
            StreamFunction<MapFunction, Ctx>&& f,
            unique_ptr<T> consumer)
        : Map<_TTp, T>(std::move(consumer)), f_(std::move(f), *this) {
      this->setConfigIdentity(cfg);
      StreamConsumer<_Tp>::serde_     = serde;
      Map<_TTp, _CCp>::resolveDefaultSerde();
      this->env_                      = env;
    }

    template <typename F = MapFunction, typename Ctx = MapImpl>
    static unique_ptr<MapImpl<_TTp, F>> make(StreamFunction<F, Ctx>&& f) {
      return unique_ptr<MapImpl<_TTp, F>>(new MapImpl<_TTp, F>(std::move(f)));
    }

    template <typename T, typename F = MapFunction, typename Ctx = MapImpl>
    static unique_ptr<MapImpl<_TTp, F, T>> make(StreamFunction<F, Ctx>&& f, unique_ptr<T> consumer) {
      return unique_ptr<MapImpl<_TTp, F, T>>(new MapImpl<_TTp, F, T>(std::move(f), std::move(consumer)));
    }

    template <typename F = MapFunction, typename Ctx = MapImpl>
    static unique_ptr<MapImpl<_TTp, F>> make(const servicelib::config::MapStreamConfig& cfg,
                                              serde::StreamSerde<_Tp>* serde,
                                              IRuntimeEnvironment* env,
                                              StreamFunction<F, Ctx>&& f) {
      return unique_ptr<MapImpl<_TTp, F>>(new MapImpl<_TTp, F>(cfg, serde, env, std::move(f)));
    }

    template <typename T, typename F = MapFunction, typename Ctx = MapImpl>
    static unique_ptr<MapImpl<_TTp, F, T>> make(const servicelib::config::MapStreamConfig& cfg,
                                                  serde::StreamSerde<_Tp>* serde,
                                                  IRuntimeEnvironment* env,
                                                  StreamFunction<F, Ctx>&& f,
                                                  unique_ptr<T> consumer) {
      return unique_ptr<MapImpl<_TTp, F, T>>(
          new MapImpl<_TTp, F, T>(cfg, serde, env, std::move(f), std::move(consumer)));
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

    const StreamBase& getConsumer() const override { return Map<_TTp, _CCp>::getConsumer(); }

    StreamBase& getConsumer() override { return Map<_TTp, _CCp>::getConsumer(); }

    bool hasConsumer() const noexcept override { return Map<_TTp, _CCp>::hasConsumer(); }

    const StreamBase& getBase() const noexcept override { return Map<_TTp, _CCp>::getBase(); }

    StreamBase& getBase() noexcept override { return Map<_TTp, _CCp>::getBase(); }

    void verifyTopology(StreamVerifyContext& ctx) const override { Map<_TTp, _CCp>::verifyTopology(ctx); }

    void printTopology(TopologyPrinter& tp, std::unordered_set<size_t>& visited) const override {
      Map<_TTp, _CCp>::printTopology(tp, visited);
    }

    const std::string_view& getType() const override {
      if (f_.isInternalType()) {
        return Map<_TTp, _CCp>::getType();
      }
      return StreamBuilderContext::getType<decltype(*this)>();
    }

    static auto build(MapImpl& stream) {
      auto r = make(std::move(stream.f_));
      r->copySettings(stream);
      static_cast<StreamConsumer<_Tp>&>(*r).copyConsumerSettings(
          static_cast<const StreamConsumer<_Tp>&>(stream));
      return r;
    }

    template <typename T>
    static auto build(MapImpl& stream, unique_ptr<T> consumer) {
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
  class MapBuilder final {
    template <typename, typename>
    friend class Map;

   protected:
    template <typename MapFunction, typename Ctx>
    static auto build(StreamFunction<MapFunction, Ctx>&& f) {
      return MapImpl<_TTp, MapFunction, _CCp>::build(std::move(f));
    }

    template <typename MapFunction, typename Ctx>
    static auto build(unique_ptr<_CCp> consumer, StreamFunction<MapFunction, Ctx>&& f) {
      return MapImpl<_TTp, MapFunction, _CCp>::build(std::move(consumer), std::move(f));
    }
  };

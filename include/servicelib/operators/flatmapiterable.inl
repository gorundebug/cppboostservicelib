// Part of class Stream<_Tp, _Cp, _Context> — included by stream.hpp
// Go analog: operators/flatmapiterable.go — MakeFlatMapIterableStream / FlatMapIterableStream

  template <typename T, bool isIter = detail::is_iterable_v<T> && std::is_same_v<T, _Tp>>
  struct flatmap_iterable_type {};

  template <typename T>
  struct flatmap_iterable_type<T, true> {
    using type =
        std::conditional_t<std::is_const_v<_Tp>,
                           const typename std::iterator_traits<decltype(std::begin(std::declval<T>()))>::value_type,
                           typename std::iterator_traits<decltype(std::begin(std::declval<T>()))>::value_type>;
  };

  template <typename _CCp>
  class FlatMapIterable : public Stream<typename flatmap_iterable_type<_Tp>::type, _CCp, _Context> {
   protected:
    FlatMapIterable() = default;
    ~FlatMapIterable() override = default;

    template <typename T>
    explicit FlatMapIterable(unique_ptr<T> consumer)
        : Stream<typename flatmap_iterable_type<_Tp>::type, _CCp, _Context>(std::move(consumer)) {}
  };

  template <typename _CCp>
  class FlatMapIterableImpl final : public FlatMapIterable<_CCp>, public virtual StreamConsumer<_Tp> {
    template <typename, typename>
    friend class StreamExecutionEnvironment;
    template <typename, typename, typename>
    friend class Stream;
    friend class StreamBuilderContext;

    using _VT = typename flatmap_iterable_type<_Tp>::type;

   public:
    using FlatMapIterable<_CCp>::consume;

    void consume(MessageContext ctx, Payload<_Tp> payload) override {
      [[maybe_unused]] auto activeSpan =
          tracing::StartStreamSpan(ctx, *this, "stream.flatmap_iterable");
      if (this->hasConsumer()) {
        auto& values = payload.get();
        auto current = std::begin(values);
        const auto end = std::end(values);
        while (current != end) {
          auto next = current;
          ++next;
          auto item = payload.unique()
                          ? Payload<_VT>::make(std::move(*current))
                          : Payload<_VT>::make(*current);
          this->context().template consume<_VT>(
              next == end ? std::move(ctx) : ctx,
              *this, *this->consumer(), std::move(item));
          current = next;
        }
      }
    }

    size_t getId() const noexcept override { return FlatMapIterable<_CCp>::getId(); }

    const std::string& getName() const noexcept override { return FlatMapIterable<_CCp>::getName(); }

   protected:
    FlatMapIterableImpl() noexcept = default;

    template <typename T>
    explicit FlatMapIterableImpl(unique_ptr<T> consumer) noexcept : FlatMapIterable<_CCp>(std::move(consumer)) {}

    // Go-aligned: config id + env; output serde (_VT) is freshly resolved
    // (Go: runtime.MakeSerde[R](env)). The _Tp serde parameter is just
    // propagated as-is — Go has no serde slot for the input type of a
    // type-changing operator.
    explicit FlatMapIterableImpl(const servicelib::config::FlatMapIterableStreamConfig& cfg,
                                 serde::StreamSerde<_Tp>* serde,
                                 IRuntimeEnvironment* env)
        : FlatMapIterable<_CCp>() {
      this->setConfigIdentity(cfg);
      StreamConsumer<_Tp>::serde_     = serde;
      FlatMapIterable<_CCp>::resolveDefaultSerde();
      this->env_                      = env;
    }

    template <typename T>
    FlatMapIterableImpl(const servicelib::config::FlatMapIterableStreamConfig& cfg,
                        serde::StreamSerde<_Tp>* serde,
                        IRuntimeEnvironment* env,
                        unique_ptr<T> consumer)
        : FlatMapIterable<_CCp>(std::move(consumer)) {
      this->setConfigIdentity(cfg);
      StreamConsumer<_Tp>::serde_     = serde;
      FlatMapIterable<_CCp>::resolveDefaultSerde();
      this->env_                      = env;
    }

    static unique_ptr<FlatMapIterableImpl> make() { return unique_ptr<FlatMapIterableImpl>(new FlatMapIterableImpl()); }

    template <typename T>
    static auto make(unique_ptr<T> consumer) {
      return unique_ptr<FlatMapIterableImpl<T>>(new FlatMapIterableImpl<T>(std::move(consumer)));
    }

    static unique_ptr<FlatMapIterableImpl> make(const servicelib::config::FlatMapIterableStreamConfig& cfg,
                                                serde::StreamSerde<_Tp>* serde,
                                                IRuntimeEnvironment* env) {
      return unique_ptr<FlatMapIterableImpl>(new FlatMapIterableImpl(cfg, serde, env));
    }

    template <typename T>
    static auto make(const servicelib::config::FlatMapIterableStreamConfig& cfg,
                     serde::StreamSerde<_Tp>* serde,
                     IRuntimeEnvironment* env,
                     unique_ptr<T> consumer) {
      return unique_ptr<FlatMapIterableImpl<T>>(new FlatMapIterableImpl<T>(cfg, serde, env, std::move(consumer)));
    }

    bool hasConsumer() const noexcept override { return FlatMapIterable<_CCp>::hasConsumer(); }

    size_t buildTopology(StreamBuilderContext& ctx, size_t id, StreamBuilderContext::TIdsList* splitConsumerIds,
                         bool skip) override {
      ctx.buildTopology(*this, id);
      if (splitConsumerIds != nullptr) {
        splitConsumerIds->emplace_back(id);
      }
      return this->buildTopologyCommon(ctx, id, nullptr, skip);
    }

    void verifyTopology(StreamVerifyContext& ctx) const override { FlatMapIterable<_CCp>::verifyTopology(ctx); }

    void printTopology(TopologyPrinter& tp, std::unordered_set<size_t>& visited) const override {
      FlatMapIterable<_CCp>::printTopology(tp, visited);
    }

    const StreamBase& getConsumer() const override { return FlatMapIterable<_CCp>::getConsumer(); }

    StreamBase& getConsumer() override { return FlatMapIterable<_CCp>::getConsumer(); }

    const StreamBase& getBase() const noexcept override { return FlatMapIterable<_CCp>::getBase(); }

    StreamBase& getBase() noexcept override { return FlatMapIterable<_CCp>::getBase(); }

    const std::string_view& getType() const override { return StreamBuilderContext::getType<decltype(*this)>(); }

    std::string getCode() const override { return FlatMapIterable<_CCp>::getCode(); }

    static auto build(FlatMapIterableImpl& stream) {
      auto r = make();
      r->copySettings(stream);
      static_cast<StreamConsumer<_Tp>&>(*r).copyConsumerSettings(
          static_cast<const StreamConsumer<_Tp>&>(stream));
      return r;
    }

    template <typename T>
    static auto build(FlatMapIterableImpl& stream, unique_ptr<T> consumer) {
      auto r = make(std::move(consumer));
      r->copySettings(stream);
      static_cast<StreamConsumer<_Tp>&>(*r).copyConsumerSettings(
          static_cast<const StreamConsumer<_Tp>&>(stream));
      return r;
    }
  };

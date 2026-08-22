// Part of class Stream<_Tp, _Cp, _Context> — included by stream.hpp
// Go analog: operators/filter.go — MakeFilterStream / FilterStream

  template <typename _CCp>
  class FilterBuilder;

  template <typename _CCp = StreamConsumer<_Tp>>
  class Filter : public TransformStream<_Tp, _CCp> {
    template <typename, typename>
    friend class StreamExecutionEnvironment;

   protected:
    Filter() = default;
    ~Filter() override = default;

    template <typename T>
    explicit Filter(unique_ptr<T> consumer) : TransformStream<_Tp, T>(std::move(consumer)) {}

    const std::string_view& getType() const override { return StreamBuilderContext::getType<decltype(*this)>(); }

    template <typename _FilterFunction, typename Ctx>
    static auto build(Filter& stream, StreamFunction<_FilterFunction, Ctx>&& f) {
      auto r = FilterBuilder<_CCp>::build(std::move(f));
      r->copySettings(stream);           // name_ + env_ (StreamBase)
      r->copyConsumerSettings(stream);   // serde_ (StreamConsumer<_Tp>)
      return r;
    }

    template <typename T, typename _FilterFunction, typename Ctx>
    static auto build(Filter& stream, unique_ptr<T> consumer, StreamFunction<_FilterFunction, Ctx>&& f) {
      auto r = FilterBuilder<T>::build(std::move(consumer), std::move(f));
      r->copySettings(stream);
      r->copyConsumerSettings(stream);
      return r;
    }
  };

  template <typename _FilterFunction, typename _CCp = StreamConsumer<_Tp>>
  class FilterImpl final : public Filter<_CCp> {
    template <typename, typename>
    friend class StreamExecutionEnvironment;
    template <typename, typename, typename>
    friend class Stream;
    template <typename>
    friend class FilterBuilder;
    friend class StreamBuilderContext;

    StreamFunction<_FilterFunction, FilterImpl> f_;

   public:
    // Go: FilterStream.Consume — calls predicate, emits downstream if true
    void consume(MessageContext ctx, Payload<_Tp> payload) override {
      [[maybe_unused]] auto activeSpan =
          tracing::StartStreamSpan(ctx, *this, "stream.filter");
      if (f_(ctx, *this, payload.get())) {
        if (this->hasConsumer())
          this->context().template consume<_Tp>(
              std::move(ctx), *this, *this->consumer(), std::move(payload));
      }
    }

   protected:
    // Internal constructor — used by build() for codegen path; settings copied after
    template <typename Ctx = FilterImpl>
    explicit FilterImpl(StreamFunction<_FilterFunction, Ctx>&& f)
        : Filter<_CCp>(), f_(std::move(f), *this) {}

    template <typename T, typename Ctx = FilterImpl>
    FilterImpl(StreamFunction<_FilterFunction, Ctx>&& f, unique_ptr<T> consumer)
        : Filter<T>(std::move(consumer)), f_(std::move(f), *this) {}

    // Go-aligned constructor: config ID and serde are set from ctor args.
    template <typename Ctx = FilterImpl>
    FilterImpl(const servicelib::config::FilterStreamConfig& cfg,
               serde::StreamSerde<_Tp>* serde,
               IRuntimeEnvironment* env,
               StreamFunction<_FilterFunction, Ctx>&& f)
        : Filter<_CCp>(), f_(std::move(f), *this) {
      this->setConfigIdentity(cfg);
      this->serde_     = serde;
      this->env_       = env;
    }

    template <typename T, typename Ctx = FilterImpl>
    FilterImpl(const servicelib::config::FilterStreamConfig& cfg,
               serde::StreamSerde<_Tp>* serde,
               IRuntimeEnvironment* env,
               StreamFunction<_FilterFunction, Ctx>&& f,
               unique_ptr<T> consumer)
        : Filter<T>(std::move(consumer)), f_(std::move(f), *this) {
      this->setConfigIdentity(cfg);
      this->serde_     = serde;
      this->env_       = env;
    }

    // --- make() — internal (codegen build path) ---

    template <typename F = _FilterFunction, typename Ctx = FilterImpl>
    static unique_ptr<FilterImpl<F>> make(StreamFunction<F, Ctx>&& f) {
      return unique_ptr<FilterImpl<F>>(new FilterImpl<F>(std::move(f)));
    }

    template <typename T, typename F = _FilterFunction, typename Ctx = FilterImpl>
    static unique_ptr<FilterImpl<F, T>> make(StreamFunction<F, Ctx>&& f, unique_ptr<T> consumer) {
      return unique_ptr<FilterImpl<F, T>>(new FilterImpl<F, T>(std::move(f), std::move(consumer)));
    }

    // --- make() — Go-aligned: from config + upstream serde + env ---

    template <typename F = _FilterFunction, typename Ctx = FilterImpl>
    static unique_ptr<FilterImpl<F>> make(const servicelib::config::FilterStreamConfig& cfg,
                                          serde::StreamSerde<_Tp>* serde,
                                          IRuntimeEnvironment* env,
                                          StreamFunction<F, Ctx>&& f) {
      return unique_ptr<FilterImpl<F>>(new FilterImpl<F>(cfg, serde, env, std::move(f)));
    }

    template <typename T, typename F = _FilterFunction, typename Ctx = FilterImpl>
    static unique_ptr<FilterImpl<F, T>> make(const servicelib::config::FilterStreamConfig& cfg,
                                             serde::StreamSerde<_Tp>* serde,
                                             IRuntimeEnvironment* env,
                                             StreamFunction<F, Ctx>&& f,
                                             unique_ptr<T> consumer) {
      return unique_ptr<FilterImpl<F, T>>(new FilterImpl<F, T>(cfg, serde, env, std::move(f), std::move(consumer)));
    }

    const std::string_view& getType() const override {
      if (f_.isInternalType()) {
        return Filter<_CCp>::getType();
      }
      return StreamBuilderContext::getType<decltype(*this)>();
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

    static auto build(FilterImpl& stream) {
      auto r = make(std::move(stream.f_));
      r->copySettings(stream);
      r->copyConsumerSettings(stream);
      return r;
    }

    template <typename T>
    static auto build(FilterImpl& stream, unique_ptr<T> consumer) {
      auto r = make(std::move(stream.f_), std::move(consumer));
      r->copySettings(stream);
      r->copyConsumerSettings(stream);
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

  template <typename _CCp>
  class FilterBuilder final {
    template <typename>
    friend class Filter;

   protected:
    template <typename _FilterFunction, typename Ctx>
    static auto build(StreamFunction<_FilterFunction, Ctx>&& f) {
      return FilterImpl<_FilterFunction, _CCp>::build(std::move(f));
    }

    template <typename _FilterFunction, typename Ctx>
    static auto build(unique_ptr<_CCp> consumer, StreamFunction<_FilterFunction, Ctx>&& f) {
      return FilterImpl<_FilterFunction, _CCp>::build(std::move(consumer), std::move(f));
    }
  };

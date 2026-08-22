// Part of class Stream<_Tp, _Cp, _Context> — included by stream.hpp
// Go analog: operators/merge.go — MakeMergeStream / MergeStream

  template <const size_t NLinks, typename _CCp = StreamConsumer<_Tp>>
  class Merge : public Stream<_Tp, _CCp, _Context> {
    template <typename, typename>
    friend class StreamExecutionEnvironment;
    template <typename, typename, typename>
    friend class Stream;
    friend class StreamBuilderContext;

   protected:
    template <typename T>
    explicit Merge(unique_ptr<T> consumer) : Stream<_Tp, T, _Context>(std::move(consumer)) {}

    Merge() = default;
    ~Merge() override = default;

    size_t buildTopology(StreamBuilderContext& ctx, size_t id, StreamBuilderContext::TIdsList* splitConsumerIds,
                         bool) override {
      auto& os = ctx.getOut();

      size_t nextid = id - 1;
      if (this->getId() == 0) {
        if (splitConsumerIds != nullptr) {
          splitConsumerIds->emplace_back(id);
        }
        ctx.buildTopology(*this, id);
        os << "auto &stream" << id << " = stream" << id - 1 << "t.getConsumer();" << std::endl;
        nextid = this->buildTopologyCommon(ctx, id, nullptr, false);
      }
      return nextid;
    }

    void verifyTopology(StreamVerifyContext& ctx) const override {
      if (!ctx.isProcessed(this->getBase())) {
        ctx.setProcessed(this->getBase());
        Stream<_Tp, _CCp, _Context>::verifyTopology(ctx);
      }
    }
  };

  template <typename _Merge, bool isMaster = false>
  class MergeLink : public StreamConsumer<_Tp>, public StreamBase {
    template <typename, typename>
    friend class StreamExecutionEnvironment;
    template <typename, typename, typename>
    friend class Stream;
    friend class StreamVerifyContext;
    friend class StreamBuilderContext;

   protected:
    std::conditional_t<isMaster, unique_ptr<_Merge>, _Merge&> consumer_;

    template <bool _isMaster = isMaster, typename = std::enable_if_t<_isMaster, void>>
    explicit MergeLink(unique_ptr<_Merge> consumer) noexcept : consumer_(std::move(consumer)) {}

    template <bool _isMaster = isMaster, typename = std::enable_if_t<!_isMaster, void>>
    explicit MergeLink(_Merge& consumer) noexcept : consumer_(consumer) {}

    ~MergeLink() override = default;

    bool hasConsumer() const noexcept override { return true; }

    _Merge& consumer() {
      if constexpr (isMaster) {
        return *consumer_;
      } else {
        return consumer_;
      }
    }

    _Merge& consumer() const {
      if constexpr (isMaster) {
        return *consumer_;
      } else {
        return consumer_;
      }
    }

    size_t buildTopology(StreamBuilderContext& ctx, size_t id, StreamBuilderContext::TIdsList* splitConsumerIds,
                         [[maybe_unused]] bool skip) override {
      auto& os = ctx.getOut();
      this->setId(id);
      ctx.buildTopology(*this, id);
      if (splitConsumerIds != nullptr) {
        splitConsumerIds->emplace_back(id);
      }

      os << "auto &stream" << id << "t = static_cast<" << this->getType() << "&>(stream" << id << ");" << std::endl;
      size_t nextid = consumer().buildTopology(ctx, id + 1, nullptr, false);
      os << "auto stream" << id << "c = stream" << id << "t.build(";
      os << "stream" << id << "t, ";
      if (consumer().getId() == 0) {
        throw StreamException("Build stream logic error. Merge link id isn't assigned.");
      }
      if constexpr (isMaster) {
        os << "std::move(stream" << consumer().getId() << "c));" << std::endl;
      } else {
        os << "stream" << consumer().getId() << "r);" << std::endl;
      }
      os << "auto &stream" << id << "r = *stream" << id << "c;" << std::endl;
      os << "stream" << id << "r.setId(" << id << ");" << std::endl;
      id = nextid;
      return nextid;
    }

    void verifyTopology(StreamVerifyContext& ctx) const override {
      ctx.verify(*this);
      consumer().verifyTopology(ctx);
    }

    void printTopology(TopologyPrinter& tp, std::unordered_set<size_t>& visited) const override {
      if (visited.find(this->getId()) == visited.end()) {
        visited.emplace(this->getId());
        if (hasConsumer()) {
          tp.printLink(tp.makeNode(*this), tp.makeNode(consumer()));
        }
        consumer().printTopology(tp, visited);
      }
    }

    const StreamBase& getConsumer() const override { return consumer().getBase(); }

    StreamBase& getConsumer() override { return consumer().getBase(); }

    const StreamBase& getBase() const noexcept override { return *this; }

    StreamBase& getBase() noexcept override { return *this; }

    std::string getCode() const override { return std::string(); }

   public:
    size_t getId() const noexcept override { return StreamBase::getId(); }

    size_t getConfigId() const noexcept override {
      return consumer().getConfigId();
    }

    const std::string& getName() const noexcept override { return StreamBase::getName(); }
  };

  template <const size_t NLinks = 2, typename _CCp = StreamConsumer<_Tp>>
  class MergeImpl final : public Merge<NLinks, _CCp> {
    template <typename, typename>
    friend class StreamExecutionEnvironment;
    template <typename, typename, typename>
    friend class Stream;
    friend class StreamBuilderContext;

    decltype(_Context::getExecutionEnvironment())& context_{_Context::getExecutionEnvironment()};

   public:
    void consume(MessageContext ctx, Payload<_Tp> payload) override {
      [[maybe_unused]] auto activeSpan =
          tracing::StartStreamSpan(ctx, *this, "stream.merge");
      if (this->hasConsumer()) {
        context_.template consume<_Tp>(
            std::move(ctx), *this, *this->consumer(), std::move(payload));
      }
    }

   protected:
    template <typename T>
    explicit MergeImpl(unique_ptr<T> consumer) noexcept : Merge<NLinks, T>(std::move(consumer)) {}

    MergeImpl() noexcept = default;

    // Go-aligned: config id + upstream serde (same type _Tp) + env
    MergeImpl(const servicelib::config::MergeStreamConfig& cfg,
              serde::StreamSerde<_Tp>* serde,
              IRuntimeEnvironment* env) noexcept
        : Merge<NLinks, _CCp>() {
      this->setConfigIdentity(cfg);
      this->serde_     = serde;
      this->env_       = env;
    }

    template <typename T>
    MergeImpl(const servicelib::config::MergeStreamConfig& cfg,
              serde::StreamSerde<_Tp>* serde,
              IRuntimeEnvironment* env,
              unique_ptr<T> consumer) noexcept
        : Merge<NLinks, T>(std::move(consumer)) {
      this->setConfigIdentity(cfg);
      this->serde_     = serde;
      this->env_       = env;
    }

    static unique_ptr<MergeImpl> make() { return unique_ptr<MergeImpl>(new MergeImpl()); }

    template <typename T>
    static auto make(unique_ptr<T> consumer) {
      return unique_ptr<MergeImpl<NLinks, T>>(new MergeImpl<NLinks, T>(std::move(consumer)));
    }

    static unique_ptr<MergeImpl> make(const servicelib::config::MergeStreamConfig& cfg,
                                      serde::StreamSerde<_Tp>* serde,
                                      IRuntimeEnvironment* env) {
      return unique_ptr<MergeImpl>(new MergeImpl(cfg, serde, env));
    }

    template <typename T>
    static auto build(MergeImpl& stream, unique_ptr<T> consumer) {
      auto r = make(std::move(consumer));
      r->copySettings(stream);
      r->copyConsumerSettings(stream);
      return r;
    }

    // Go-aligned build: pass config + serde + env alongside streams
    template <typename C1, typename... C>
    static auto& build(C1& stream, C&... streams) {
      auto merge = make();
      auto& rmerge = *merge;
      stream.setConsumer(MergeLinkImpl<sizeof...(C) + 1, true, MergeImpl>::make(std::move(merge)));
      (streams.setConsumer(MergeLinkImpl<sizeof...(C) + 1, false, MergeImpl>::make(rmerge)), ...);
      return rmerge;
    }

    template <typename C1, typename... C>
    static auto& build(const servicelib::config::MergeStreamConfig& cfg,
                       serde::StreamSerde<_Tp>* serde,
                       IRuntimeEnvironment* env,
                       C1& stream,
                       C&... streams) {
      auto merge = make(cfg, serde, env);
      auto& rmerge = *merge;
      stream.setConsumer(MergeLinkImpl<sizeof...(C) + 1, true, MergeImpl>::make(std::move(merge)));
      (connectAdditionalSource(streams, rmerge), ...);
      return rmerge;
    }

    template <typename Source>
    static void connectAdditionalSource(Source& source, MergeImpl& merge) {
      if constexpr (requires {
                      source.setConsumer(
                          MergeLinkImpl<NLinks, false, MergeImpl>::make(merge));
                    }) {
        source.setConsumer(
            MergeLinkImpl<NLinks, false, MergeImpl>::make(merge));
      } else {
        // Derived outputs such as Process::getErrorStream() are virtual
        // branches and do not own a topology node. They point directly at the
        // same merge consumer, matching Go's GetErrorStream()+Merge graph.
        source.setConsumer(merge);
      }
    }

    const std::string_view& getType() const override { return StreamBuilderContext::getType<decltype(*this)>(); }
  };

  template <const size_t NLinks, bool isMaster, typename _Merge>
  class MergeLinkImpl final : public MergeLink<_Merge, isMaster> {
    template <typename, typename>
    friend class StreamExecutionEnvironment;
    template <typename, typename, typename>
    friend class Stream;

   public:
    void consume(MessageContext ctx, Payload<_Tp> payload) override {
      if (this->hasConsumer()) {
        this->consumer().consume(std::move(ctx), std::move(payload));
      }
    }

   protected:
    template <bool _isMaster = isMaster, typename = std::enable_if_t<_isMaster, void>>
    explicit MergeLinkImpl(unique_ptr<_Merge> consumer) noexcept : MergeLink<_Merge, isMaster>(std::move(consumer)) {}

    template <bool _isMaster = isMaster, typename = std::enable_if_t<!_isMaster, void>>
    explicit MergeLinkImpl(_Merge& consumer) noexcept : MergeLink<_Merge, isMaster>(consumer) {}

    template <typename T>
    static std::enable_if_t<!isMaster, unique_ptr<MergeLinkImpl<NLinks, isMaster, T>>> make(T& consumer) {
      return unique_ptr<MergeLinkImpl<NLinks, isMaster, T>>(new MergeLinkImpl<NLinks, isMaster, T>(consumer));
    }

    template <typename T>
    static std::enable_if_t<isMaster, unique_ptr<MergeLinkImpl<NLinks, isMaster, T>>> make(unique_ptr<T> consumer) {
      return unique_ptr<MergeLinkImpl<NLinks, isMaster, T>>(
          new MergeLinkImpl<NLinks, isMaster, T>(std::move(consumer)));
    }

    const std::string_view& getType() const override { return StreamBuilderContext::getType<decltype(*this)>(); }

    template <typename T>
    static auto build(MergeLinkImpl&, unique_ptr<T> consumer)
        -> std::enable_if_t<isMaster, decltype(make(std::move(consumer)))> {
      return make(std::move(consumer));
    }

    template <typename T>
    static auto build(MergeLinkImpl&, T& consumer) -> std::enable_if_t<!isMaster, decltype(make(consumer))> {
      return make(consumer);
    }
  };

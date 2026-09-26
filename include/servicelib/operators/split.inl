// Part of class Stream<_Tp, _Cp, _Context> — included by stream.hpp

  template <typename _TTp, typename _CCp, typename _Ctx>
  class SplitLink;

  template <typename T>
  struct stream_args_helper {
    template <typename _T, typename _C, typename _Ctx>
    static auto streamTypes(Stream<_T, _C, _Ctx>&)
        -> std::type_identity<SplitLink<_T, _C, _Ctx>>;

    using Stream =
        typename decltype(streamTypes(std::declval<T&>()))::type;
  };

  template <typename T>
  class SplitBase : protected T, public StreamConsumer<_Tp>, public StreamBase {
    template <typename, typename, typename>
    friend class Stream;
    friend class StreamVerifyContext;
    friend class StreamBuilderContext;

   public:
    template <std::size_t N>
    auto get() noexcept
        -> typename stream_args_helper<std::tuple_element_t<N, T>>::Stream& {
      return std::get<N>(static_cast<T&>(*this));
    }

    template <std::size_t N>
    auto get() const noexcept
        -> typename stream_args_helper<std::tuple_element_t<N, T>>::Stream& {
      return std::get<N>(static_cast<const T&>(*this));
    }

    static constexpr std::size_t size() { return std::tuple_size_v<T>; }

    size_t getId() const noexcept override { return StreamBase::getId(); }

    const std::string& getName() const noexcept override { return StreamBase::getName(); }

   protected:
    template <std::size_t I>
    using BranchConsumer = std::remove_reference_t<decltype(
        std::declval<std::tuple_element_t<I, T>>().callerTarget())>;

    template <std::size_t I>
    using BranchCaller = std::conditional_t<
        std::is_same_v<BranchConsumer<I>, StreamConsumer<_Tp>>, Caller<_Tp>*,
        std::optional<Caller<_Tp, BranchConsumer<I>>>>;

    template <std::size_t... I>
    static auto dispatcherTypes(std::index_sequence<I...>)
        -> std::tuple<BranchCaller<I>...>;

    using Dispatchers = decltype(dispatcherTypes(
        std::make_index_sequence<std::tuple_size_v<T>>{}));
    Dispatchers dispatchers_{};
    std::array<bool, std::tuple_size_v<T>> asyncBranches_{};

    template <std::size_t I>
    void prepareBranch() {
      auto& branch = std::get<I>(static_cast<T&>(*this));
      auto& consumer = branch.callerTarget();
      auto& caller = std::get<I>(dispatchers_);
      auto& environment = _Context::getExecutionEnvironment();
      if constexpr (std::is_same_v<BranchConsumer<I>, StreamConsumer<_Tp>>) {
        caller = environment.template prepareCaller<_Tp>(
            *this, consumer, branch.getName());
      } else {
        caller.emplace(environment.template prepareTypedCaller<_Tp>(
            *this, consumer, branch.getName()));
      }
      asyncBranches_[I] = caller->isAsync();
    }

    template <std::size_t... I>
    void prepareBranches(std::index_sequence<I...>) {
      (prepareBranch<I>(), ...);
    }

    template <std::size_t... I>
    void dispatchBranches(MessageContext context, Payload<_Tp> payload,
                          std::index_sequence<I...>) {
      std::size_t remaining = sizeof...(I);
      // Two stable passes preserve async-first ordering without sorting away
      // the branch types or scheduling any extra work.
      for (const bool async : {true, false}) {
        (([&] {
          if (asyncBranches_[I] != async) return;
          auto& caller = *std::get<I>(dispatchers_);
          if (--remaining == 0) {
            caller.consume(std::move(context), std::move(payload));
          } else {
            caller.consume(context, payload);
          }
        }()), ...);
      }
    }

    explicit SplitBase(const T& t) : T(t) {}
    ~SplitBase() override = default;

    void verifyTopology(StreamVerifyContext& ctx) const override {
      ctx.verify(*this);
      std::apply([&ctx](auto&&... consumer) { (consumer.verifyTopology(ctx), ...); }, static_cast<const T&>(*this));
    }

    void printTopology(TopologyPrinter& tp, std::unordered_set<size_t>& visited) const override {
      std::apply([&tp, &visited, this](auto&&... consumer) { (this->printTopology(consumer, tp, visited), ...); },
                 static_cast<const T&>(*this));
    }

    const StreamBase& getConsumer() const override {
      throw StreamException("getConsumer() call is incorrect for Split logic");
    }

    StreamBase& getConsumer() override { throw StreamException("getConsumer() call is incorrect for Split logic"); }

    const StreamBase& getBase() const noexcept override { return *this; }

    StreamBase& getBase() noexcept override { return *this; }

    bool hasConsumer() const noexcept override {
      return std::apply(
          [](auto&&... consumer) {
            bool f = false;
            ((f = f || consumer.hasConsumer()), ...);
            return f;
          },
          static_cast<const T&>(*this));
    }

    std::string getCode() const override { return std::string(); }

    size_t buildTopology(StreamBuilderContext& ctx, size_t id, StreamBuilderContext::TIdsList* splitConsumerIds,
                         bool) override {
      ctx.buildTopology(*this, id);
      this->setId(id);
      auto& os = ctx.getOut();

      std::array<size_t, std::tuple_size_v<T>> ids;
      size_t next_id = id;

      StreamBuilderContext::TIdsList endLeafIds;
      StreamBuilderContext::TIdsList* _splitConsumerIds = splitConsumerIds == nullptr ? &endLeafIds : splitConsumerIds;

      os << "auto &stream" << id << "t = static_cast<" << this->getType() << "&>(stream" << id << ");" << std::endl;

      std::apply(
          [&ctx, &ids, idx = size_t{0}, &next_id, id, _splitConsumerIds, this](auto&&... consumer) mutable {
            ((ids[idx] = next_id + (consumer.hasConsumer() ? 2 : 1),
              next_id = this->buildTopology(consumer, ctx, idx, id, next_id + 1, _splitConsumerIds), ++idx),
             ...);
          },
          static_cast<const T&>(*this));

      if (splitConsumerIds == nullptr) {
        os << "auto stream" << id << "c = stream" << id << "t.build(stream" << id << "t";
        std::for_each(_splitConsumerIds->begin(), _splitConsumerIds->end(),
                      [&os](const auto& id) mutable { os << ", std::move(stream" << id << "c)"; });
        os << ");" << std::endl;
        os << "auto &stream" << id << "r = *stream" << id << "c;" << std::endl;
        os << "stream" << id << "r.setId(" << id << ");" << std::endl;
      }

      // Split dispatches directly to multiple branch links rather than
      // through Stream::consumer_, so buildTopologyCommon() cannot prepare
      // these edges for us. Prepare them while userver still permits metric
      // registration; lazy preparation from the first request is too late.
      prepareBranches(std::make_index_sequence<std::tuple_size_v<T>>{});
      return next_id;
    }

   private:
    template <typename C, typename = void>
    struct has_build_topology_common : std::false_type {};

    template <typename C>
    struct has_build_topology_common<C, std::void_t<decltype(std::declval<C>().buildTopologyCommon(
                                            std::declval<StreamBuilderContext&>(), std::declval<size_t>(),
                                            std::declval<StreamBuilderContext::TIdsList*>(), std::declval<bool>()))>>
        : std::true_type {};

    template <typename C>
    size_t buildTopology(C& consumer, StreamBuilderContext& ctx, size_t idx, size_t curid, size_t id,
                         StreamBuilderContext::TIdsList* splitConsumerIds) {
      auto& os = ctx.getOut();
      os << "auto &stream" << id << " = stream" << curid << "t.get<" << idx << ">();" << std::endl;
      if constexpr (has_build_topology_common<C>::value) {
        return consumer.buildTopologyCommon(ctx, id, splitConsumerIds, consumer.hasConsumer());
      }
      return 0;
    }

    template <typename C>
    void printTopology(C& consumer, TopologyPrinter& tp, std::unordered_set<size_t>& visited) const {
      tp.printLink(tp.makeNode(*this), tp.makeNode(consumer));
      consumer.printTopology(tp, visited);
    }
  };

  template <typename MakeType>
  struct make_protected {
    static unique_ptr<MakeType> make() { return MakeType::make(); }
  };

  template <typename _TTp, typename _CCp, typename _Ctx>
  class SplitLink : public Stream<_TTp, _CCp, _Ctx> {
    template <typename, typename>
    friend class StreamExecutionEnvironment;
    template <typename>
    friend struct make_protected;
    template <typename>
    friend class SplitBase;
    friend class StreamBuilderContext;

   protected:
    SplitLink() = default;
    ~SplitLink() override = default;

    auto& callerTarget() {
      if (!this->consumer()) {
        throw StreamException("Split branch must not have an empty consumer.");
      }
      return Stream::callerConsumer(*this->consumer());
    }
  };

  template <typename _TTp, typename _CCp, typename _Ctx>
  class SplitLinkImpl final : public SplitLink<_TTp, _CCp, _Ctx> {
    template <typename, typename>
    friend class StreamExecutionEnvironment;
    template <typename>
    friend struct make_protected;
    template <typename>
    friend class SplitBase;
    friend class StreamBuilderContext;

   protected:
    SplitLinkImpl() = default;

    static unique_ptr<SplitLinkImpl> make() { return unique_ptr<SplitLinkImpl>(new SplitLinkImpl()); }

    const std::string_view& getType() const override { return StreamBuilderContext::getType<decltype(*this)>(); }

    static auto build(SplitLinkImpl&) { return make(); }

    size_t buildTopology(StreamBuilderContext& ctx, size_t id, StreamBuilderContext::TIdsList* splitConsumerIds,
                         bool skip) override {
      ctx.buildTopology(*this, id);
      return this->buildTopologyCommon(ctx, id, splitConsumerIds, skip);
    }
  };

  template <typename T, typename V = detail::tuple_to_variant_uptr<T, unique_ptr>, std::size_t N = std::tuple_size_v<T>>
  class Split : protected std::array<V, N>, public SplitBase<T> {
    template <typename, typename>
    friend class StreamExecutionEnvironment;
    template <typename, typename, typename>
    friend class Stream;
    friend class StreamBuilderContext;

   protected:
    template <typename = std::enable_if_t<std::is_same_v<_Cp, StreamConsumer<_Tp>>, void>>
    Split()
        : std::array<V, N>{detail::make_array_variant<make_protected<SplitLinkImpl<_Tp, _Cp, _Context>>, V>(
              std::make_index_sequence<N>{})},
          SplitBase<T>(detail::make_tuple<unique_ptr, T, N>(*static_cast<std::array<V, N>*>(this))) {}

    template <typename... C>
    Split(const T& t, unique_ptr<C>... consumers) : std::array<V, N>{std::move(consumers)...}, SplitBase<T>(t) {}

    template <typename... C>
    explicit Split(unique_ptr<C>... consumers)
        : std::array<V, N>{std::move(consumers)...},
          SplitBase<T>(detail::make_tuple<unique_ptr, T, N>(
              *static_cast<std::array<V, N>*>(this))) {
      static_assert(sizeof...(C) == N);
    }

    ~Split() override = default;

   public:
    using SplitBase<T>::get;
  };

  template <typename T>
  class SplitImpl final : public Split<T> {
    template <typename, typename>
    friend class StreamExecutionEnvironment;
    template <typename, typename, typename>
    friend class Stream;
    friend class StreamBuilderContext;

   public:
    void consume(MessageContext ctx, Payload<_Tp> payload) override {
      tracing::ActiveSpan activeSpan;
      if (this->getStreamTracer() && tracing::SamplingEnabled(ctx)) {
        activeSpan = tracing::StartStreamSpan(ctx, *this, "stream.split");
      }
      this->dispatchBranches(std::move(ctx), std::move(payload),
                             std::make_index_sequence<std::tuple_size_v<T>>{});
    }

   protected:
    SplitImpl() = default;

    // Go-aligned: config id + upstream serde (same type _Tp) + env
    explicit SplitImpl(const servicelib::config::SplitStreamConfig& cfg,
                       serde::StreamSerde<_Tp>* serde,
                       IRuntimeEnvironment* env) noexcept
        : Split<T>() {
      configure(cfg, serde, env);
    }

    template <typename... C>
    SplitImpl(const servicelib::config::SplitStreamConfig& cfg,
              serde::StreamSerde<_Tp>* serde, IRuntimeEnvironment* env,
              unique_ptr<C>... consumers)
        : Split<T>(std::move(consumers)...) {
      configure(cfg, serde, env);
    }

    void configure(const servicelib::config::SplitStreamConfig& cfg,
                   serde::StreamSerde<_Tp>* serde, IRuntimeEnvironment* env) {
      this->setConfigIdentity(cfg);
      this->serde_     = serde;
      this->setEnv(env);
      // Go SplitLink.GetRuntimeEnvironment delegates to its parent Split.
      // C++ links are independent StreamBase nodes, so propagate the same
      // environment explicitly. Operators created from a branch then keep
      // runtime-config reload and telemetry semantics identical to Go.
      std::apply(
          [env, serde, &cfg, index = std::size_t{0}](auto&... link) mutable {
            ((link.serde_ = serde,
              link.setEnv(env),
              link.setName(
                  (cfg.name + "SplitLink" + std::to_string(index++)).c_str())),
             ...);
          },
          static_cast<T&>(*this));
    }

    template <typename... C>
    SplitImpl(const T& t, unique_ptr<C>... consumers) : Split<T>(t, std::move(consumers)...) {}

    static unique_ptr<SplitImpl> make() { return unique_ptr<SplitImpl>(new SplitImpl()); }

    template <typename... C>
    static auto make(unique_ptr<C>... consumers) {
      auto t = std::tie(*consumers...);
      return unique_ptr<SplitImpl<decltype(t)>>(new SplitImpl<decltype(t)>(t, std::move(consumers)...));
    }

    static unique_ptr<SplitImpl> make(const servicelib::config::SplitStreamConfig& cfg,
                                      serde::StreamSerde<_Tp>* serde,
                                      IRuntimeEnvironment* env) {
      return unique_ptr<SplitImpl>(new SplitImpl(cfg, serde, env));
    }

    const std::string_view& getType() const override { return StreamBuilderContext::getType<decltype(*this)>(); }

    template <typename... C>
    static auto build(SplitImpl& stream, unique_ptr<C>... consumers) {
      auto r = make(std::move(consumers)...);
      r->copySettings(stream);
      r->copyConsumerSettings(stream);
      return r;
    }

  };

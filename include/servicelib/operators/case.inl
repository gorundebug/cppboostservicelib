// Part of class Stream<_Tp, _Cp, _Context> — included by stream.hpp
// Go analog: operators/case.go — MakeCaseStream / CaseStream + WhenStream
//
// CaseStream dispatches each value to exactly ONE of N WhenLink branches,
// selected by SwitchFunction(v) -> size_t index.
// Contrast with Split which broadcasts to ALL branches.

// WhenLink: a single case branch — same-type pass-through stream.
// Analogous to SplitLink; user chains map/filter/etc downstream from each
// branch.
template <typename _CCp, typename _Ctx>
class WhenLink : public Stream<_Tp, _CCp, _Ctx> {
  template <typename, typename>
  friend class StreamExecutionEnvironment;
  template <typename>
  friend struct make_protected;
  template <typename>
  friend class CaseBase;
  friend class StreamBuilderContext;

 public:
  void configure(const servicelib::config::WhenStreamConfig& cfg,
                 serde::StreamSerde<_Tp>* serde,
                 IRuntimeEnvironment* environment) {
    this->setConfigIdentity(cfg);
    this->serde_ = serde;
    this->env_ = environment;
  }

 protected:
  WhenLink() = default;
  ~WhenLink() override = default;
};

template <typename _CCp, typename _Ctx>
class WhenLinkImpl final : public WhenLink<_CCp, _Ctx> {
  template <typename, typename>
  friend class StreamExecutionEnvironment;
  template <typename>
  friend struct make_protected;
  template <typename>
  friend class CaseBase;
  friend class StreamBuilderContext;

 public:
  WhenLinkImpl() = default;

  static unique_ptr<WhenLinkImpl> make() {
    return unique_ptr<WhenLinkImpl>(new WhenLinkImpl());
  }

  const std::string_view& getType() const override {
    return StreamBuilderContext::getType<decltype(*this)>();
  }

  static auto build(WhenLinkImpl&) { return make(); }

  size_t buildTopology(StreamBuilderContext& ctx, size_t id,
                       StreamBuilderContext::TIdsList* splitConsumerIds,
                       bool skip) override {
    ctx.buildTopology(*this, id);
    return this->buildTopologyCommon(ctx, id, splitConsumerIds, skip);
  }
};

template <typename T>
struct when_stream_args_helper {
  template <typename C, typename Ctx>
  static auto streamTypes(WhenLink<C, Ctx>&)
      -> std::type_identity<WhenLink<C, Ctx>>;

  using Stream = typename decltype(streamTypes(std::declval<T&>()))::type;
};

// CaseBase<T>: holds N WhenLink branches and dispatches consume to one of them.
// T = tuple<WhenLinkImpl<...>&, WhenLinkImpl<...>&, ...> (N references into the
// array).
template <typename T>
class CaseBase : protected T, public StreamConsumer<_Tp>, public StreamBase {
  template <typename, typename, typename>
  friend class Stream;
  friend class StreamVerifyContext;
  friend class StreamBuilderContext;

 public:
  template <std::size_t N>
  auto get() noexcept -> typename when_stream_args_helper<
      std::tuple_element_t<N, T>>::Stream& {
    return std::get<N>(static_cast<T&>(*this));
  }

  template <std::size_t N>
  auto get() const noexcept -> typename when_stream_args_helper<
      std::tuple_element_t<N, T>>::Stream& {
    return std::get<N>(static_cast<const T&>(*this));
  }

  static constexpr std::size_t size() { return std::tuple_size_v<T>; }

  size_t getId() const noexcept override { return StreamBase::getId(); }

  const std::string& getName() const noexcept override {
    return StreamBase::getName();
  }

 protected:
  std::array<std::function<void(MessageContext, Payload<_Tp>)>,
             std::tuple_size_v<T>>
      branchDispatchers_{};

  explicit CaseBase(const T& t) : T(t) {}
  ~CaseBase() override = default;

  void verifyTopology(StreamVerifyContext& ctx) const override {
    ctx.verify(*this);
    std::apply(
        [&ctx](auto&&... consumer) { (consumer.verifyTopology(ctx), ...); },
        static_cast<const T&>(*this));
  }

  void printTopology(TopologyPrinter& tp,
                     std::unordered_set<size_t>& visited) const override {
    std::apply(
        [&tp, &visited, this](auto&&... consumer) {
          (this->printTopologyBranch(consumer, tp, visited), ...);
        },
        static_cast<const T&>(*this));
  }

  const StreamBase& getConsumer() const override {
    throw StreamException("getConsumer() is not valid for Case stream");
  }

  StreamBase& getConsumer() override {
    throw StreamException("getConsumer() is not valid for Case stream");
  }

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

  size_t buildTopology(StreamBuilderContext& ctx, size_t id,
                       StreamBuilderContext::TIdsList* splitConsumerIds,
                       bool) override {
    ctx.buildTopology(*this, id);
    this->setId(id);
    auto& os = ctx.getOut();

    size_t next_id = id;
    StreamBuilderContext::TIdsList endLeafIds;
    StreamBuilderContext::TIdsList* _splitConsumerIds =
        splitConsumerIds == nullptr ? &endLeafIds : splitConsumerIds;

    os << "auto &stream" << id << "t = static_cast<" << this->getType()
       << "&>(stream" << id << ");" << std::endl;

    std::apply(
        [&ctx, idx = size_t{0}, &next_id, id, _splitConsumerIds,
         this](auto&&... consumer) mutable {
          ((next_id = this->buildTopologyBranch(
                consumer, ctx, idx++, id, next_id + 1, _splitConsumerIds)),
           ...);
        },
        static_cast<const T&>(*this));

    if (splitConsumerIds == nullptr) {
      os << "auto stream" << id << "c = stream" << id << "t.build(stream" << id
         << "t";
      std::for_each(_splitConsumerIds->begin(), _splitConsumerIds->end(),
                    [&os](const auto& eid) {
                      os << ", std::move(stream" << eid << "c)";
                    });
      os << ");" << std::endl;
      os << "auto &stream" << id << "r = *stream" << id << "c;" << std::endl;
      os << "stream" << id << "r.setId(" << id << ");" << std::endl;
    }
    std::apply(
        [this, index = std::size_t{0}](auto&... consumer) mutable {
          (([&] {
             auto* caller = _Context::getExecutionEnvironment()
                                .template prepareCaller<_Tp>(
                                    *this, consumer, consumer.getName());
             branchDispatchers_[index++] =
                 [caller](MessageContext context, Payload<_Tp> payload) {
                   caller->consume(std::move(context), std::move(payload));
                 };
           }()),
           ...);
        },
        static_cast<T&>(*this));
    return next_id;
  }

 private:
  template <typename C, typename = void>
  struct has_build_topology_common : std::false_type {};

  template <typename C>
  struct has_build_topology_common<
      C, std::void_t<decltype(std::declval<C>().buildTopologyCommon(
             std::declval<StreamBuilderContext&>(), std::declval<size_t>(),
             std::declval<StreamBuilderContext::TIdsList*>(),
             std::declval<bool>()))>> : std::true_type {};

  template <typename C>
  size_t buildTopologyBranch(C& consumer, StreamBuilderContext& ctx, size_t idx,
                             size_t curid, size_t id,
                             StreamBuilderContext::TIdsList* splitConsumerIds) {
    auto& os = ctx.getOut();
    os << "auto &stream" << id << " = stream" << curid << "t.get<" << idx
       << ">();" << std::endl;
    if constexpr (has_build_topology_common<C>::value) {
      return consumer.buildTopologyCommon(ctx, id, splitConsumerIds,
                                          consumer.hasConsumer());
    }
    return 0;
  }

  template <typename C>
  void printTopologyBranch(C& consumer, TopologyPrinter& tp,
                           std::unordered_set<size_t>& visited) const {
    tp.printLink(tp.makeNode(*this), tp.makeNode(consumer));
    consumer.printTopology(tp, visited);
  }
};

// Case<T, V, N>: owns the WhenLinkImpl array, exposes them via CaseBase<T>.
template <typename T, typename V = detail::tuple_to_variant_uptr<T, unique_ptr>,
          std::size_t N = std::tuple_size_v<T>>
class Case : protected std::array<V, N>, public CaseBase<T> {
  template <typename, typename, typename>
  friend class Stream;
  friend class StreamBuilderContext;

 protected:
  template <typename = std::enable_if_t<
                std::is_same_v<_Cp, StreamConsumer<_Tp>>, void>>
  Case()
      : std::array<V, N>{detail::make_array_variant<
            make_protected<WhenLinkImpl<StreamConsumer<_Tp>, _Context>>, V>(
            std::make_index_sequence<N>{})},
        CaseBase<T>(detail::make_tuple<unique_ptr, T, N>(
            *static_cast<std::array<V, N>*>(this))) {}

  template <typename... C>
  Case(const T& t, unique_ptr<C>... consumers)
      : std::array<V, N>{std::move(consumers)...}, CaseBase<T>(t) {}

  ~Case() override = default;

  template <std::size_t I>
  typename std::tuple_element<I, T>::type& get() const noexcept {
    return std::get<I>(*static_cast<const CaseBase<T>*>(this));
  }
};

// CaseImpl<T, SwitchFunction>: dispatches to one branch per value.
// SwitchFunction: (const _Tp&) -> size_t  (0-based branch index)
template <typename T, typename SwitchFunction>
class CaseImpl final : public Case<T> {
  template <typename, typename, typename>
  friend class Stream;
  friend class StreamBuilderContext;

  static constexpr std::size_t NBranches = std::tuple_size_v<T>;

  StreamFunction<SwitchFunction, CaseImpl> f_;
 public:
  void consume(MessageContext ctx, Payload<_Tp> payload) override {
    [[maybe_unused]] auto activeSpan =
        tracing::StartStreamSpan(ctx, *this, "stream.case");
    size_t idx = static_cast<size_t>(f_(ctx, *this, payload.get()));
    if (idx < NBranches && this->branchDispatchers_[idx]) {
      this->branchDispatchers_[idx](std::move(ctx), std::move(payload));
    }
  }

 protected:
  CaseImpl() noexcept : Case<T>(), f_(this) {}

  // Internal: for build(SwitchFunction&&) from codegen path
  template <typename Ctx = CaseImpl>
  explicit CaseImpl(StreamFunction<SwitchFunction, Ctx>&& f) noexcept
      : Case<T>(), f_(std::move(f), *this) {}

  // Go-aligned: config id + upstream serde (same type _Tp) + env
  template <typename Ctx = CaseImpl>
  CaseImpl(const servicelib::config::CaseStreamConfig& cfg,
           serde::StreamSerde<_Tp>* serde, IRuntimeEnvironment* env,
           StreamFunction<SwitchFunction, Ctx>&& f) noexcept
      : Case<T>(), f_(std::move(f), *this) {
    this->setConfigIdentity(cfg);
    this->serde_ = serde;
    this->env_ = env;
  }

  template <typename... C>
  CaseImpl(const T& t, unique_ptr<C>... consumers) noexcept
      : Case<T>(t, std::move(consumers)...), f_(this) {}

  template <typename F = SwitchFunction, typename Ctx = CaseImpl>
  static unique_ptr<CaseImpl> make(StreamFunction<F, Ctx>&& f) {
    return unique_ptr<CaseImpl>(new CaseImpl(std::move(f)));
  }

  template <typename Ctx>
  static unique_ptr<CaseImpl> make(
      const servicelib::config::CaseStreamConfig& cfg,
      serde::StreamSerde<_Tp>* serde, IRuntimeEnvironment* env,
      StreamFunction<SwitchFunction, Ctx>&& f) {
    return unique_ptr<CaseImpl>(new CaseImpl(cfg, serde, env, std::move(f)));
  }

  template <typename... C>
  static auto make(unique_ptr<C>... consumers) {
    auto t = std::tie(*consumers...);
    return unique_ptr<CaseImpl<decltype(t), SwitchFunction>>(
        new CaseImpl<decltype(t), SwitchFunction>(t, std::move(consumers)...));
  }

  const std::string_view& getType() const override {
    return StreamBuilderContext::getType<decltype(*this)>();
  }

  template <typename... C>
  static auto build(CaseImpl& stream, unique_ptr<C>... consumers) {
    auto r = make(std::move(consumers)...);
    r->copySettings(stream);
    r->copyConsumerSettings(stream);
    return r;
  }

};

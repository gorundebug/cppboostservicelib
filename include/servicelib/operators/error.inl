// Part of class Stream<_Tp, _Cp, _Context> — included by stream.hpp.
// Go analog: operators/error.go — virtual error stream owned by operators
// that have a secondary error output.
//
// ErrorStream<E> is a full Stream<E, StreamConsumer<E>, _Context> (like Go's
// ErrorStream[T], which embeds ConsumedStream[T] and therefore implements the
// same interface as any other stream) so it can be used directly as a source
// for .map()/.filter()/.merge()/etc., not just as a plain consumer target.

template <typename E, typename Consumer>
class ErrorLinkImpl final : public StreamConsumer<E>, public StreamBase {
  template <typename, typename>
  friend class StreamExecutionEnvironment;
  template <typename, typename, typename>
  friend class Stream;

  Consumer& consumer_;

 public:
  size_t getId() const noexcept override { return StreamBase::getId(); }

  size_t getConfigId() const noexcept override {
    return consumer_.getConfigId();
  }

  const std::string& getName() const noexcept override {
    return StreamBase::getName();
  }

  void consume(MessageContext ctx, Payload<E> payload) override {
    consumer_.consume(std::move(ctx), std::move(payload));
  }

 protected:
  explicit ErrorLinkImpl(Consumer& consumer) noexcept : consumer_(consumer) {}

  bool hasConsumer() const noexcept override { return true; }

  size_t buildTopology(StreamBuilderContext& ctx, size_t id, StreamBuilderContext::TIdsList* splitConsumerIds,
                       [[maybe_unused]] bool skip) override {
    auto& os = ctx.getOut();
    this->setId(id);
    ctx.buildTopology(*this, id);
    if (splitConsumerIds != nullptr) {
      splitConsumerIds->emplace_back(id);
    }

    os << "auto &stream" << id << "t = static_cast<" << this->getType() << "&>(stream" << id << ");" << std::endl;
    size_t nextid = id;
    if (consumer_.getId() == 0) {
      nextid = consumer_.buildTopology(ctx, id + 1, nullptr, false);
    }
    os << "auto stream" << id << "c = stream" << id << "t.build(";
    os << "stream" << id << "t, ";
    os << "stream" << consumer_.getId() << "r);" << std::endl;
    os << "auto &stream" << id << "r = *stream" << id << "c;" << std::endl;
    os << "stream" << id << "r.setId(" << id << ");" << std::endl;
    return nextid;
  }

  void verifyTopology(StreamVerifyContext& ctx) const override { ctx.verify(*this); }

  void printTopology(TopologyPrinter& tp, std::unordered_set<size_t>& visited) const override {
    if (visited.find(this->getId()) == visited.end()) {
      visited.emplace(this->getId());
      tp.printLink(tp.makeNode(*this), tp.makeNode(consumer_));
      consumer_.printTopology(tp, visited);
    }
  }

  const StreamBase& getBase() const noexcept override { return *this; }

  StreamBase& getBase() noexcept override { return *this; }

  std::string getCode() const override { return std::string(); }

  const std::string_view& getType() const override {
    return StreamBuilderContext::getType<decltype(*this)>();
  }

  const StreamBase& getConsumer() const override { return consumer_.getBase(); }

  StreamBase& getConsumer() override { return consumer_.getBase(); }

  static unique_ptr<ErrorLinkImpl> make(Consumer& consumer) {
    return unique_ptr<ErrorLinkImpl>(new ErrorLinkImpl(consumer));
  }
};

template <typename E>
class ErrorStream final : public Stream<E, StreamConsumer<E>, _Context> {
  using ErrorBase = Stream<E, StreamConsumer<E>, _Context>;
  std::atomic<std::int64_t> topologyCount_{0};

 public:
  ErrorStream() = default;
  ~ErrorStream() override = default;

  // Go: MakeErrorStream[E](id, env) — always resolves a fresh serde for E,
  // exactly like every other root-typed ConsumedStream[T] in Go. Called
  // explicitly (rather than baked into the constructor) so operators whose
  // own constructors don't yet have a config/env available (e.g. the
  // function-only constructors used to rebuild a stream in place) can defer
  // it, matching how the rest of this class's own config id/env are set.
  void configure(size_t configId, std::string name, IRuntimeEnvironment* environment) {
    this->setConfigId(configId);
    this->setName(name.c_str());
    this->setEnv(environment);
    this->resolveDefaultSerde();
  }

  // Attaches an already-owned-elsewhere stream as this error stream's
  // consumer without taking ownership of it (matching Go's GC-free aliasing —
  // e.g. .merge()'s connectAdditionalSource fallback, or an explicit
  // Process::setErrorConsumer-style external wiring).
  template <typename Downstream>
  void setConsumer(Downstream& consumer) {
    ErrorBase::setConsumer(
        typename StreamBase::template unique_ptr<ErrorLinkImpl<E, Downstream>>(
            new ErrorLinkImpl<E, Downstream>(consumer)));
    ErrorBase::prepareConsumerCaller();
  }

  // Used by the owning operator to publish into the virtual error branch.
  // errorStream_ shares its owner's configId (see Process/Sink constructors),
  // so using *this as the producer yields the same Caller link identity the
  // owner itself would have produced.
  void emit(MessageContext context, Payload<E> payload) {
    if (this->hasConsumer()) {
      topologyCount_.fetch_add(1, std::memory_order_relaxed);
      this->context().template consume<E>(std::move(context), *this,
                                          *this->consumer(),
                                          std::move(payload));
    }
  }

  // Virtual error branches are not always visited as standalone topology
  // nodes. Their owner calls this during its own build so emit() has the same
  // direct, prebuilt Caller path as every ordinary Stream edge.
  void prepareConsumerCaller() {
    ErrorBase::prepareConsumerCaller();
  }

  void prepareTopologyId(size_t ownerId) {
    this->setId(size_t{0} - ownerId);
  }

  [[nodiscard]] std::int64_t getTopologyCount() const noexcept {
    return topologyCount_.load(std::memory_order_relaxed);
  }

  void printErrorTopology(TopologyPrinter& printer,
                          std::unordered_set<size_t>& visited) const {
    ErrorBase::printTopology(printer, visited);
  }

  // Nullable accessor distinct from the inherited Stream<>::getConsumer()
  // (which returns StreamBase& and throws when unset); used by the owning
  // operator to eagerly prepare the Caller link during its own buildTopology.
  [[nodiscard]] StreamConsumer<E>* getErrorConsumer() const noexcept {
    return this->consumer().get();
  }

 protected:
  size_t buildTopology(StreamBuilderContext& context, size_t id, StreamBuilderContext::TIdsList* splitConsumerIds,
                       bool skip) override {
    context.buildTopology(*this, id);
    if (splitConsumerIds) splitConsumerIds->push_back(id);
    return this->buildTopologyCommon(context, id, nullptr, skip);
  }
};

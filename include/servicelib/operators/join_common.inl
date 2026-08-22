// Part of class Stream<_Tp, _Cp, _Context> — included by stream.hpp.
// Shared secondary-input link implementation for Join and MultiJoin.

template <typename _TTp>
class JoinLinkBuilder;

template <typename _TTp, size_t N>
class JoinLink : public StreamConsumer<_TTp>, public StreamBase {
  template <typename, typename>
  friend class StreamExecutionEnvironment;
  template <typename, typename, typename>
  friend class Stream;
  friend class StreamVerifyContext;
  friend class StreamBuilderContext;

 public:
  size_t getId() const noexcept override { return StreamBase::getId(); }

  const std::string& getName() const noexcept override {
    return StreamBase::getName();
  }

 protected:
  JoinLink() = default;

  ~JoinLink() override = default;

  const StreamBase& getBase() const noexcept override { return *this; }

  StreamBase& getBase() noexcept override { return *this; }

  std::string getCode() const override { return std::string(); }

  const std::string_view& getType() const override {
    return StreamBuilderContext::getType<decltype(*this)>();
  }

  template <typename T>
  static auto build(JoinLink& jl, T& consumer) {
    return JoinLinkBuilder<_TTp>::build(jl, consumer);
  }
};

template <typename _TTp, typename _Join, size_t N = 0>
class JoinLinkImpl final : public JoinLink<_TTp, N> {
  template <typename, typename>
  friend class StreamExecutionEnvironment;
  template <typename, typename, typename>
  friend class Stream;

  _Join& consumer_;

 public:
  size_t getConfigId() const noexcept override {
    return consumer_.getConfigId();
  }

  void consume(MessageContext ctx, Payload<_TTp> payload) override {
    consumer_.template consumeRight<_TTp, N>(ctx, std::move(payload));
  }

 protected:
  explicit JoinLinkImpl(_Join& consumer) noexcept : consumer_(consumer) {}

  bool hasConsumer() const noexcept override { return consumer_.hasConsumer(); }

  size_t buildTopology(StreamBuilderContext& ctx, size_t id,
                       StreamBuilderContext::TIdsList* splitConsumerIds,
                       [[maybe_unused]] bool skip) override {
    auto& os = ctx.getOut();
    this->setId(id);
    ctx.buildTopology(*this, id);
    if (splitConsumerIds != nullptr) {
      splitConsumerIds->emplace_back(id);
    }

    os << "auto &stream" << id << "t = static_cast<" << this->getType()
       << "&>(stream" << id << ");" << std::endl;
    size_t nextid = id;
    if (consumer_.getId() == 0) {
      os << "auto &stream" << id + 1 << " = stream" << id << "t.getConsumer();"
         << std::endl;
      nextid = consumer_.buildTopology(ctx, id + 1, nullptr, false);
    }
    os << "auto stream" << id << "c = stream" << id << "t.build(";
    os << "stream" << id << "t, ";
    os << "stream" << consumer_.getId() << "r);" << std::endl;
    os << "auto &stream" << id << "r = *stream" << id << "c;" << std::endl;
    os << "stream" << id << "r.setId(" << id << ");" << std::endl;
    return nextid;
  }

  void verifyTopology(StreamVerifyContext& ctx) const override {
    ctx.verify(*this);
  }

  void printTopology(TopologyPrinter& tp,
                     std::unordered_set<size_t>& visited) const override {
    if (visited.find(this->getId()) == visited.end()) {
      visited.emplace(this->getId());
      tp.printLink(tp.makeNode(*this), tp.makeNode(consumer_));
      consumer_.printTopology(tp, visited);
    }
  }

  const StreamBase& getConsumer() const override { return consumer_.getBase(); }

  StreamBase& getConsumer() override { return consumer_.getBase(); }

  template <typename T>
  static unique_ptr<JoinLinkImpl<_TTp, T, N>> make(T& consumer) {
    return unique_ptr<JoinLinkImpl<_TTp, T, N>>(
        new JoinLinkImpl<_TTp, T, N>(consumer));
  }

  const std::string_view& getType() const override {
    if (consumer_.f_.isInternalType()) {
      return JoinLink<_TTp, N>::getType();
    }
    return StreamBuilderContext::getType<decltype(*this)>();
  }

  template <typename T>
  static auto build(JoinLinkImpl&, T& consumer) {
    return make(consumer);
  }

  template <typename T>
  static auto build(JoinLink<_TTp, N>&, T& consumer) {
    return make(consumer);
  }
};

template <typename _TTp>
class JoinLinkBuilder final {
  template <typename, size_t>
  friend class JoinLink;

 protected:
  template <typename T, size_t N>
  static auto build(JoinLinkImpl<_TTp, T, N>& jl, T& consumer) {
    return JoinLinkImpl<_TTp, T, N>::build(jl, consumer);
  }

  template <typename T, size_t N>
  static auto build(JoinLink<_TTp, N>& jl, T& consumer) {
    return JoinLinkImpl<_TTp, T, N>::build(jl, consumer);
  }
};

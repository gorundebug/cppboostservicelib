// Part of class Stream<_Tp, _Cp, _Context> — included by stream.hpp
// Go analog: operators/process.go — MakeProcessStream / ProcessStream
//
// Three type parameters: _Tp (input), _TTp (output R), _ETp (error output E).
// Primary output goes through the normal TransformStream<_TTp, _CCp> chain.
// Error output goes to an optional errorConsumer_ pointer (set via
// setErrorConsumer).

template <typename _TTp, typename _ETp, typename _CCp>
class ProcessBuilder;

template <typename _TTp, typename _ETp, typename _CCp = StreamConsumer<_TTp>>
class Process : public TransformStream<_TTp, _CCp> {
  template <typename, typename>
  friend class StreamExecutionEnvironment;
  friend class StreamBuilderContext;

 protected:
  Process() = default;
  ~Process() override = default;

  template <typename T>
  explicit Process(unique_ptr<T> consumer)
      : TransformStream<_TTp, T>(std::move(consumer)) {}

  virtual ErrorStream<_ETp>& getErrorStreamImpl() noexcept = 0;

 public:
  // Go: ProcessStream.GetErrorStream().SetConsumer(...). The C++ topology
  // keeps the virtual error stream lightweight and binds its downstream
  // explicitly; values are still dispatched through the configured edge
  // caller by ProcessImpl::produce.
  void setErrorConsumer(StreamConsumer<_ETp>& consumer) {
    getErrorStream().setConsumer(consumer);
  }

  ErrorStream<_ETp>& getErrorStream() noexcept { return getErrorStreamImpl(); }

  const std::string_view& getType() const override {
    return StreamBuilderContext::getType<decltype(*this)>();
  }

  template <typename ProcessFunction, typename Ctx>
  static auto build(Process& stream, StreamFunction<ProcessFunction, Ctx>&& f) {
    auto r = ProcessBuilder<_TTp, _ETp, _CCp>::build(std::move(f));
    r->copySettings(stream);
    return r;
  }

  template <typename T, typename ProcessFunction, typename Ctx>
  static auto build(Process& stream, unique_ptr<T> consumer,
                    StreamFunction<ProcessFunction, Ctx>&& f) {
    auto r =
        ProcessBuilder<_TTp, _ETp, T>::build(std::move(consumer), std::move(f));
    r->copySettings(stream);
    return r;
  }
};

// ProcessFunction: callable (MessageContext, const _Tp&, Collector<_TTp>,
// Collector<_ETp>) -> void
template <typename _TTp, typename _ETp, typename ProcessFunction,
          typename _CCp = StreamConsumer<_TTp>>
class ProcessImpl final : public Process<_TTp, _ETp, _CCp>,
                          public virtual StreamConsumer<_Tp> {
  template <typename, typename>
  friend class StreamExecutionEnvironment;
  template <typename, typename, typename>
  friend class Stream;
  template <typename, typename, typename>
  friend class ProcessBuilder;
  template <typename, typename>
  friend class Collector;
  friend class StreamBuilderContext;

  StreamFunction<ProcessFunction, ProcessImpl> f_;
  ErrorStream<_ETp> errorStream_;

 public:
  using topology_value_type = _TTp;
  using Process<_TTp, _ETp, _CCp>::consume;

  // Go: ProcessStream.Consume — call process function with normal + error
  // collectors
  void consume(MessageContext ctx, Payload<_Tp> payload) override {
    [[maybe_unused]] auto activeSpan =
        tracing::StartStreamSpan(ctx, *this, "stream.process");
    Output output{*this};
    ErrorOutput errors{*this};
    f_(ctx, *this, payload.get(), Collector<_TTp, Output>(output),
       Collector<_ETp, ErrorOutput>(errors));
  }

  size_t getId() const noexcept override {
    return Process<_TTp, _ETp, _CCp>::getId();
  }

  const std::string& getName() const noexcept override {
    return Process<_TTp, _ETp, _CCp>::getName();
  }

  ErrorStream<_ETp>& getErrorStreamImpl() noexcept override {
    return errorStream_;
  }

 protected:
  struct Output {
    ProcessImpl& owner;
    void produce(MessageContext ctx, _TTp value) {
      owner.produceOutput(std::move(ctx), std::move(value));
    }
  };

  struct ErrorOutput {
    ProcessImpl& owner;
    void produce(MessageContext ctx, _ETp value) {
      owner.produceError(std::move(ctx), std::move(value));
    }
  };

  void produceOutput(MessageContext ctx, _TTp v) {
    if (this->hasConsumer()) {
      this->context().template consume<_TTp>(
          std::move(ctx), *this, *this->consumer(),
          Payload<_TTp>::make(std::move(v)));
    }
  }

  void produceError(MessageContext ctx, _ETp v) {
    errorStream_.emit(std::move(ctx), Payload<_ETp>::make(std::move(v)));
  }

  template <typename Ctx = ProcessImpl>
  explicit ProcessImpl(StreamFunction<ProcessFunction, Ctx>&& f)
      : Process<_TTp, _ETp, _CCp>(), f_(std::move(f), *this) {}

  template <typename T, typename Ctx = ProcessImpl>
  ProcessImpl(StreamFunction<ProcessFunction, Ctx>&& f, unique_ptr<T> consumer)
      : Process<_TTp, _ETp, T>(std::move(consumer)), f_(std::move(f), *this) {}

  // Go-aligned: config id + env; output serde (_TTp) is freshly resolved
  // (Go: runtime.MakeSerde[R](env)). The _Tp serde parameter is just
  // propagated as-is — Go has no serde slot for the input type of a
  // type-changing operator. errorStream_ shares the same config id as this
  // Process instance (Go: MakeErrorStream[E](id, env) called with the same
  // id) so that using it as its own producer yields the same Caller link
  // identity Process itself would have produced.
  template <typename Ctx = ProcessImpl>
  ProcessImpl(const servicelib::config::ProcessStreamConfig& cfg,
              serde::StreamSerde<_Tp>* serde, IRuntimeEnvironment* env,
              StreamFunction<ProcessFunction, Ctx>&& f)
      : Process<_TTp, _ETp, _CCp>(), f_(std::move(f), *this) {
    this->setConfigIdentity(cfg);
    StreamConsumer<_Tp>::serde_ = serde;
    Process<_TTp, _ETp, _CCp>::resolveDefaultSerde();
    this->env_ = env;
    errorStream_.configure(static_cast<size_t>(cfg.id), cfg.name + "Error", env);
  }

  template <typename T, typename Ctx = ProcessImpl>
  ProcessImpl(const servicelib::config::ProcessStreamConfig& cfg,
              serde::StreamSerde<_Tp>* serde, IRuntimeEnvironment* env,
              StreamFunction<ProcessFunction, Ctx>&& f, unique_ptr<T> consumer)
      : Process<_TTp, _ETp, T>(std::move(consumer)), f_(std::move(f), *this) {
    this->setConfigIdentity(cfg);
    StreamConsumer<_Tp>::serde_ = serde;
    Process<_TTp, _ETp, _CCp>::resolveDefaultSerde();
    this->env_ = env;
    errorStream_.configure(static_cast<size_t>(cfg.id), cfg.name + "Error", env);
  }

  template <typename F = ProcessFunction, typename Ctx = ProcessImpl>
  static unique_ptr<ProcessImpl<_TTp, _ETp, F>> make(
      StreamFunction<F, Ctx>&& f) {
    return unique_ptr<ProcessImpl<_TTp, _ETp, F>>(
        new ProcessImpl<_TTp, _ETp, F>(std::move(f)));
  }

  template <typename T, typename F = ProcessFunction,
            typename Ctx = ProcessImpl>
  static unique_ptr<ProcessImpl<_TTp, _ETp, F, T>> make(
      StreamFunction<F, Ctx>&& f, unique_ptr<T> consumer) {
    return unique_ptr<ProcessImpl<_TTp, _ETp, F, T>>(
        new ProcessImpl<_TTp, _ETp, F, T>(std::move(f), std::move(consumer)));
  }

  template <typename F = ProcessFunction, typename Ctx = ProcessImpl>
  static unique_ptr<ProcessImpl<_TTp, _ETp, F>> make(
      const servicelib::config::ProcessStreamConfig& cfg,
      serde::StreamSerde<_Tp>* serde, IRuntimeEnvironment* env,
      StreamFunction<F, Ctx>&& f) {
    return unique_ptr<ProcessImpl<_TTp, _ETp, F>>(
        new ProcessImpl<_TTp, _ETp, F>(cfg, serde, env, std::move(f)));
  }

  template <typename T, typename F = ProcessFunction,
            typename Ctx = ProcessImpl>
  static unique_ptr<ProcessImpl<_TTp, _ETp, F, T>> make(
      const servicelib::config::ProcessStreamConfig& cfg,
      serde::StreamSerde<_Tp>* serde, IRuntimeEnvironment* env,
      StreamFunction<F, Ctx>&& f, unique_ptr<T> consumer) {
    return unique_ptr<ProcessImpl<_TTp, _ETp, F, T>>(
        new ProcessImpl<_TTp, _ETp, F, T>(cfg, serde, env, std::move(f),
                                          std::move(consumer)));
  }

  const std::string_view& getType() const override {
    if (f_.isInternalType()) {
      return Process<_TTp, _ETp, _CCp>::getType();
    }
    return StreamBuilderContext::getType<decltype(*this)>();
  }

  std::string getCode() const override {
    if (f_.isInternalType()) {
      return f_.getFunctionCode();
    }
    return std::string();
  }

  bool hasConsumer() const noexcept override {
    return Process<_TTp, _ETp, _CCp>::hasConsumer();
  }

  size_t buildTopology(StreamBuilderContext& ctx, size_t id,
                       StreamBuilderContext::TIdsList* splitConsumerIds,
                       bool skip) override {
    ctx.buildTopology(*this, id);
    if (splitConsumerIds != nullptr) {
      splitConsumerIds->emplace_back(id);
    }
    const auto nextId = this->buildTopologyCommon(ctx, id, nullptr, skip);
    errorStream_.prepareTopologyId(this->getId());
    errorStream_.prepareConsumerCaller();
    return nextId;
  }

  void verifyTopology(StreamVerifyContext& ctx) const override {
    Process<_TTp, _ETp, _CCp>::verifyTopology(ctx);
  }

  void printTopology(TopologyPrinter& tp,
                     std::unordered_set<size_t>& visited) const override {
    Process<_TTp, _ETp, _CCp>::printTopology(tp, visited);
    if (errorStream_.getErrorConsumer()) {
      tp.printLink(tp.makeNode(*this), tp.makeNode(errorStream_));
      errorStream_.printErrorTopology(tp, visited);
    }
  }

  const StreamBase& getConsumer() const override {
    return Process<_TTp, _ETp, _CCp>::getConsumer();
  }

  StreamBase& getConsumer() override {
    return Process<_TTp, _ETp, _CCp>::getConsumer();
  }

  const StreamBase& getBase() const noexcept override {
    return Process<_TTp, _ETp, _CCp>::getBase();
  }

  StreamBase& getBase() noexcept override {
    return Process<_TTp, _ETp, _CCp>::getBase();
  }

  static auto build(ProcessImpl& stream) {
    auto r = make(std::move(stream.f_));
    r->copySettings(stream);
    static_cast<StreamConsumer<_Tp>&>(*r).copyConsumerSettings(
        static_cast<const StreamConsumer<_Tp>&>(stream));
    r->errorStream_.configure(stream.getConfigId(), stream.getName(), stream.getEnv());
    return r;
  }

  template <typename T>
  static auto build(ProcessImpl& stream, unique_ptr<T> consumer) {
    auto r = make(std::move(stream.f_), std::move(consumer));
    r->copySettings(stream);
    static_cast<StreamConsumer<_Tp>&>(*r).copyConsumerSettings(
        static_cast<const StreamConsumer<_Tp>&>(stream));
    r->errorStream_.configure(stream.getConfigId(), stream.getName(), stream.getEnv());
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

template <typename _TTp, typename _ETp, typename _CCp>
class ProcessBuilder final {
  template <typename, typename, typename>
  friend class Process;

 protected:
  template <typename ProcessFunction, typename Ctx>
  static auto build(StreamFunction<ProcessFunction, Ctx>&& f) {
    return ProcessImpl<_TTp, _ETp, ProcessFunction, _CCp>::build(std::move(f));
  }

  template <typename ProcessFunction, typename Ctx>
  static auto build(unique_ptr<_CCp> consumer,
                    StreamFunction<ProcessFunction, Ctx>&& f) {
    return ProcessImpl<_TTp, _ETp, ProcessFunction, _CCp>::build(
        std::move(consumer), std::move(f));
  }
};

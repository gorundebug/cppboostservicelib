// Part of class Stream<_Tp, _Cp, _Context> — included by stream.hpp
// Go analog: operators/delay.go — MakeDelayStream / DelayStream

template <typename _CCp>
class DelayBuilder;

template <typename _CCp = StreamConsumer<_Tp>>
class Delay : public TransformStream<_Tp, _CCp> {
  template <typename, typename>
  friend class StreamExecutionEnvironment;

 protected:
  Delay() = default;
  ~Delay() override = default;

  template <typename T>
  explicit Delay(unique_ptr<T> consumer)
      : TransformStream<_Tp, T>(std::move(consumer)) {}

  const std::string_view& getType() const override {
    return StreamBuilderContext::getType<decltype(*this)>();
  }

  template <typename DelayFunction, typename Ctx>
  static auto build(Delay& stream, StreamFunction<DelayFunction, Ctx>&& f) {
    auto r = DelayBuilder<_CCp>::build(std::move(f));
    r->copySettings(stream);
    r->copyConsumerSettings(stream);
    return r;
  }

  template <typename T, typename DelayFunction, typename Ctx>
  static auto build(Delay& stream, unique_ptr<T> consumer,
                    StreamFunction<DelayFunction, Ctx>&& f) {
    auto r = DelayBuilder<T>::build(std::move(consumer), std::move(f));
    r->copySettings(stream);
    r->copyConsumerSettings(stream);
    return r;
  }
};

// DelayFunction: callable (MessageContext, const _Tp&) -> a chrono duration.
// The context is required for Go parity: soft-deadline functions commonly
// derive their delay from the request deadline.
template <typename DelayFunction, typename _CCp = StreamConsumer<_Tp>>
class DelayImpl final : public Delay<_CCp> {
  template <typename, typename>
  friend class StreamExecutionEnvironment;
  template <typename, typename, typename>
  friend class Stream;
  template <typename>
  friend class DelayBuilder;
  friend class StreamBuilderContext;

  StreamFunction<DelayFunction, DelayImpl> f_;

 public:
  // Go: DelayStream.Consume — compute per-element duration and use the
  // service-wide delay scheduler. Context completion executes the scheduler
  // callback early, but the cancelled message is not emitted downstream.
  void consume(MessageContext ctx, Payload<_Tp> payload) override {
    // Unlike ordinary caller spans, stream.delay logically includes the
    // time spent waiting. The userver adapter therefore creates this span
    // detached from the current coroutine stack: it starts here and is
    // safely ended by the timer coroutine.
    auto* const environment = &this->context();
    std::shared_ptr<tracing::Span> span;
    if (tracing::SamplingEnabled(ctx)) {
      if (auto* tracingEngine = environment->getTracing()) {
        auto tracer = tracingEngine->tracer(environment->getServiceName());
        if (tracer) {
          auto parent = ctx.trace();
          if (!parent.isValid()) {
            parent = tracer->currentSpanContext();
          }
          span = tracer->startDetachedChildOf(
              "stream.delay", parent,
              {tracing::Attribute::String("stream", this->getName())});
          if (span) {
            ctx = std::move(ctx).withTrace(span->spanContext());
          }
        }
      }
    }

    decltype(f_(ctx, *this, payload.get())) duration;
    try {
      duration = f_(ctx, *this, payload.get());
    } catch (const std::exception& error) {
      tracing::SpanError(span.get(), error.what());
      tracing::SpanEnd(span.get());
      throw;
    } catch (...) {
      tracing::SpanError(span.get(), "<unknown>");
      tracing::SpanEnd(span.get());
      throw;
    }
    if (!this->hasConsumer()) {
      tracing::SpanEnd(span.get());
      return;
    }

    if (duration <= std::remove_cv_t<decltype(duration)>::zero()) {
      try {
        this->context().template consume<_Tp>(
            std::move(ctx), *this, *this->consumer(), std::move(payload));
      } catch (const std::exception& error) {
        tracing::SpanError(span.get(), error.what());
        tracing::SpanEnd(span.get());
        throw;
      } catch (...) {
        tracing::SpanError(span.get(), "<unknown>");
        tracing::SpanEnd(span.get());
        throw;
      }
      tracing::SpanEnd(span.get());
      return;
    }

    // Split branches are lightweight derived streams and may not carry a
    // copied StreamBase::env_ pointer. The typed execution context is the
    // authoritative environment for every operator in the graph.
    auto* const downstream = this->consumer().get();
    auto* const producer = this;
    // Delay is an explicit asynchronous graph boundary, just like the Go
    // delay scheduler: scheduling completes the current FunctionCall. The
    // timer callback starts a new continuation and must not retain or revive
    // the caller's logical completion frame.
    ctx = std::move(ctx).withoutCompletion();
    try {
      environment->delay(
          ctx, duration,
          [downstream, producer, context = std::move(ctx),
           value = std::move(payload), span]() mutable {
            if (context.cancelled()) {
              if (span) {
                tracing::SpanEvent(span.get(), "delay.skipped",
                                   {tracing::Attribute::String(
                                       "reason", "context cancelled")});
              }
              tracing::SpanEnd(span.get());
              return;
            }
            try {
              producer->context().template consume<_Tp>(
                  std::move(context), *producer, *downstream, std::move(value));
            } catch (const std::exception& error) {
              tracing::SpanError(span.get(), error.what());
              tracing::SpanEnd(span.get());
              throw;
            } catch (...) {
              tracing::SpanError(span.get(), "<unknown>");
              tracing::SpanEnd(span.get());
              throw;
            }
            tracing::SpanEnd(span.get());
          });
    } catch (const std::exception& error) {
      tracing::SpanError(span.get(), error.what());
      tracing::SpanEnd(span.get());
      throw;
    } catch (...) {
      tracing::SpanError(span.get(), "<unknown>");
      tracing::SpanEnd(span.get());
      throw;
    }
  }

 protected:
  template <typename Ctx = DelayImpl>
  explicit DelayImpl(StreamFunction<DelayFunction, Ctx>&& f)
      : Delay<_CCp>(), f_(std::move(f), *this) {}

  template <typename T, typename Ctx = DelayImpl>
  DelayImpl(StreamFunction<DelayFunction, Ctx>&& f, unique_ptr<T> consumer)
      : Delay<T>(std::move(consumer)), f_(std::move(f), *this) {}

  // Go-aligned: config id + upstream serde (same type _Tp) + env
  template <typename Ctx = DelayImpl>
  DelayImpl(const servicelib::config::DelayStreamConfig& cfg,
            serde::StreamSerde<_Tp>* serde, IRuntimeEnvironment* env,
            StreamFunction<DelayFunction, Ctx>&& f)
      : Delay<_CCp>(), f_(std::move(f), *this) {
    this->setConfigIdentity(cfg);
    this->serde_ = serde;
    this->env_ = env;
  }

  template <typename T, typename Ctx = DelayImpl>
  DelayImpl(const servicelib::config::DelayStreamConfig& cfg,
            serde::StreamSerde<_Tp>* serde, IRuntimeEnvironment* env,
            StreamFunction<DelayFunction, Ctx>&& f, unique_ptr<T> consumer)
      : Delay<T>(std::move(consumer)), f_(std::move(f), *this) {
    this->setConfigIdentity(cfg);
    this->serde_ = serde;
    this->env_ = env;
  }

  template <typename F = DelayFunction, typename Ctx = DelayImpl>
  static unique_ptr<DelayImpl<F>> make(StreamFunction<F, Ctx>&& f) {
    return unique_ptr<DelayImpl<F>>(new DelayImpl<F>(std::move(f)));
  }

  template <typename T, typename F = DelayFunction, typename Ctx = DelayImpl>
  static unique_ptr<DelayImpl<F, T>> make(StreamFunction<F, Ctx>&& f,
                                          unique_ptr<T> consumer) {
    return unique_ptr<DelayImpl<F, T>>(
        new DelayImpl<F, T>(std::move(f), std::move(consumer)));
  }

  template <typename F = DelayFunction, typename Ctx = DelayImpl>
  static unique_ptr<DelayImpl<F>> make(
      const servicelib::config::DelayStreamConfig& cfg,
      serde::StreamSerde<_Tp>* serde, IRuntimeEnvironment* env,
      StreamFunction<F, Ctx>&& f) {
    return unique_ptr<DelayImpl<F>>(
        new DelayImpl<F>(cfg, serde, env, std::move(f)));
  }

  template <typename T, typename F = DelayFunction, typename Ctx = DelayImpl>
  static unique_ptr<DelayImpl<F, T>> make(
      const servicelib::config::DelayStreamConfig& cfg,
      serde::StreamSerde<_Tp>* serde, IRuntimeEnvironment* env,
      StreamFunction<F, Ctx>&& f, unique_ptr<T> consumer) {
    return unique_ptr<DelayImpl<F, T>>(new DelayImpl<F, T>(
        cfg, serde, env, std::move(f), std::move(consumer)));
  }

  const std::string_view& getType() const override {
    if (f_.isInternalType()) {
      return Delay<_CCp>::getType();
    }
    return StreamBuilderContext::getType<decltype(*this)>();
  }

  size_t buildTopology(StreamBuilderContext& ctx, size_t id,
                       StreamBuilderContext::TIdsList* splitConsumerIds,
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

  static auto build(DelayImpl& stream) {
    auto r = make(std::move(stream.f_));
    r->copySettings(stream);
    r->copyConsumerSettings(stream);
    return r;
  }

  template <typename T>
  static auto build(DelayImpl& stream, unique_ptr<T> consumer) {
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
class DelayBuilder final {
  template <typename>
  friend class Delay;

 protected:
  template <typename DelayFunction, typename Ctx>
  static auto build(StreamFunction<DelayFunction, Ctx>&& f) {
    return DelayImpl<DelayFunction, _CCp>::build(std::move(f));
  }

  template <typename DelayFunction, typename Ctx>
  static auto build(unique_ptr<_CCp> consumer,
                    StreamFunction<DelayFunction, Ctx>&& f) {
    return DelayImpl<DelayFunction, _CCp>::build(std::move(consumer),
                                                 std::move(f));
  }
};

// Service-local callable graph. Results return through the existing source.
#pragma once

#include <atomic>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <stop_token>
#include <vector>

#include <servicelib/runtime/common.hpp>
#include <servicelib/runtime/detail/sync.hpp>

namespace servicelib {
namespace detail {

template <typename R>
class SubStreamCall final : public LocalExecutionScope {
 public:
  SubStreamCall(MessageContext context,
                std::shared_ptr<SubStreamCollector<R>> collector)
      : context_(std::move(context)), collector_(std::move(collector)) {}

  void close() noexcept {
    closed_.store(true, std::memory_order_release);
    done_.Send();
  }

  void deliver(const R& value) {
    std::lock_guard lock(callbackMutex_);
    if (closed_.load(std::memory_order_acquire)) return;
    if (context_->cancelled()) { close(); return; }
    try {
      if (collector_->out(*context_, value)) {
        completed_ = !closed_.exchange(true, std::memory_order_acq_rel);
        done_.Send();
      }
    } catch (...) {
      error_ = std::current_exception();
      close();
    }
  }

  void wait(const Deadline& deadline) {
    if (deadline) {
      if (!done_.WaitUntil(*deadline)) close();
    } else {
      done_.Wait();
    }
    std::lock_guard lock(callbackMutex_);
    collector_.reset();
    context_.reset();
    if (error_) std::rethrow_exception(error_);
    if (!completed_) throw std::runtime_error("SubStream invocation cancelled");
  }

  void cleanup() noexcept {
    close();
    std::lock_guard lock(callbackMutex_);
    collector_.reset();
    context_.reset();
    error_ = {};
  }

 private:
  std::atomic<bool> closed_{false};
  std::mutex callbackMutex_;
  SingleUseEvent done_;
  std::optional<MessageContext> context_;
  std::shared_ptr<SubStreamCollector<R>> collector_;
  std::exception_ptr error_;
  bool completed_{};
};
}  // namespace detail

template <typename T, typename R, typename Context>
class SubStream final : public Stream<T, StreamConsumer<T>, Context>,
                        public ISubStream<T, R> {
  using Base = Stream<T, StreamConsumer<T>, Context>;
  using Call = detail::SubStreamCall<R>;

  class ResultLink final : public StreamConsumer<R>, public StreamBase {
   public:
    explicit ResultLink(const SubStream& entry) : key_(entry.key_) {
      this->copySettings(entry);
    }
    void consume(MessageContext context, Payload<R> value) override {
      if (auto call = context.localValue(key_)) call->deliver(value.get());
    }
    size_t getId() const noexcept override { return StreamBase::getId(); }
    const std::string& getName() const noexcept override { return StreamBase::getName(); }
   private:
    bool hasConsumer() const noexcept override { return false; }
    const StreamBase& getConsumer() const override { throw std::logic_error("SubStream result has no consumer"); }
    StreamBase& getConsumer() override { throw std::logic_error("SubStream result has no consumer"); }
    const StreamBase& getBase() const noexcept override { return *this; }
    StreamBase& getBase() noexcept override { return *this; }
    const std::string_view& getType() const override { return StreamBuilderContext::getType<decltype(*this)>(); }
    std::string getCode() const override { return {}; }
    size_t buildTopology(StreamBuilderContext& context, size_t id,
                         StreamBuilderContext::TIdsList* ids, bool) override {
      context.buildTopology(*this, id);
      if (ids) ids->push_back(id);
      if (getId() == 0) this->setId(id);
      return id;
    }
    void verifyTopology(StreamVerifyContext& context) const override { context.verify(*this); }
    void printTopology(TopologyPrinter& printer, std::unordered_set<size_t>& visited) const override {
      if (visited.emplace(getId()).second) printer.printNode(printer.makeNode(*this));
    }
    ContextKey<Call> key_;
  };

 public:
  static std::shared_ptr<SubStream> make(const config::SubStreamConfig& config,
                                        IRuntimeEnvironment& environment) {
    auto stream = std::shared_ptr<SubStream>(new SubStream(config, environment));
    environment.registerStream(stream);
    return stream;
  }
  ~SubStream() override = default;

  void consume(MessageContext, Payload<T>) override {
    throw std::logic_error("SubStream invocation requires a collector");
  }

  void consume(MessageContext context, Payload<T> value,
               std::shared_ptr<SubStreamCollector<R>> collector) override {
    if (!collector) throw std::invalid_argument("SubStream collector is required");
    if (!source_ || !this->hasConsumer()) throw std::logic_error("SubStream body or result source is missing");
    if (context.cancelled()) throw std::runtime_error("SubStream invocation cancelled");
    [[maybe_unused]] auto invocation = this->context().beginInputInvocation();
    auto call = std::make_shared<Call>(context, std::move(collector));
    struct Guard {
      std::shared_ptr<Call> call;
      ~Guard() { call->cleanup(); }
    } guard{call};
    const auto cancel = [call] { call->close(); };
    using StopCallback = std::stop_callback<decltype(cancel)>;
    std::optional<StopCallback> cancellation;
    if (context.stopToken().stop_possible()) cancellation.emplace(context.stopToken(), cancel);
    std::vector<std::unique_ptr<StopCallback>> externalCancellations;
    for (const auto& token : context.externalStopTokens()) {
      if (token.stop_possible()) externalCancellations.push_back(std::make_unique<StopCallback>(token, cancel));
    }
    auto bodyContext = context.withLocalValue(key_, call).withExecutionScope(call);
    tracing::ActiveSpan span;
    if (this->getStreamTracer() && tracing::SamplingEnabled(bodyContext)) {
      span = tracing::StartStreamSpan(bodyContext, *this, "stream.substream");
    }
    this->context().template consume<T>(std::move(bodyContext), *this, *this->consumer(), std::move(value));
    call->wait(context.deadline());
  }

  template <typename Consumer, typename SourceContext>
  void setSource(Stream<R, Consumer, SourceContext>& source) {
    if (source_) throw std::logic_error("SubStream result source is already set");
    if (static_cast<const StreamBase*>(&source) == this) throw std::logic_error("SubStream cannot return itself");
    if (source.getEnv() != this->getEnv()) throw std::logic_error("SubStream result source belongs to another service");
    source.setConsumer(typename StreamBase::template unique_ptr<ResultLink>(new ResultLink(*this)));
    source_ = &source;
  }

 protected:
  size_t buildTopology(StreamBuilderContext& context, size_t id,
                       StreamBuilderContext::TIdsList*, bool skip) override {
    context.buildTopology(*this, id);
    return this->buildTopologyCommon(context, id, nullptr, skip);
  }
  void verifyTopology(StreamVerifyContext& context) const override {
    if (!source_ || !this->hasConsumer()) throw std::logic_error("SubStream body or result source is missing");
    Base::verifyTopology(context);
  }

 private:
  SubStream(const config::SubStreamConfig& config, IRuntimeEnvironment& environment) {
    this->setConfigIdentity(config);
    this->setEnv(&environment);
    this->resolveDefaultSerde();
  }
  ContextKey<Call> key_;
  StreamBase* source_{};
};

template <typename T, typename R, typename Context>
std::shared_ptr<SubStream<T, R, Context>> makeSubStream(
    const config::SubStreamConfig& config, IRuntimeEnvironment& environment) {
  return SubStream<T, R, Context>::make(config, environment);
}
}  // namespace servicelib

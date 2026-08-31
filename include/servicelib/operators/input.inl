// Config-driven Input operator. Go analog: operators/input.go.
#pragma once

#include <functional>
#include <memory>
#include <string>
#include <utility>

#include <servicelib/runtime/config/stream_types.hpp>

namespace servicelib {

template <typename T, typename R, typename E, typename Context>
class InputStream final : public Stream<T, StreamConsumer<T>, Context> {
  using Base = Stream<T, StreamConsumer<T>, Context>;

  class ResultLink final : public StreamConsumer<R>, public StreamBase {
   public:
    explicit ResultLink(InputStream& input) noexcept
        : configId_(input.getConfigId()), input_(input) {}

    void consume(MessageContext context, Payload<R> payload) override {
      input_.consumeResult(std::move(context), std::move(payload));
    }

    size_t getId() const noexcept override { return StreamBase::getId(); }
    size_t getConfigId() const noexcept override { return configId_; }
    const std::string& getName() const noexcept override {
      return StreamBase::getName();
    }

   private:
    bool hasConsumer() const noexcept override { return false; }
    const StreamBase& getConsumer() const override {
      throw StreamException("Input result link has no consumer");
    }
    StreamBase& getConsumer() override {
      throw StreamException("Input result link has no consumer");
    }
    const StreamBase& getBase() const noexcept override { return *this; }
    StreamBase& getBase() noexcept override { return *this; }
    const std::string_view& getType() const override {
      return StreamBuilderContext::getType<decltype(*this)>();
    }
    std::string getCode() const override { return {}; }

    size_t buildTopology(StreamBuilderContext& context, size_t id,
                         StreamBuilderContext::TIdsList* splitConsumerIds,
                         bool) override {
      context.buildTopology(*this, id);
      if (splitConsumerIds) splitConsumerIds->push_back(id);
      if (this->getId() == 0) this->setId(id);
      return id;
    }

    void verifyTopology(StreamVerifyContext& context) const override {
      context.verify(*this);
    }

    void printTopology(TopologyPrinter& printer,
                       std::unordered_set<size_t>& visited) const override {
      if (visited.emplace(getId()).second) {
        printer.printNode(printer.makeNode(*this));
      }
    }

    size_t configId_{};
    InputStream& input_;
  };

 public:
  class ErrorStream final : public Stream<E, StreamConsumer<E>, Context> {
    using ErrorBase = Stream<E, StreamConsumer<E>, Context>;
    friend class InputStream;

   public:
    // Go: MakeErrorStream[E](id, env) — always resolves a fresh serde for E.
    ErrorStream(size_t configId, std::string name,
                IRuntimeEnvironment* environment) {
      this->setConfigId(configId);
      this->setName(name.c_str());
      this->setEnv(environment);
      this->resolveDefaultSerde();
    }
    ~ErrorStream() override = default;

    void emit(MessageContext context, Payload<E> payload) {
      if (this->hasConsumer()) {
        this->context().template consume<E>(
            std::move(context), *this, *this->consumer(), std::move(payload));
      }
    }

   protected:
    size_t buildTopology(StreamBuilderContext& context, size_t id,
                         StreamBuilderContext::TIdsList* splitConsumerIds,
                         bool skip) override {
      context.buildTopology(*this, id);
      if (splitConsumerIds) splitConsumerIds->push_back(id);
      return this->buildTopologyCommon(context, id, nullptr, skip);
    }
  };

  using ResultConsumer = std::function<void(MessageContext, Payload<R>)>;

  static std::shared_ptr<InputStream> make(
      const config::InputStreamConfig& config, serde::StreamSerde<T>* serde,
      IRuntimeEnvironment& environment) {
    auto input = std::shared_ptr<InputStream>(
        new InputStream(config, serde, &environment));
    environment.registerStream(input);
    return input;
  }

  ~InputStream() override = default;

  [[nodiscard]] int getEndpointId() const noexcept { return endpointId_; }

  ErrorStream& getErrorStream() noexcept { return errorStream_; }
  const ErrorStream& getErrorStream() const noexcept { return errorStream_; }

  void consume(MessageContext context, Payload<T> payload) override {
    [[maybe_unused]] auto activeSpan =
        tracing::StartStreamSpan(context, *this, "stream.input");
    if (this->hasConsumer()) {
      this->context().template consume<T>(
          std::move(context), *this, *this->consumer(), std::move(payload));
    }
  }

  void consumeError(MessageContext context, Payload<E> payload) {
    errorStream_.emit(std::move(context), std::move(payload));
  }

  void setResultConsumer(ResultConsumer consumer) {
    resultConsumer_ = std::move(consumer);
  }

  void consumeResult(MessageContext context, Payload<R> payload) {
    if (resultConsumer_) {
      resultConsumer_(std::move(context), std::move(payload));
    }
  }

  template <typename Consumer, typename SourceContext>
  void setSource(Stream<R, Consumer, SourceContext>& source) {
    if (resultSource_) {
      throw StreamException("InputStream result source is already set");
    }
    resultSource_ = &source;
    source.setConsumer(typename StreamBase::template unique_ptr<ResultLink>(
        new ResultLink(*this)));
  }

  [[nodiscard]] const StreamBase* getResultStream() const noexcept {
    return resultSource_;
  }

 protected:
  size_t buildTopology(StreamBuilderContext& context, size_t id,
                       StreamBuilderContext::TIdsList*, bool skip) override {
    context.buildTopology(*this, id);
    const auto nextId = this->buildTopologyCommon(context, id, nullptr, skip);
    return errorStream_.buildTopology(context, nextId + 1, nullptr, false);
  }

  void verifyTopology(StreamVerifyContext& context) const override {
    Base::verifyTopology(context);
    errorStream_.verifyTopology(context);
  }

  void printTopology(TopologyPrinter& printer,
                     std::unordered_set<size_t>& visited) const override {
    Base::printTopology(printer, visited);
    errorStream_.printTopology(printer, visited);
  }

 private:
  // Go: MakeInputStream[T, R, E](cfg, env) — always resolves a fresh serde
  // for T itself; there is no parent to propagate one from, and unlike
  // downstream operators Go's MakeInputStream doesn't even accept a serde
  // parameter, so `serde` here is not read.
  InputStream(const config::InputStreamConfig& config,
              [[maybe_unused]] serde::StreamSerde<T>* serde, IRuntimeEnvironment* environment)
      : endpointId_(config.idEndpoint),
        errorStream_(static_cast<size_t>(config.id), config.name + "Error",
                     environment) {
    this->setConfigId(static_cast<size_t>(config.id));
    this->resolveDefaultSerde();
    this->setEnv(environment);
    this->setName(config.name.c_str());
  }

  int endpointId_{};
  ErrorStream errorStream_;
  StreamBase* resultSource_{nullptr};
  ResultConsumer resultConsumer_;
};

template <typename T, typename R, typename E, typename Context>
std::shared_ptr<InputStream<T, R, E, Context>> makeInputStream(
    const config::InputStreamConfig& config, serde::StreamSerde<T>* serde,
    IRuntimeEnvironment& environment) {
  auto input = InputStream<T, R, E, Context>::make(config, serde, environment);
  return input;
}

template <typename T, typename R, typename E, typename Context>
InputStream<T, R, E, Context>& makeInputStreamRef(
    const config::InputStreamConfig& config, serde::StreamSerde<T>* serde,
    IRuntimeEnvironment& environment) {
  return *InputStream<T, R, E, Context>::make(config, serde, environment);
}

}  // namespace servicelib

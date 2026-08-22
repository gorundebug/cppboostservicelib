// Config-driven cycle link. The root is constructed before the rest of the
// graph and is bound to its source only after the DAG portion is complete.
#pragma once

#include <memory>
#include <utility>

namespace servicelib {

template <typename T, typename Context>
class CycleLinkStream final : public Stream<T, StreamConsumer<T>, Context> {
  class SourceLink final : public StreamConsumer<T>, public StreamBase {
   public:
    explicit SourceLink(CycleLinkStream& target) : target_(target) {}

    void consume(MessageContext context, Payload<T> payload) override {
      target_.consume(std::move(context), std::move(payload));
    }

    size_t getId() const noexcept override {
      return target_.getId();
    }
    size_t getConfigId() const noexcept override {
      return target_.getConfigId();
    }
    const std::string& getName() const noexcept override {
      return target_.getName();
    }

   private:
    bool hasConsumer() const noexcept override { return false; }
    const StreamBase& getConsumer() const override {
      throw StreamException("cycle source link has no owned consumer");
    }
    StreamBase& getConsumer() override {
      throw StreamException("cycle source link has no owned consumer");
    }
    const StreamBase& getBase() const noexcept override { return *this; }
    StreamBase& getBase() noexcept override { return *this; }
    const std::string_view& getType() const override {
      return StreamBuilderContext::getType<decltype(*this)>();
    }
    std::string getCode() const override { return {}; }

    size_t buildTopology(StreamBuilderContext&, size_t,
                         StreamBuilderContext::TIdsList*, bool) override {
      const auto id = getId();
      if (id == 0) {
        throw StreamException(
            "cycle target must be registered before its source");
      }
      return id;
    }
    void verifyTopology(StreamVerifyContext&) const override {
      if (getId() == 0) {
        throw StreamException("cycle source link id wasn't set");
      }
    }
    void printTopology(TopologyPrinter&,
                       std::unordered_set<size_t>&) const override {}

    CycleLinkStream& target_;
  };

 public:
  static std::shared_ptr<CycleLinkStream> make(
      const config::CycleLinkStreamConfig& config,
      serde::StreamSerde<T>* serde, IRuntimeEnvironment& environment) {
    auto stream = std::shared_ptr<CycleLinkStream>(
        new CycleLinkStream(config, serde, &environment));
    environment.registerStream(stream);
    return stream;
  }

  template <typename Consumer, typename SourceContext>
  void setSource(Stream<T, Consumer, SourceContext>& source) {
    if (source_) {
      throw StreamException("cycle link source is already set");
    }
    source_ = &source;
    source.setConsumer(typename StreamBase::template unique_ptr<SourceLink>(
        new SourceLink(*this)));
  }

  void consume(MessageContext context, Payload<T> payload) override {
    if (this->hasConsumer()) {
      this->context().template consume<T>(
          std::move(context), *this, *this->consumer(), std::move(payload));
    }
  }

 protected:
  size_t buildTopology(StreamBuilderContext& context, size_t id,
                       StreamBuilderContext::TIdsList*, bool skip) override {
    context.buildTopology(*this, id);
    return this->buildTopologyCommon(context, id, nullptr, skip);
  }

 private:
  // No Go stream type maps directly to CycleLinkStream, but as a graph root
  // with no parent to propagate from, it follows the same rule as
  // InputStream: resolve its own serde freshly.
  CycleLinkStream(const config::CycleLinkStreamConfig& config,
                  [[maybe_unused]] serde::StreamSerde<T>* serde,
                  IRuntimeEnvironment* environment) {
    this->setConfigId(static_cast<std::size_t>(config.id));
    this->resolveDefaultSerde();
    this->setEnv(environment);
    this->setName(config.name.c_str());
  }

  StreamBase* source_{nullptr};
};

template <typename T, typename Context>
std::shared_ptr<CycleLinkStream<T, Context>> makeCycleLinkStream(
    const config::CycleLinkStreamConfig& config,
    serde::StreamSerde<T>* serde, IRuntimeEnvironment& environment) {
  return CycleLinkStream<T, Context>::make(config, serde, environment);
}

template <typename T, typename Context>
CycleLinkStream<T, Context>& makeCycleLinkStreamRef(
    const config::CycleLinkStreamConfig& config,
    serde::StreamSerde<T>* serde, IRuntimeEnvironment& environment) {
  return *CycleLinkStream<T, Context>::make(config, serde, environment);
}

}  // namespace servicelib

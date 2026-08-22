// Part of class Stream<_Tp, _Cp, _Context> — included by stream.hpp
// Go analog: operators/link.go — MakeLinkStream / LinkStream

  class Link final : public StreamConsumer<_Tp>, public StreamBase {
    template <typename, typename>
    friend class StreamExecutionEnvironment;
    template <typename, typename, typename>
    friend class Stream;
    friend class StreamVerifyContext;
    friend class StreamBuilderContext;

    StreamConsumer<_Tp>* consumer_;
    decltype(_Context::getExecutionEnvironment())& context_{_Context::getExecutionEnvironment()};
    std::function<void(MessageContext, Payload<_Tp>)> preparedCaller_;

   public:
    void consume(MessageContext ctx, Payload<_Tp> payload) override {
      [[maybe_unused]] auto activeSpan =
          tracing::StartStreamSpan(ctx, *this, "stream.link");
      if (this->hasConsumer()) {
        preparedCaller_(ctx, std::move(payload));
      }
    }

    size_t getId() const noexcept override { return StreamBase::getId(); }

    const std::string& getName() const noexcept override { return StreamBase::getName(); }

   protected:
    explicit Link(StreamConsumer<_Tp>& consumer) noexcept : consumer_(&consumer) {}

    Link() noexcept : consumer_(nullptr) {}

    static unique_ptr<Link> make() { return unique_ptr<Link>(new Link()); }

    static unique_ptr<Link> make(StreamConsumer<_Tp>& consumer) { return unique_ptr<Link>(new Link(consumer)); }

    bool hasConsumer() const noexcept override {
      assert(consumer_ != nullptr);
      return true;
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
      os << "auto stream" << id << "c = stream" << id << "t.build(";
      os << "stream" << id << "t);" << std::endl;
      os << "auto &stream" << id << "r = *stream" << id << "c;" << std::endl;
      os << "stream" << id << "r.setId(" << id << ");" << std::endl;
      ctx.addLink(id, consumer_->getBase());
      auto* caller =
          context_.template prepareCaller<_Tp>(*this, *consumer_);
      preparedCaller_ =
          [caller](MessageContext context, Payload<_Tp> payload) {
            caller->consume(std::move(context), std::move(payload));
          };
      return id + 1;
    }

    void verifyTopology(StreamVerifyContext& ctx) const override { ctx.verify(*this); }

    void printTopology(TopologyPrinter& tp, std::unordered_set<size_t>& visited) const override {
      if (visited.find(getId()) == visited.end()) {
        visited.emplace(getId());
        tp.printLink(tp.makeNode(*this), tp.makeNode(*consumer_));
      }
    }

    const StreamBase& getConsumer() const override {
      if (consumer_ == nullptr) {
        throw StreamException("Consumer for Link does not set");
      }
      return consumer_->getBase();
    }

    StreamBase& getConsumer() override {
      if (consumer_ == nullptr) {
        throw StreamException("Consumer for Link does not set");
      }
      return consumer_->getBase();
    }

    const StreamBase& getBase() const noexcept override { return *this; }

    StreamBase& getBase() noexcept override { return *this; }

    std::string getCode() const override { return std::string(); }

    const std::string_view& getType() const override { return StreamBuilderContext::getType<decltype(*this)>(); }

    static auto build(Link& stream) {
      auto r = make();
      r->copySettings(stream);
      r->copyConsumerSettings(stream);
      return r;
    }

    void setLinkConsumer(StreamConsumer<_Tp>& consumer) { consumer_ = &consumer; }
  };

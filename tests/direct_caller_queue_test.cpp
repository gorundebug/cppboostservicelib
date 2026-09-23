#include <algorithm>
#include <functional>
#include <iostream>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

#include <servicelib/runtime/caller.hpp>

namespace {

using servicelib::MessageContext;
using servicelib::Payload;

void Require(bool condition, const char* message) {
  if (!condition) throw std::runtime_error(message);
}

class Consumer final : public servicelib::StreamConsumer<int> {
 public:
  std::function<void(MessageContext, int)> receive;

  void consume(MessageContext context, Payload<int> value) override {
    receive(std::move(context), value.get());
  }
  const std::string& getName() const noexcept override { return name_; }
  size_t getId() const noexcept override { return 1; }

 protected:
  const servicelib::StreamBase& getConsumer() const override {
    throw std::logic_error("unused topology accessor");
  }
  servicelib::StreamBase& getConsumer() override {
    throw std::logic_error("unused topology accessor");
  }
  bool hasConsumer() const noexcept override { return false; }
  const servicelib::StreamBase& getBase() const noexcept override {
    std::terminate();
  }
  servicelib::StreamBase& getBase() noexcept override { std::terminate(); }
  const std::string_view& getType() const override { return type_; }
  std::string getCode() const override { return {}; }
  size_t buildTopology(servicelib::StreamBuilderContext&, size_t,
                       servicelib::StreamBuilderContext::TIdsList*, bool) override {
    return 0;
  }
  void verifyTopology(servicelib::StreamVerifyContext&) const override {}
  void printTopology(servicelib::TopologyPrinter&,
                     std::unordered_set<size_t>&) const override {}

 private:
  const std::string name_{"queue-test"};
  const std::string_view type_{"int"};
};

servicelib::CallerBase::Params Params() {
  servicelib::CallerBase::Params params;
  params.metricsEnabled = false;
  return params;
}

void OrderedCompletionAndIndependentRequests() {
  Consumer consumer;
  servicelib::DirectCaller<int> caller{consumer, Params()};
  std::vector<int> received;
  std::function<void()> retained;
  consumer.receive = [&](MessageContext context, int value) {
    received.push_back(value);
    if (value == 0) {
      retained = [lease = context.retainCompletion()] { (void)lease; };
    }
  };
  const auto a = MessageContext{}.withStreamId("request-a");
  const auto b = MessageContext{}.withStreamId("request-b");
  for (int value = 0; value != 4; ++value) {
    caller.consume(a, Payload<int>::make(value));
  }
  caller.consume(b, Payload<int>::make(10));
  Require(received == std::vector<int>({0, 10}),
          "pending completion must block only its own request");
  retained = {};
  Require(received == std::vector<int>({0, 10, 1, 2, 3}),
          "queued messages must retain FIFO order");
  caller.consume(a, Payload<int>::make(4));
  Require(received.back() == 4, "drained request key must be reusable");
  Require(caller.statistics().count() == 6, "message counter changed");
}

void ConcurrentEnqueueAndDrain() {
  Consumer consumer;
  servicelib::DirectCaller<int> caller{consumer, Params()};
  std::vector<int> received;
  std::function<void()> retained;
  consumer.receive = [&](MessageContext context, int value) {
    received.push_back(value);
    if (value == 0) {
      retained = [lease = context.retainCompletion()] { (void)lease; };
    }
  };
  const auto context = MessageContext{}.withStreamId("shared-request");
  caller.consume(context, Payload<int>::make(0));
  std::vector<std::thread> threads;
  for (int value = 1; value <= 32; ++value) {
    threads.emplace_back([&, value] {
      caller.consume(context, Payload<int>::make(value));
    });
  }
  for (auto& thread : threads) thread.join();
  Require(received == std::vector<int>({0}),
          "concurrent enqueues must wait for the first completion");
  retained = {};
  std::sort(received.begin(), received.end());
  Require(received.size() == 33, "queued messages lost or duplicated");
  for (int value = 0; value <= 32; ++value) {
    Require(received[static_cast<size_t>(value)] == value,
            "concurrent queue delivered wrong payload");
  }
}

void FailureAndReentrantEnqueue() {
  Consumer consumer;
  servicelib::DirectCaller<int> caller{consumer, Params()};
  const auto context = MessageContext{}.withStreamId("reentrant-request");
  std::vector<int> received;
  consumer.receive = [&](MessageContext, int value) {
    received.push_back(value);
    if (value < 0) throw std::runtime_error("expected consumer failure");
    if (value == 0) caller.consume(context, Payload<int>::make(1));
  };
  bool threw = false;
  try {
    caller.consume(context, Payload<int>::make(-1));
  } catch (const std::runtime_error&) {
    threw = true;
  }
  Require(threw, "consumer exception must propagate");
  caller.consume(context, Payload<int>::make(0));
  Require(received == std::vector<int>({-1, 0, 1}),
          "failed or reentrant delivery left the queue stuck");
}

}  // namespace

int main() {
  try {
    OrderedCompletionAndIndependentRequests();
    ConcurrentEnqueueAndDrain();
    FailureAndReentrantEnqueue();
    std::cout << "DirectCaller queue regressions passed\n";
    return 0;
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}

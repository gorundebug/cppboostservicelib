#include <algorithm>
#include <atomic>
#include <chrono>
#include <functional>
#include <iostream>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>

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

void InlineAndSharedTokensPreserveCompletion() {
  int completed = 0;
  auto state = servicelib::AsyncCompletionState::make([&] { ++completed; });
  auto token = state->retainToken();
  auto shared = state->retain();
  auto sharedCopy = shared;
  state->release();
  servicelib::AsyncCompletionToken moved(std::move(token));
  Require(!token && static_cast<bool>(moved), "token move lost ownership");
  const auto moveAssign = [](servicelib::AsyncCompletionToken& destination,
                             servicelib::AsyncCompletionToken& source) {
    destination = std::move(source);
  };
  moveAssign(moved, moved);
  moved.reset();
  moved.reset();
  shared.reset();
  Require(completed == 0, "a shared lease ended before its final owner");
  sharedCopy.reset();
  Require(completed == 1, "completion did not run exactly once");
  Require(!state->retainToken() && !state->retain(),
          "a completed frame was revived");

  int replaced = 0;
  auto first = servicelib::AsyncCompletionState::make([&] { ++replaced; });
  auto second = servicelib::AsyncCompletionState::make([&] { ++replaced; });
  auto destination = first->retainToken();
  auto source = second->retainToken();
  first->release();
  second->release();
  destination = std::move(source);
  Require(replaced == 1 && !source,
          "move assignment did not release the replaced lease");
  destination.reset();
  Require(replaced == 2, "moved lease was not released");
}

void ConcurrentInlineTokenRelease() {
  std::atomic<int> completed{0};
  auto state = servicelib::AsyncCompletionState::make([&] { ++completed; });
  std::vector<servicelib::AsyncCompletionToken> tokens;
  for (int index = 0; index < 64; ++index) tokens.push_back(state->retainToken());
  state->release();
  std::vector<std::thread> threads;
  for (auto& token : tokens) {
    threads.emplace_back([lease = std::move(token)]() mutable { lease.reset(); });
  }
  for (auto& thread : threads) thread.join();
  Require(completed.load() == 1, "concurrent releases completed more than once");
  Require(!state->retainToken(), "concurrent completion left the frame active");
}

void TimerRetainsCoallocatedFrame() {
  Consumer consumer;
  servicelib::DirectCaller<int> caller{consumer, Params()};
  boost::asio::io_context io;
  boost::asio::steady_timer timer(io, std::chrono::milliseconds(1));
  std::vector<int> received;
  int parentCompleted = 0;
  auto parent = servicelib::AsyncCompletionState::make([&] { ++parentCompleted; });
  auto context = MessageContext{}.withStreamId("timer-request").withCompletion(parent);
  MessageContext lateContext;
  consumer.receive = [&](MessageContext current, int value) {
    received.push_back(value);
    if (value == 0) {
      lateContext = current;
      timer.async_wait([&, lease = current.retainCompletionToken()](
                           const boost::system::error_code& error) mutable {
        Require(!error, "timer failed");
        Require(received == std::vector<int>({0}) && parentCompleted == 0,
                "timer's frame or parent completed early");
        lease.reset();
      });
    }
  };
  caller.consume(context, Payload<int>::make(0));
  caller.consume(context, Payload<int>::make(1));
  parent->release();
  Require(parentCompleted == 0, "parent did not wait for the timer");
  io.run();
  Require(received == std::vector<int>({0, 1}) && parentCompleted == 1,
          "timer completion failed to drain the queue and its parent");
  Require(!lateContext.retainCompletionToken() && !lateContext.retainCompletion(),
          "a late context revived a finished coallocated job");
  caller.consume(context, Payload<int>::make(2));
  Require(received == std::vector<int>({0, 1, 2}) && parentCompleted == 1,
          "a saved completed context prevented reuse of the request key");
}

}  // namespace

int main() {
  try {
    OrderedCompletionAndIndependentRequests();
    ConcurrentEnqueueAndDrain();
    FailureAndReentrantEnqueue();
    InlineAndSharedTokensPreserveCompletion();
    ConcurrentInlineTokenRelease();
    TimerRetainsCoallocatedFrame();
    std::cout << "DirectCaller queue regressions passed\n";
    return 0;
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}

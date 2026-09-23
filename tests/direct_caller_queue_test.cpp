#include <array>
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
  const std::string name_{"direct-call-test"};
  const std::string_view type_{"int"};
};

servicelib::CallerBase::Params Params() {
  servicelib::CallerBase::Params params;
  params.metricsEnabled = false;
  return params;
}

void DirectDeliveryWithoutCompletionFrame() {
  Consumer consumer;
  servicelib::DirectCaller<int> caller{consumer, Params()};
  bool called = false;
  const auto thread = std::this_thread::get_id();
  consumer.receive = [&](MessageContext context, int value) {
    Require(std::this_thread::get_id() == thread, "direct call changed threads");
    Require(!context.retainCompletionToken() && !context.retainCompletion(),
            "direct call created an unnecessary completion frame");
    Require(value == 42, "direct call changed the payload");
    called = true;
  };
  caller.consume(MessageContext{}, Payload<int>::make(42));
  Require(called, "direct call returned before invoking the consumer");
  Require(!caller.isAsync(), "default direct call became async");
  servicelib::DirectCaller<int> asyncCaller{consumer, Params(), true};
  called = false;
  asyncCaller.consume(MessageContext{}, Payload<int>::make(42));
  Require(called && asyncCaller.isAsync(),
          "async metadata must not detach direct delivery");
}

void PendingCompletionDoesNotBlockDelivery() {
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
  int completed = 0;
  auto parent = servicelib::AsyncCompletionState::make([&] { ++completed; });
  const auto a = MessageContext{}.withStreamId("request-a").withCompletion(parent);
  const auto b = MessageContext{}.withStreamId("request-b");
  for (int value = 0; value != 4; ++value) {
    caller.consume(a, Payload<int>::make(value));
  }
  caller.consume(b, Payload<int>::make(10));
  Require(received == std::vector<int>({0, 1, 2, 3, 10}),
          "pending completion must not block direct delivery");
  parent->release();
  Require(completed == 0, "retained operation completed early");
  retained = {};
  Require(completed == 1 && received == std::vector<int>({0, 1, 2, 3, 10}),
          "completion must not trigger deferred direct calls");
  caller.consume(a, Payload<int>::make(4));
  Require(received.back() == 4, "completed context prevented direct delivery");
  Require(caller.statistics().count() == 6, "message counter changed");
}

void ConcurrentCallsDoNotWaitForCompletion() {
  Consumer consumer;
  servicelib::DirectCaller<int> caller{consumer, Params()};
  std::array<std::atomic<int>, 33> received{};
  std::function<void()> retained;
  consumer.receive = [&](MessageContext context, int value) {
    ++received.at(static_cast<size_t>(value));
    if (value == 0) {
      retained = [lease = context.retainCompletion()] { (void)lease; };
    }
  };
  std::atomic<int> completed{0};
  auto parent = servicelib::AsyncCompletionState::make([&] { ++completed; });
  const auto context = MessageContext{}.withStreamId("shared-request").withCompletion(parent);
  caller.consume(context, Payload<int>::make(0));
  std::vector<std::thread> threads;
  for (int value = 1; value <= 32; ++value) {
    threads.emplace_back([&, value] {
      caller.consume(context, Payload<int>::make(value));
    });
  }
  for (auto& thread : threads) thread.join();
  for (int value = 0; value <= 32; ++value) {
    Require(received[static_cast<size_t>(value)].load() == 1,
            "concurrent direct call was deferred, lost or duplicated");
  }
  parent->release();
  Require(completed.load() == 0, "concurrent calls released retained operation");
  retained = {};
  Require(completed.load() == 1, "retained operation did not complete");
  Require(caller.statistics().count() == 33, "concurrent message count changed");
}

void FailureAndReentrantDelivery() {
  Consumer consumer;
  servicelib::DirectCaller<int> caller{consumer, Params()};
  const auto context = MessageContext{}.withStreamId("reentrant-request");
  std::vector<int> received;
  consumer.receive = [&](MessageContext, int value) {
    received.push_back(value);
    if (value < 0) throw std::runtime_error("expected consumer failure");
    if (value == 0) {
      caller.consume(context, Payload<int>::make(1));
      received.push_back(2);
    }
  };
  bool threw = false;
  try {
    caller.consume(context, Payload<int>::make(-1));
  } catch (const std::runtime_error&) {
    threw = true;
  }
  Require(threw, "consumer exception must propagate");
  caller.consume(context, Payload<int>::make(0));
  Require(received == std::vector<int>({-1, 0, 1, 2}),
          "reentrant delivery must complete before the outer call resumes");
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

void TimerRetainsParentWithoutBlockingDelivery() {
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
        Require(received == std::vector<int>({0, 1}) && parentCompleted == 0,
                "timer blocked direct delivery or its parent completed early");
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
          "timer completion changed deliveries or failed to complete its parent");
  Require(!lateContext.retainCompletionToken() && !lateContext.retainCompletion(),
          "a late context revived a finished parent");
  caller.consume(context, Payload<int>::make(2));
  Require(received == std::vector<int>({0, 1, 2}) && parentCompleted == 1,
          "a saved completed context prevented reuse of the request key");
}

}  // namespace

int main() {
  try {
    DirectDeliveryWithoutCompletionFrame();
    PendingCompletionDoesNotBlockDelivery();
    ConcurrentCallsDoNotWaitForCompletion();
    FailureAndReentrantDelivery();
    InlineAndSharedTokensPreserveCompletion();
    ConcurrentInlineTokenRelease();
    TimerRetainsParentWithoutBlockingDelivery();
    std::cout << "DirectCaller delivery and completion regressions passed\n";
    return 0;
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}

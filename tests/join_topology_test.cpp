#include <gtest/gtest.h>

#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <servicelib/runtime/store/storage.hpp>
#include <servicelib/transformation/streams.hpp>

#include "test_async.hpp"

namespace {

test_async::AsioRuntime asioRuntime;

struct JoinDataTypes final {
  template <typename>
  struct DataType {};
};

using Left = servicelib::KeyValueType<int, std::string>;
using Right = servicelib::KeyValueType<int, int>;
using Third = servicelib::KeyValueType<int, double>;

servicelib::config::InputStreamConfig inputConfig(int id, std::string name) {
  servicelib::config::InputStreamConfig config;
  config.id = id;
  config.name = std::move(name);
  return config;
}

servicelib::config::SinkStreamConfig sinkConfig(int id, std::string name) {
  servicelib::config::SinkStreamConfig config;
  config.id = id;
  config.name = std::move(name);
  return config;
}

struct JoinValues final {
  void operator()(
      servicelib::MessageContext context, servicelib::StreamBase&, int& key,
      std::pair<std::vector<std::string>, std::vector<int>>& values,
      auto&& output) const {
    if (values.first.empty() || values.second.empty()) return;
    output.out(context, std::to_string(key) + ":" + values.first.front() +
                            ":" + std::to_string(values.second.front()));
  }
};

struct RecordValue final {
  std::vector<std::string>* values{};
  void operator()(servicelib::MessageContext,
                  const std::string& value) const {
    values->push_back(value);
  }
};

struct MultiJoinValues final {
  void operator()(
      servicelib::MessageContext context, servicelib::StreamBase&, int& key,
      std::tuple<std::vector<std::string>, std::vector<int>,
                 std::vector<double>>& values,
      auto&& output) const {
    if (std::get<0>(values).empty() || std::get<1>(values).empty() ||
        std::get<2>(values).empty()) {
      return;
    }
    output.out(context,
               std::to_string(key) + ":" + std::get<0>(values).front() +
                   ":" + std::to_string(std::get<1>(values).front()) + ":" +
                   std::to_string(std::get<2>(values).front()));
  }
};

struct JoinAvailability final {
  void operator()(
      servicelib::MessageContext context, servicelib::StreamBase&, int& key,
      std::pair<std::vector<std::string>, std::vector<int>>& values,
      auto&& output) const {
    output.out(context, std::to_string(key) + ":L" +
                            std::to_string(values.first.size()) + ":R" +
                            std::to_string(values.second.size()));
  }
};

class JoinApp final : public servicelib::StreamApp<JoinApp, JoinDataTypes> {
 public:
  using Environment = JoinApp::TStreamExecutionEnvironment;

  void streamsInit() {
    left_ = servicelib::makeInputStream<Left, std::monostate, int, Environment>(
        inputConfig(1, "left"), nullptr, *this);
    right_ =
        servicelib::makeInputStream<Right, std::monostate, int, Environment>(
            inputConfig(2, "right"), nullptr, *this);

    servicelib::config::JoinStreamConfig config;
    config.id = 3;
    config.name = "inner-join";
    config.joinType = servicelib::api::JoinType::kInner;
    config.joinStorage = servicelib::api::JoinStorageType::kHashMap;
    auto& joined = left_->join(
        config, *right_, servicelib::StreamType<std::string>{},
        servicelib::Inner{}, servicelib::InMemory_Strategy{},
        servicelib::StreamFunction(JoinValues{}));
    joined.sink(sinkConfig(4, "joined-values"),
                servicelib::StreamType<int>{},
                servicelib::StreamFunction(RecordValue{&results_}));

    multiLeft_ =
        servicelib::makeInputStream<Left, std::monostate, int, Environment>(
            inputConfig(10, "multi-left"), nullptr, *this);
    multiRight_ =
        servicelib::makeInputStream<Right, std::monostate, int, Environment>(
            inputConfig(11, "multi-right"), nullptr, *this);
    multiThird_ =
        servicelib::makeInputStream<Third, std::monostate, int, Environment>(
            inputConfig(12, "multi-third"), nullptr, *this);
    servicelib::config::MultiJoinStreamConfig multiConfig;
    multiConfig.id = 13;
    multiConfig.name = "multi-join";
    multiConfig.joinStorage = servicelib::api::JoinStorageType::kHashMap;
    auto& multiJoined = multiLeft_->multiJoin(
        multiConfig, servicelib::StreamType<std::string>{},
        servicelib::InMemory_Strategy{},
        servicelib::StreamFunction(MultiJoinValues{}), *multiRight_,
        *multiThird_);
    multiJoined.sink(sinkConfig(14, "multi-joined-values"),
                     servicelib::StreamType<int>{},
                     servicelib::StreamFunction(
                         RecordValue{&multiResults_}));

    addAvailabilityJoin(20, servicelib::api::JoinType::kLeft,
                        servicelib::Left{}, "left-join", leftResults_);
    addAvailabilityJoin(30, servicelib::api::JoinType::kRight,
                        servicelib::Right{}, "right-join", rightResults_);
    addAvailabilityJoin(40, servicelib::api::JoinType::kOuter,
                        servicelib::Outer{}, "outer-join", outerResults_);
  }

  int start() { return 0; }

  template <typename T>
  void registerStorage(std::shared_ptr<T> storage) {
    storages_.push_back(std::move(storage));
  }

  void startStorages() {
    for (const auto& storage : storages_) storage->start({});
  }

  void stopStorages() {
    for (auto iterator = storages_.rbegin(); iterator != storages_.rend();
         ++iterator) {
      (*iterator)->stop({});
    }
  }

  void prepareTopology() { static_cast<void>(getExecutionRuntime<>()); }

  void delay(servicelib::Context, servicelib::pool::IDelayPool::Duration,
             std::function<void()> task) override {
    task();
  }

  void pushLeft(int key, std::string value) {
    left_->consume({}, servicelib::Payload<Left>::make(
                           Left::make(key, std::move(value))));
  }

  void pushRight(int key, int value) {
    right_->consume({}, servicelib::Payload<Right>::make(
                            Right::make(key, value)));
  }

  void pushMultiLeft(int key, std::string value) {
    multiLeft_->consume({}, servicelib::Payload<Left>::make(
                                Left::make(key, std::move(value))));
  }

  void pushMultiRight(int key, int value) {
    multiRight_->consume({}, servicelib::Payload<Right>::make(
                                 Right::make(key, value)));
  }

  void pushMultiThird(int key, double value) {
    multiThird_->consume({}, servicelib::Payload<Third>::make(
                                 Third::make(key, value)));
  }

  void pushAvailabilityLeft(std::size_t index, int key, std::string value) {
    availabilityLeft_.at(index)->consume(
        {}, servicelib::Payload<Left>::make(
                Left::make(key, std::move(value))));
  }

  void pushAvailabilityRight(std::size_t index, int key, int value) {
    availabilityRight_.at(index)->consume(
        {}, servicelib::Payload<Right>::make(Right::make(key, value)));
  }

  const std::vector<std::string>& results() const { return results_; }
  const std::vector<std::string>& multiResults() const {
    return multiResults_;
  }
  std::size_t storageCount() const { return storages_.size(); }
  const std::vector<std::string>& leftResults() const { return leftResults_; }
  const std::vector<std::string>& rightResults() const {
    return rightResults_;
  }
  const std::vector<std::string>& outerResults() const {
    return outerResults_;
  }

 private:
  template <typename JoinTag>
  void addAvailabilityJoin(int baseId, servicelib::api::JoinType joinType,
                           JoinTag tag, std::string name,
                           std::vector<std::string>& results) {
    auto left =
        servicelib::makeInputStream<Left, std::monostate, int, Environment>(
            inputConfig(baseId, name + "-left"), nullptr, *this);
    auto right =
        servicelib::makeInputStream<Right, std::monostate, int, Environment>(
            inputConfig(baseId + 1, name + "-right"), nullptr, *this);
    servicelib::config::JoinStreamConfig config;
    config.id = baseId + 2;
    config.name = name;
    config.joinType = joinType;
    config.joinStorage = servicelib::api::JoinStorageType::kHashMap;
    auto& joined = left->join(
        config, *right, servicelib::StreamType<std::string>{}, tag,
        servicelib::InMemory_Strategy{},
        servicelib::StreamFunction(JoinAvailability{}));
    joined.sink(sinkConfig(baseId + 3, name + "-output"),
                servicelib::StreamType<int>{},
                servicelib::StreamFunction(RecordValue{&results}));
    availabilityLeft_.push_back(std::move(left));
    availabilityRight_.push_back(std::move(right));
  }

  std::shared_ptr<servicelib::InputStream<Left, std::monostate, int,
                                          Environment>>
      left_;
  std::shared_ptr<servicelib::InputStream<Right, std::monostate, int,
                                          Environment>>
      right_;
  std::shared_ptr<servicelib::InputStream<Left, std::monostate, int,
                                          Environment>>
      multiLeft_;
  std::shared_ptr<servicelib::InputStream<Right, std::monostate, int,
                                          Environment>>
      multiRight_;
  std::shared_ptr<servicelib::InputStream<Third, std::monostate, int,
                                          Environment>>
      multiThird_;
  std::vector<std::shared_ptr<servicelib::store::IStorage>> storages_;
  std::vector<std::shared_ptr<servicelib::InputStream<
      Left, std::monostate, int, Environment>>>
      availabilityLeft_;
  std::vector<std::shared_ptr<servicelib::InputStream<
      Right, std::monostate, int, Environment>>>
      availabilityRight_;
  std::vector<std::string> results_;
  std::vector<std::string> multiResults_;
  std::vector<std::string> leftResults_;
  std::vector<std::string> rightResults_;
  std::vector<std::string> outerResults_;
};

TEST(JoinTopology, InnerJoinUsesRegisteredStorageLifecycle) {
  auto& app = JoinApp::createStreamApp();
  app.prepareTopology();
  ASSERT_EQ(app.storageCount(), 5U);
  app.startStorages();

  app.pushLeft(7, "order");
  EXPECT_TRUE(app.results().empty());
  app.pushRight(8, 12);
  EXPECT_TRUE(app.results().empty());
  app.pushRight(7, 42);
  EXPECT_EQ(app.results(), (std::vector<std::string>{"7:order:42"}));

  // A successful callback clears the key, so a new right value alone cannot
  // produce a second result.
  app.pushRight(7, 43);
  EXPECT_EQ(app.results(), (std::vector<std::string>{"7:order:42"}));

  // Right links occupy tuple/storage slots 1 and 2. The left slot is 0 and
  // triggers evaluation only after it arrives, matching MultiJoin in Go.
  app.pushMultiThird(9, 2.5);
  app.pushMultiRight(9, 17);
  EXPECT_TRUE(app.multiResults().empty());
  app.pushMultiLeft(9, "cart");
  EXPECT_EQ(app.multiResults(),
            (std::vector<std::string>{"9:cart:17:2.500000"}));

  app.pushAvailabilityLeft(0, 20, "left-only");
  EXPECT_EQ(app.leftResults(), (std::vector<std::string>{"20:L1:R0"}));
  app.pushAvailabilityLeft(1, 21, "blocked-left");
  EXPECT_TRUE(app.rightResults().empty());
  app.pushAvailabilityRight(1, 21, 3);
  EXPECT_EQ(app.rightResults(), (std::vector<std::string>{"21:L1:R1"}));
  app.pushAvailabilityRight(1, 23, 5);
  EXPECT_EQ(app.rightResults(),
            (std::vector<std::string>{"21:L1:R1", "23:L0:R1"}));
  app.pushAvailabilityRight(2, 22, 4);
  EXPECT_EQ(app.outerResults(), (std::vector<std::string>{"22:L0:R1"}));
  app.stopStorages();
}

}  // namespace

#pragma once

#include <servicelib/api/serviceapi.hpp>
#include <servicelib/runtime/detail/asio_dispatch.hpp>
#include <servicelib/runtime/environment/environment.hpp>
#include <servicelib/runtime/store/joinstore.hpp>
#include <servicelib/runtime/store/rotatingmap.hpp>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>

#include <any>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <stop_token>
#include <unordered_map>
#include <utility>
#include <vector>

namespace servicelib::store {

template <typename K, typename Hash = std::hash<K>,
          typename Equal = std::equal_to<K>>
class HashMapJoinStorage final : public IJoinStorage<K> {
 public:
  using Duration = JoinStorageConfig::Duration;

  HashMapJoinStorage(IServiceEnvironment& env, JoinStorageConfig config)
      : state_(std::make_shared<State>(
            detail::ParallelExecutorRegistry::Get(), std::move(config),
            env.getMetrics(), [&env] {
              const auto service = env.getServiceConfigSnapshot();
              return service ? service->name : std::string{};
            }())) {}
  ~HashMapJoinStorage() override {
    if (state_->running) std::terminate();
  }

  void start([[maybe_unused]] Context context) override {
    std::lock_guard lock(state_->mutex);
    if (state_->running) throw StoreAlreadyStartedError();
    if (state_->stopped) throw StoreStoppedError();
    state_->running = true;
    if (state_->config.ttl > Duration::zero()) ArmRotation(state_);
  }

  void stop([[maybe_unused]] Context context) override {
    std::vector<std::shared_ptr<Item>> items;
    {
      std::lock_guard lock(state_->mutex);
      if (state_->stopped) return;
      state_->running = false;
      state_->stopped = true;
      static_cast<void>(state_->rotationTimer.cancel());
      for (const auto& [unused, item] : state_->current)
        items.push_back(item);
      for (const auto& [unused, item] : state_->previous)
        items.push_back(item);
      const auto count = state_->current.size() + state_->previous.size();
      state_->current.clear();
      state_->previous.clear();
      if (state_->metricsEnabled && count != 0)
        state_->count->sub(static_cast<std::int64_t>(count));
    }
    for (const auto& item : items) {
      std::lock_guard lock(item->mutex);
      item->processed = true;
      ++item->generation;
      static_cast<void>(item->timer.cancel());
    }
  }

  void joinValue(Context context, K key, std::size_t index, std::any value,
                 JoinValueFunction callback) override {
    if (!callback) throw std::invalid_argument("join callback is required");
    const auto effectiveDeadline = EffectiveDeadline(context);

    for (;;) {
      auto [item, created] = FindOrCreate(key, index, callback);
      std::unique_lock itemLock(item->mutex);
      if (item->processed) continue;
      if (item->deadline && *item->deadline <= Clock::now()) {
        item->processed = true;
        const auto expiry = item->callback;
        itemLock.unlock();
        static_cast<void>(expiry(item->values));
        RemoveIfSame(key, item, true);
        continue;
      }

      if (created && effectiveDeadline) {
        item->deadline = effectiveDeadline;
        ArmExpiry(key, item, context);
      } else if (!created && state_->config.renewTtl && effectiveDeadline) {
        item->deadline = effectiveDeadline;
        ArmExpiry(key, item, context);
        MoveToCurrent(key, item);
      }

      if (item->values.size() <= index) item->values.resize(index + 1);
      item->values[index].push_back(std::move(value));
      item->processed = callback(item->values);
      if (item->processed) {
        ++item->generation;
        static_cast<void>(item->timer.cancel());
        itemLock.unlock();
        RemoveIfSame(key, item, false);
      }
      return;
    }
  }

  [[nodiscard]] std::size_t size() const {
    std::lock_guard lock(state_->mutex);
    return state_->current.size() + state_->previous.size();
  }


 private:
  HashMapJoinStorage(const HashMapJoinStorage&) = delete;
  HashMapJoinStorage& operator=(const HashMapJoinStorage&) = delete;

  using Clock = std::chrono::steady_clock;
  using Cancellation = std::stop_callback<std::function<void()>>;

  struct State;
  struct Item final {
    explicit Item(boost::asio::any_io_executor executor)
        : timer(std::move(executor)) {}
    std::mutex mutex;
    JoinValues values;
    JoinValueFunction callback;
    boost::asio::steady_timer timer;
    std::vector<std::unique_ptr<Cancellation>> cancellations;
    std::optional<Clock::time_point> deadline;
    std::uint64_t generation{};
    bool processed{};
  };
  struct State final {
    State(boost::asio::any_io_executor executor, JoinStorageConfig value,
          metrics::Metrics& metrics, std::string service)
        : executor(std::move(executor)),
          config(std::move(value)),
          metricsEnabled(metrics.enabled()),
          rotationTimer(this->executor) {
      auto scope = metrics.scope(
          "hashmap_join_storage",
          {{"service", std::move(service)}, {"name", config.name}});
      count = scope->gauge("count", "Elements count stored in a join storage");
      evictionsTotal = scope->counter(
          "evictions_total",
          "Total number of items evicted from join storage by TTL");
    }
    boost::asio::any_io_executor executor;
    JoinStorageConfig config;
    bool metricsEnabled{};
    mutable std::mutex mutex;
    std::unordered_map<K, std::shared_ptr<Item>, Hash, Equal> current;
    std::unordered_map<K, std::shared_ptr<Item>, Hash, Equal> previous;
    boost::asio::steady_timer rotationTimer;
    std::size_t highWaterMark{};
    std::atomic<std::size_t> evictions{};
    std::unique_ptr<metrics::Int64Gauge> count;
    std::unique_ptr<metrics::Int64Counter> evictionsTotal;
    bool running{};
    bool stopped{};
  };

  [[nodiscard]] std::optional<Clock::time_point> EffectiveDeadline(
      const Context& context) const {
    if (context.deadline()) return context.deadline();
    if (state_->config.ttl > Duration::zero())
      return Clock::now() + state_->config.ttl;
    return std::nullopt;
  }

  std::pair<std::shared_ptr<Item>, bool> FindOrCreate(
      const K& key, std::size_t index, const JoinValueFunction& callback) {
    std::lock_guard lock(state_->mutex);
    if (!state_->running) {
      if (state_->stopped) throw StoreStoppedError();
      throw StoreNotStartedError();
    }
    if (const auto found = state_->current.find(key);
        found != state_->current.end())
      return {found->second, false};
    if (const auto found = state_->previous.find(key);
        found != state_->previous.end())
      return {found->second, false};
    auto item = std::make_shared<Item>(state_->executor);
    item->values.resize(index + 1);
    item->callback = callback;
    state_->current.emplace(key, item);
    if (state_->metricsEnabled) state_->count->inc();
    return {std::move(item), true};
  }

  void ArmExpiry(const K& key, const std::shared_ptr<Item>& item,
                 const Context& context) {
    const auto generation = ++item->generation;
    static_cast<void>(item->timer.cancel());
    item->timer.expires_at(*item->deadline);
    const std::weak_ptr<State> weakState = state_;
    const std::weak_ptr<Item> weakItem = item;
    item->timer.async_wait([weakState, weakItem, key, generation](
                               const boost::system::error_code& error) {
      if (!error) Expire(weakState, key, weakItem, generation);
    });
    auto expire = [weakState, weakItem, key, generation] {
      const auto state = weakState.lock();
      if (!state) return;
      boost::asio::post(state->executor,
                        [weakState, weakItem, key, generation] {
                          Expire(weakState, key, weakItem, generation);
                        });
    };
    AddCancellation(item, context.stopToken(), expire);
    for (const auto& token : context.externalStopTokens())
      AddCancellation(item, token, expire);
  }

  static void AddCancellation(const std::shared_ptr<Item>& item,
                              std::stop_token token,
                              const std::function<void()>& callback) {
    if (token.stop_possible())
      item->cancellations.push_back(
          std::make_unique<Cancellation>(token, callback));
  }

  static void Expire(const std::weak_ptr<State>& weakState, const K& key,
                     const std::weak_ptr<Item>& weakItem,
                     std::uint64_t generation) {
    const auto state = weakState.lock();
    const auto item = weakItem.lock();
    if (!state || !item) return;
    JoinValueFunction callback;
    {
      std::lock_guard lock(item->mutex);
      if (item->processed || item->generation != generation) return;
      item->processed = true;
      callback = item->callback;
    }
    try {
      static_cast<void>(callback(item->values));
    } catch (...) {
    }
    RemoveIfSame(*state, key, item, true);
  }

  void RemoveIfSame(const K& key, const std::shared_ptr<Item>& item,
                    bool eviction) {
    RemoveIfSame(*state_, key, item, eviction);
  }
  static void RemoveIfSame(State& state, const K& key,
                           const std::shared_ptr<Item>& item, bool eviction) {
    std::lock_guard lock(state.mutex);
    bool removed{};
    if (const auto found = state.current.find(key);
        found != state.current.end() && found->second == item) {
      state.current.erase(found);
      removed = true;
    }
    if (const auto found = state.previous.find(key);
        found != state.previous.end() && found->second == item) {
      state.previous.erase(found);
      removed = true;
    }
    if (removed && state.metricsEnabled) state.count->dec();
    if (removed && eviction) {
      state.evictions.fetch_add(1);
      if (state.metricsEnabled) state.evictionsTotal->inc();
    }
  }

  void MoveToCurrent(const K& key, const std::shared_ptr<Item>& item) {
    std::lock_guard lock(state_->mutex);
    if (const auto found = state_->previous.find(key);
        found != state_->previous.end() && found->second == item) {
      state_->previous.erase(found);
      state_->current.insert_or_assign(key, item);
    }
  }

  static void ArmRotation(const std::shared_ptr<State>& state) {
    state->rotationTimer.expires_after(state->config.ttl);
    const std::weak_ptr<State> weak = state;
    state->rotationTimer.async_wait(
        [weak](const boost::system::error_code& error) {
          if (error) return;
          const auto state = weak.lock();
          if (!state) return;
          Rotate(*state);
          std::lock_guard lock(state->mutex);
          if (state->running) ArmRotation(state);
        });
  }

  static void Rotate(State& state) {
    std::lock_guard lock(state.mutex);
    const auto total = state.current.size() + state.previous.size();
    const bool shouldRotate =
        state.highWaterMark == 0 ||
        total < (state.highWaterMark + 3) / 4;
    state.highWaterMark = std::max(state.highWaterMark, total);
    if (!shouldRotate) return;
    state.highWaterMark = total;
    for (auto& [key, item] : state.previous)
      state.current.try_emplace(key, std::move(item));
    state.previous = std::move(state.current);
    state.current.clear();
  }

  std::shared_ptr<State> state_;
};
template <typename K, typename Hash = std::hash<K>,
          typename Equal = std::equal_to<K>>
std::unique_ptr<IJoinStorage<K>> makeHashMapJoinStorage(
    IServiceEnvironment& env, JoinStorageConfig config) {
  return std::make_unique<HashMapJoinStorage<K, Hash, Equal>>(
      env, std::move(config));
}

template <typename K, typename Hash = std::hash<K>,
          typename Equal = std::equal_to<K>>
std::unique_ptr<IJoinStorage<K>> makeJoinStorage(api::JoinStorageType type,
                                                 IServiceEnvironment& env,
                                                 JoinStorageConfig config) {
  switch (type) {
    case api::JoinStorageType::kHashMap:
      return makeHashMapJoinStorage<K, Hash, Equal>(env, std::move(config));
    case api::JoinStorageType::kUndefined:
    case api::JoinStorageType::kRocksDB:
    case api::JoinStorageType::kAerospike:
      throw UnsupportedStoreError();
  }
  throw UnsupportedStoreError();
}


}  // namespace servicelib::store

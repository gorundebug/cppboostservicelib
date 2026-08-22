#pragma once

#include <servicelib/runtime/detail/asio_dispatch.hpp>
#include <servicelib/runtime/store/storage.hpp>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/steady_timer.hpp>

#include <array>
#include <chrono>
#include <concepts>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <utility>

namespace servicelib::store {

inline constexpr std::size_t kRotatingMapShrinkFactor = 4;
inline constexpr std::size_t kRotatingMapShardCount = 64;
inline constexpr std::size_t kRotatingMapMinCapacity = 1'000;

template <typename K, typename V, typename Hash = std::hash<K>,
          typename Equal = std::equal_to<K>>
class RotatingMap final : public IStorage {
 public:
  using Duration = std::chrono::steady_clock::duration;

  explicit RotatingMap(Duration interval,
                       std::size_t minCapacity = kRotatingMapMinCapacity)
      : state_(std::make_shared<State>(
            detail::ParallelExecutorRegistry::Get(), interval, minCapacity)) {
    if (interval <= Duration::zero())
      throw std::invalid_argument("rotating map interval must be positive");
  }
  ~RotatingMap() override {
    if (state_->running) std::terminate();
  }

  void start([[maybe_unused]] Context context) override {
    std::lock_guard lock(state_->lifecycleMutex);
    if (state_->running) throw StoreAlreadyStartedError();
    if (state_->stopped) throw StoreStoppedError();
    state_->running = true;
    Arm(state_);
  }

  void stop([[maybe_unused]] Context context) override {
    std::lock_guard lock(state_->lifecycleMutex);
    if (state_->stopped) return;
    state_->running = false;
    state_->stopped = true;
    static_cast<void>(state_->timer.cancel());
  }

  void set(K key, V value) {
    auto& shard = ShardFor(*state_, key);
    std::lock_guard lock(shard.mutex);
    if (shard.current.contains(key) || shard.previous.contains(key))
      throw DuplicateKeyError();
    shard.current.emplace(std::move(key), std::move(value));
  }

  template <typename Factory>
  [[nodiscard]] std::pair<V, bool> getOrCreate(const K& key,
                                               Factory&& factory) {
    auto& shard = ShardFor(*state_, key);
    std::lock_guard lock(shard.mutex);
    if (const auto it = shard.current.find(key); it != shard.current.end())
      return {it->second, true};
    if (const auto it = shard.previous.find(key); it != shard.previous.end())
      return {it->second, true};
    V value = std::forward<Factory>(factory)();
    shard.current.emplace(key, value);
    return {std::move(value), false};
  }

  [[nodiscard]] std::optional<V> get(const K& key) const
    requires std::copy_constructible<V>
  {
    const auto& shard = ShardFor(*state_, key);
    std::lock_guard lock(shard.mutex);
    if (const auto it = shard.current.find(key); it != shard.current.end())
      return it->second;
    if (const auto it = shard.previous.find(key); it != shard.previous.end())
      return it->second;
    return std::nullopt;
  }

  [[nodiscard]] std::optional<V> pop(const K& key) {
    auto& shard = ShardFor(*state_, key);
    std::lock_guard lock(shard.mutex);
    if (auto it = shard.current.find(key); it != shard.current.end()) {
      V value = std::move(it->second);
      shard.current.erase(it);
      return value;
    }
    if (auto it = shard.previous.find(key); it != shard.previous.end()) {
      V value = std::move(it->second);
      shard.previous.erase(it);
      return value;
    }
    return std::nullopt;
  }

  [[nodiscard]] std::size_t size() const {
    std::size_t result{};
    for (const auto& shard : state_->shards) {
      std::lock_guard lock(shard.mutex);
      result += shard.current.size() + shard.previous.size();
    }
    return result;
  }

 private:
  RotatingMap(const RotatingMap&) = delete;
  RotatingMap& operator=(const RotatingMap&) = delete;

  struct Shard final {
    mutable std::mutex mutex;
    std::unordered_map<K, V, Hash, Equal> current;
    std::unordered_map<K, V, Hash, Equal> previous;
    std::size_t highWaterMark{};
  };
  struct State final {
    State(boost::asio::any_io_executor executor, Duration intervalValue,
          std::size_t minCapacityValue)
        : timer(std::move(executor)),
          interval(intervalValue),
          minCapacity(minCapacityValue) {}
    std::mutex lifecycleMutex;
    boost::asio::steady_timer timer;
    Duration interval;
    std::size_t minCapacity;
    bool running{};
    bool stopped{};
    std::array<Shard, kRotatingMapShardCount> shards;
    Hash hash;
  };

  static Shard& ShardFor(State& state, const K& key) {
    return state.shards[state.hash(key) % state.shards.size()];
  }
  static const Shard& ShardFor(const State& state, const K& key) {
    return state.shards[state.hash(key) % state.shards.size()];
  }

  static void Arm(const std::shared_ptr<State>& state) {
    state->timer.expires_after(state->interval);
    const std::weak_ptr<State> weak = state;
    state->timer.async_wait([weak](const boost::system::error_code& error) {
      if (error) return;
      const auto state = weak.lock();
      if (!state) return;
      {
        std::lock_guard lock(state->lifecycleMutex);
        if (!state->running) return;
      }
      Rotate(*state);
      std::lock_guard lock(state->lifecycleMutex);
      if (state->running) Arm(state);
    });
  }

  static void Rotate(State& state) {
    for (auto& shard : state.shards) {
      std::lock_guard lock(shard.mutex);
      const auto total = shard.current.size() + shard.previous.size();
      const bool shouldRotate =
          shard.highWaterMark == 0 ||
          total < (shard.highWaterMark + kRotatingMapShrinkFactor - 1) /
                      kRotatingMapShrinkFactor;
      shard.highWaterMark = std::max(shard.highWaterMark, total);
      if (shard.highWaterMark < state.minCapacity || !shouldRotate) continue;

      shard.highWaterMark = total;
      decltype(shard.current) combined;
      combined.reserve(total);
      for (auto& [key, value] : shard.current)
        combined.emplace(key, std::move(value));
      for (auto& [key, value] : shard.previous)
        combined.try_emplace(key, std::move(value));
      shard.previous = std::move(combined);
      shard.current.clear();
      shard.current.rehash(0);
    }
  }

  std::shared_ptr<State> state_;
};
template <typename K, typename V, typename Hash = std::hash<K>,
          typename Equal = std::equal_to<K>>
std::unique_ptr<RotatingMap<K, V, Hash, Equal>> makeRotatingMap(
    typename RotatingMap<K, V, Hash, Equal>::Duration interval) {
  return std::make_unique<RotatingMap<K, V, Hash, Equal>>(interval);
}


}  // namespace servicelib::store

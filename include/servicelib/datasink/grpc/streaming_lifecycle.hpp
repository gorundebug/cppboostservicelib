#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <unordered_map>
#include <utility>
#include <servicelib/runtime/detail/sync.hpp>

namespace servicelib::datasink::grpc::detail {

// Admission and draining share one mutex, so close cannot miss a reader.
// Only the explicitly synchronous shutdown boundary calls Wait().
class StreamingActivity final {
  struct State final {
    std::mutex mutex;
    std::uint64_t count{};
    bool closed{};
    servicelib::detail::SingleUseEvent drained;
    void release() {
      bool notify;
      {
        std::lock_guard lock(mutex);
        --count;
        notify = closed && count == 0;
      }
      if (notify) drained.Send();
    }
  };
 public:
  class Token final {
   public:
    explicit Token(std::shared_ptr<State> state) : state_(std::move(state)) {}
    Token(const Token&) = delete;
    ~Token() {
      state_->release();
    }
   private:
    std::shared_ptr<State> state_;
  };
  std::shared_ptr<Token> acquire() {
    {
      std::lock_guard lock(state_->mutex);
      if (state_->closed) return {};
      ++state_->count;
    }
    try { return std::make_shared<Token>(state_); }
    catch (...) {
      state_->release();
      throw;
    }
  }
  bool close() {
    bool notify;
    {
      std::lock_guard lock(state_->mutex);
      if (state_->closed) return false;
      state_->closed = true;
      notify = state_->count == 0;
    }
    if (notify) state_->drained.Send();
    return true;
  }
  void wait() { state_->drained.Wait(); }
  boost::asio::awaitable<void> asyncWait() { co_await state_->drained.AsyncWait(); }
 private:
  std::shared_ptr<State> state_{std::make_shared<State>()};
};

template <typename Cell>
class StreamingRegistry;

template <typename Session>
struct StreamingCell final {
  typename StreamingRegistry<StreamingCell<Session>>::Registration registration;
  servicelib::detail::SingleUseEvent ready;
  std::shared_ptr<Session> session;
  std::exception_ptr error;
  std::atomic<bool> cancelled{false};
  std::atomic<bool> cancelSent{false};
  void markReady() {
    ready.Send();
    cancelIfReady();
  }
  void cancel() {
    cancelled.store(true, std::memory_order_release);
    cancelIfReady();
  }
 private:
  void cancelIfReady() {
    if (!ready.IsReady() || !cancelled.load(std::memory_order_acquire)) return;
    if constexpr (requires { session->rpc->cancel(); }) {
      if (session && session->rpc && !cancelSent.exchange(true))
        session->rpc->cancel();
    }
  }
};

// Publish before starting RPC. Each cell owns its registration so destruction
// removes the weak entry, including its control block, without waiting for Stop.
// Closing detaches the entries under the mutex and cancels outside it. The shared
// state also lets a registration safely outlive its endpoint's registry.
template <typename Cell>
class StreamingRegistry final {
  struct State final {
    std::mutex mutex;
    std::unordered_map<Cell*, std::weak_ptr<Cell>> cells;
    bool closed{};
  };
 public:
  class Registration final {
   public:
    Registration() = default;
    Registration(const Registration&) = delete;
    Registration& operator=(const Registration&) = delete;
    Registration(Registration&& other) noexcept
        : state_(std::move(other.state_)), cell_(std::exchange(other.cell_, nullptr)) {}
    Registration& operator=(Registration&& other) noexcept {
      if (this != &other) {
        reset();
        state_ = std::move(other.state_);
        cell_ = std::exchange(other.cell_, nullptr);
      }
      return *this;
    }
    ~Registration() { reset(); }

    void reset() {
      if (!state_) return;
      auto state = std::move(state_);
      std::lock_guard lock(state->mutex);
      state->cells.erase(std::exchange(cell_, nullptr));
    }

   private:
    friend class StreamingRegistry;
    Registration(std::shared_ptr<State> state, Cell* cell) noexcept
        : state_(std::move(state)), cell_(cell) {}
    std::shared_ptr<State> state_;
    Cell* cell_{};
  };

  StreamingRegistry() = default;
  StreamingRegistry(const StreamingRegistry&) = delete;
  StreamingRegistry& operator=(const StreamingRegistry&) = delete;
  ~StreamingRegistry() { close(); }
  [[nodiscard]] Registration add(const std::shared_ptr<Cell>& cell) {
    {
      std::lock_guard lock(state_->mutex);
      if (!state_->closed) {
        if (!state_->cells.emplace(cell.get(), cell).second)
          throw std::logic_error("streaming session already registered");
        return Registration{state_, cell.get()};
      }
    }
    cell->cancel();
    return {};
  }
  void close() {
    decltype(state_->cells) cells;
    {
      std::lock_guard lock(state_->mutex);
      state_->closed = true;
      cells.swap(state_->cells);
    }
    for (const auto& entry : cells) {
      if (auto cell = entry.second.lock()) cell->cancel();
    }
  }
 private:
  std::shared_ptr<State> state_{std::make_shared<State>()};
};
}  // namespace servicelib::datasink::grpc::detail

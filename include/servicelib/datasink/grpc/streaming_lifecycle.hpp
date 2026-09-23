#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
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

template <typename Session>
struct StreamingCell final {
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

// Publish before starting RPC. Closing detaches every accepted registration
// under the mutex; cancellation runs outside it. A racing publisher observes
// the closed sentinel and cancels
// its own cell. Weak registrations retain no finished sessions. Nodes, like
// the previous registration vector, are reclaimed at endpoint shutdown.
template <typename Cell>
class StreamingRegistry final {
  struct Node final {
    Node* next{};
    std::weak_ptr<Cell> cell;
  };
 public:
  ~StreamingRegistry() { close(); }
  void add(const std::shared_ptr<Cell>& cell) {
    auto node = std::make_unique<Node>();
    node->cell = cell;
    {
      std::lock_guard lock(mutex_);
      if (head_ != &closed_) {
        node->next = head_;
        head_ = node.release();
        return;
      }
    }
    cell->cancel();
  }
  void close() {
    Node* node;
    {
      std::lock_guard lock(mutex_);
      node = std::exchange(head_, &closed_);
    }
    while (node && node != &closed_) {
      auto* next = node->next;
      if (auto cell = node->cell.lock()) cell->cancel();
      delete node;
      node = next;
    }
  }
 private:
  Node closed_;
  std::mutex mutex_;
  Node* head_{};
};
}  // namespace servicelib::datasink::grpc::detail

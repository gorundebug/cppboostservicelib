#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <utility>
#include <servicelib/runtime/detail/sync.hpp>

namespace servicelib::datasink::grpc::detail {

// Admission and draining are one atomic state, so close cannot miss a reader.
// Only the explicitly synchronous shutdown boundary calls Wait().
class StreamingActivity final {
  static constexpr std::uint64_t kClosed = std::uint64_t{1} << 63;
  struct State final {
    std::atomic<std::uint64_t> count{0};
    servicelib::detail::SingleUseEvent drained;
  };
 public:
  class Token final {
   public:
    explicit Token(std::shared_ptr<State> state) : state_(std::move(state)) {}
    Token(const Token&) = delete;
    ~Token() {
      if (state_->count.fetch_sub(1, std::memory_order_acq_rel) == kClosed + 1)
        state_->drained.Send();
    }
   private:
    std::shared_ptr<State> state_;
  };
  std::shared_ptr<Token> acquire() {
    auto before = state_->count.load(std::memory_order_acquire);
    while (!(before & kClosed)) {
      if (state_->count.compare_exchange_weak(before, before + 1,
            std::memory_order_acq_rel, std::memory_order_acquire)) {
        try { return std::make_shared<Token>(state_); }
        catch (...) {
          if (state_->count.fetch_sub(1, std::memory_order_acq_rel) == kClosed + 1)
            state_->drained.Send();
          throw;
        }
      }
    }
    return {};
  }
  bool close() {
    const auto before = state_->count.fetch_or(kClosed, std::memory_order_acq_rel);
    if (before & kClosed) return false;
    if (before == 0) state_->drained.Send();
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

// Publish before starting RPC. Closing atomically detaches every accepted
// registration; a racing publisher observes the closed sentinel and cancels
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
    auto* before = head_.load(std::memory_order_acquire);
    while (before != &closed_) {
      node->next = before;
      if (head_.compare_exchange_weak(before, node.get(),
            std::memory_order_release, std::memory_order_acquire)) {
        static_cast<void>(node.release());
        return;
      }
    }
    cell->cancel();
  }
  void close() {
    auto* node = head_.exchange(&closed_, std::memory_order_acq_rel);
    while (node && node != &closed_) {
      auto* next = node->next;
      if (auto cell = node->cell.lock()) cell->cancel();
      delete node;
      node = next;
    }
  }
 private:
  Node closed_;
  std::atomic<Node*> head_{nullptr};
};
}  // namespace servicelib::datasink::grpc::detail

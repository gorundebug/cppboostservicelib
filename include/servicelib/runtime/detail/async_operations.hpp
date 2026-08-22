#pragma once

#include <condition_variable>
#include <cstddef>
#include <memory>
#include <mutex>

namespace servicelib::detail {

// Lifetime gate for transport operations that userver would keep attached to
// the current coroutine. Boost transport adapters acquire a token before
// detaching an Asio coroutine; stop closes admission and joins every token so
// endpoint state cannot be destroyed while a completion still references it.
class AsyncOperations final {
 private:
  struct State final {
    std::mutex mutex;
    std::condition_variable drained;
    std::size_t active{};
    bool accepting{true};
  };

 public:
  class Token final {
   public:
    Token(const Token&) = delete;
    Token& operator=(const Token&) = delete;
    ~Token() {
      std::lock_guard lock(state_->mutex);
      if (--state_->active == 0) state_->drained.notify_all();
    }

   private:
    friend class AsyncOperations;
    explicit Token(std::shared_ptr<State> state) : state_(std::move(state)) {}
    std::shared_ptr<State> state_;
  };

  [[nodiscard]] std::shared_ptr<Token> acquire() {
    std::lock_guard lock(state_->mutex);
    if (!state_->accepting) return {};
    ++state_->active;
    return std::shared_ptr<Token>(new Token(state_));
  }

  void start() {
    std::lock_guard lock(state_->mutex);
    state_->accepting = true;
  }

  void stopAndWait() {
    std::unique_lock lock(state_->mutex);
    state_->accepting = false;
    state_->drained.wait(lock, [this] { return state_->active == 0; });
  }

 private:
  std::shared_ptr<State> state_{std::make_shared<State>()};
};

}  // namespace servicelib::detail

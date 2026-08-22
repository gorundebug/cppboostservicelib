#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <string_view>
#include <utility>

namespace servicelib::async::runtime_detail::asio_handler_diagnostics {

enum class State : std::uint8_t { kNone, kQueued, kRunning, kSuspended };

struct Snapshot final {
  std::int64_t queued{};
  std::int64_t running{};
  std::int64_t suspended{};
};

class Counters final {
 public:
  void Transition(State current, State next) noexcept {
    if (current == next) return;
    Counter(next).fetch_add(next == State::kNone ? 0 : 1,
                            std::memory_order_relaxed);
    Counter(current).fetch_sub(current == State::kNone ? 0 : 1,
                               std::memory_order_relaxed);
  }

  [[nodiscard]] Snapshot Read() const noexcept {
    return {
        .queued = queued_.load(std::memory_order_relaxed),
        .running = running_.load(std::memory_order_relaxed),
        .suspended = suspended_.load(std::memory_order_relaxed),
    };
  }

 private:
  std::atomic<std::int64_t>& Counter(State state) noexcept {
    switch (state) {
      case State::kQueued:
        return queued_;
      case State::kRunning:
        return running_;
      case State::kSuspended:
        return suspended_;
      case State::kNone:
        return none_;
    }
    return none_;
  }

  std::atomic<std::int64_t> none_{0};
  std::atomic<std::int64_t> queued_{0};
  std::atomic<std::int64_t> running_{0};
  std::atomic<std::int64_t> suspended_{0};
};

inline Counters& GlobalCounters() noexcept {
  static Counters counters;
  return counters;
}

inline Snapshot Read() noexcept { return GlobalCounters().Read(); }

class Record final {
 public:
  explicit Record(State initial) { Transition(initial); }

  Record(const Record&) = delete;
  Record& operator=(const Record&) = delete;

  ~Record() { Transition(State::kNone); }

  void Transition(State next) noexcept {
    while (transitionLock_.test_and_set(std::memory_order_acquire)) {
    }
    GlobalCounters().Transition(state_, next);
    state_ = next;
    transitionLock_.clear(std::memory_order_release);
  }

 private:
  std::atomic_flag transitionLock_ = ATOMIC_FLAG_INIT;
  State state_{State::kNone};
};

inline bool IsImmediatelyQueued(std::string_view operation) noexcept {
  return operation == "execute" || operation == "post" ||
         operation == "dispatch" || operation == "defer";
}

class TrackedHandler {
 protected:
  TrackedHandler() = default;
  ~TrackedHandler() = default;

 private:
  friend class HandlerTracking;
  friend class Completion;
  std::shared_ptr<Record> record_;
};

class Completion final {
 public:
  explicit Completion(const TrackedHandler& handler)
      : record_(handler.record_) {}

  template <typename... Args>
  void InvocationBegin(Args&&...) noexcept {
    if (record_) record_->Transition(State::kRunning);
  }

  void InvocationEnd() noexcept {
    if (record_) record_->Transition(State::kNone);
  }

 private:
  std::shared_ptr<Record> record_;
};

class HandlerTracking final {
 public:
  template <typename ExecutionContext, typename Object, typename NativeHandle>
  static void Creation(ExecutionContext&, TrackedHandler& handler,
                       const char*, Object*, NativeHandle,
                       const char* operation) {
    const auto initial = IsImmediatelyQueued(operation ? operation : "")
                             ? State::kQueued
                             : State::kSuspended;
    handler.record_ = std::make_shared<Record>(initial);
  }

  template <typename... Args>
  static void ReactorOperation(const TrackedHandler& handler, Args&&...) {
    if (handler.record_) handler.record_->Transition(State::kQueued);
  }
};

}  // namespace servicelib::async::runtime_detail::asio_handler_diagnostics

// Boost.Asio includes this file through BOOST_ASIO_CUSTOM_HANDLER_TRACKING.
// Keep the hook profiling-only: no ServiceLib executor wraps or relocates a
// handler, and normal builds do not compile this base into Asio operations.
#define BOOST_ASIO_INHERIT_TRACKED_HANDLER                                  \
  : public servicelib::async::runtime_detail::asio_handler_diagnostics::    \
        TrackedHandler
#define BOOST_ASIO_ALSO_INHERIT_TRACKED_HANDLER                             \
  , public servicelib::async::runtime_detail::asio_handler_diagnostics::    \
        TrackedHandler
#define BOOST_ASIO_HANDLER_TRACKING_INIT (void)0
#define BOOST_ASIO_HANDLER_LOCATION(args) (void)0
#define BOOST_ASIO_HANDLER_CREATION(args)                                   \
  servicelib::async::runtime_detail::asio_handler_diagnostics::             \
      HandlerTracking::Creation args
#define BOOST_ASIO_HANDLER_COMPLETION(args)                                 \
  servicelib::async::runtime_detail::asio_handler_diagnostics::Completion   \
      tracked_completion args
#define BOOST_ASIO_HANDLER_INVOCATION_BEGIN(args)                           \
  tracked_completion.InvocationBegin args
#define BOOST_ASIO_HANDLER_INVOCATION_END tracked_completion.InvocationEnd()
#define BOOST_ASIO_HANDLER_OPERATION(args) (void)0
#define BOOST_ASIO_HANDLER_REACTOR_REGISTRATION(args) (void)0
#define BOOST_ASIO_HANDLER_REACTOR_DEREGISTRATION(args) (void)0
#define BOOST_ASIO_HANDLER_REACTOR_READ_EVENT 1
#define BOOST_ASIO_HANDLER_REACTOR_WRITE_EVENT 2
#define BOOST_ASIO_HANDLER_REACTOR_ERROR_EVENT 4
#define BOOST_ASIO_HANDLER_REACTOR_EVENTS(args) (void)0
#define BOOST_ASIO_HANDLER_REACTOR_OPERATION(args)                          \
  servicelib::async::runtime_detail::asio_handler_diagnostics::             \
      HandlerTracking::ReactorOperation args

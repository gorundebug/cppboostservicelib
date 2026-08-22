/*
 * context.hpp
 * C++ streams API
 *
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the
 * [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */
#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <optional>
#include <stop_token>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <servicelib/runtime/environment/tracing/tracing.hpp>

namespace servicelib {

class AsyncCompletionState;

// A retained logical call-frame. Asynchronous transport adapters keep one
// token until their operation has really completed; synchronous consumers do
// not need to know that the mechanism exists.
class AsyncCompletionToken final {
 public:
  explicit AsyncCompletionToken(
      std::shared_ptr<AsyncCompletionState> state) noexcept;
  AsyncCompletionToken(const AsyncCompletionToken&) = delete;
  AsyncCompletionToken& operator=(const AsyncCompletionToken&) = delete;
  AsyncCompletionToken(AsyncCompletionToken&&) noexcept = default;
  AsyncCompletionToken& operator=(AsyncCompletionToken&&) noexcept = default;
  ~AsyncCompletionToken();

 private:
  std::shared_ptr<AsyncCompletionState> state_;
};

class AsyncCompletionState final
    : public std::enable_shared_from_this<AsyncCompletionState> {
 public:
  explicit AsyncCompletionState(
      std::function<void()> completion,
      std::shared_ptr<AsyncCompletionToken> parent = {})
      : completion_(std::move(completion)), parent_(std::move(parent)) {}

  [[nodiscard]] std::shared_ptr<AsyncCompletionToken> retain() {
    auto pending = pending_.load(std::memory_order_acquire);
    while (pending != 0) {
      if (pending_.compare_exchange_weak(
              pending, pending + 1, std::memory_order_acq_rel,
              std::memory_order_acquire)) {
        return std::make_shared<AsyncCompletionToken>(shared_from_this());
      }
    }
    // An asynchronous boundary may still carry a copied MessageContext after
    // the logical call frame has completed. Never revive that frame.
    return nullptr;
  }

  void release() noexcept {
    if (pending_.fetch_sub(1, std::memory_order_acq_rel) != 1) return;
    auto completion = std::move(completion_);
    try {
      completion();
    } catch (...) {
      // Completion bookkeeping must never replace business semantics.
    }
    parent_.reset();
  }

  [[nodiscard]] static std::shared_ptr<AsyncCompletionState> make(
      std::function<void()> completion,
      std::shared_ptr<AsyncCompletionToken> parent = {}) {
    return std::make_shared<AsyncCompletionState>(std::move(completion),
                                                   std::move(parent));
  }

 private:
  std::atomic<std::size_t> pending_{1};
  std::function<void()> completion_;
  std::shared_ptr<AsyncCompletionToken> parent_;
};

inline AsyncCompletionToken::AsyncCompletionToken(
    std::shared_ptr<AsyncCompletionState> state) noexcept
    : state_(std::move(state)) {}

inline AsyncCompletionToken::~AsyncCompletionToken() {
  if (state_) state_->release();
}

using Deadline = std::optional<std::chrono::steady_clock::time_point>;

// Generic context state: cancellation + deadline only. Base for all context
// kinds (message, pool lifecycle, ...).
// Go analog: context.Context. Python analog: Context (cancelled/time_left).
struct ContextStateBase {
  std::stop_token stopToken;
  Deadline deadline;
  std::vector<std::stop_token> externalCancellations;
  bool traceSamplingEnabled{};
};

// Lightweight, cheaply-copyable context carrying cancellation + deadline.
// Used generically wherever stream-specific fields (streamId/priority/trace)
// aren't needed — e.g. pool::ITaskPool/IPriorityTaskPool lifecycle
// (start/stop/addTask). MessageContext derives from this and remains
// implicitly convertible to Context (one shared_ptr copy — the pointee is
// never sliced, so a Context obtained from a MessageContext still keeps the
// full state alive; it just can't see the derived fields through this type).
class Context {
 public:
  Context() : state_(std::make_shared<ContextStateBase>()) {}

  [[nodiscard]] bool cancelled() const noexcept {
    return state_->stopToken.stop_requested() || externallyCancelled() ||
           (state_->deadline &&
            *state_->deadline <= std::chrono::steady_clock::now());
  }

  [[nodiscard]] const Deadline& deadline() const noexcept {
    return state_->deadline;
  }

  // Raw token, for registering a std::stop_callback (e.g. to react to
  // cancellation that happens *after* a consumer already captured this
  // Context — see pool::ITaskPool::addTask).
  [[nodiscard]] std::stop_token stopToken() const noexcept {
    return state_->stopToken;
  }

  // Additional cancellation sources attached by transport adapters. Pools
  // subscribe to every token so these have the same post-admission semantics
  // as Go context.Context.Done(), rather than being visible only when polled.
  [[nodiscard]] const std::vector<std::stop_token>& externalStopTokens()
      const noexcept {
    return state_->externalCancellations;
  }

  [[nodiscard]] bool samplingEnabled() const noexcept {
    return state_->traceSamplingEnabled;
  }

  [[nodiscard]] Context withDeadline(Deadline d) const & {
    auto s = std::make_shared<ContextStateBase>(*state_);
    s->deadline = std::move(d);
    return Context(std::move(s));
  }

  [[nodiscard]] Context withDeadline(Deadline d) && {
    auto s = takeOrCloneBase();
    s->deadline = std::move(d);
    return Context(std::move(s));
  }

  template <typename Rep, typename Period>
  [[nodiscard]] Context bounded(
      std::chrono::duration<Rep, Period> timeout) const {
    const auto nonNegative =
        std::max(timeout, std::chrono::duration<Rep, Period>::zero());
    const auto candidate =
        std::chrono::steady_clock::now() +
        std::chrono::duration_cast<std::chrono::steady_clock::duration>(
            nonNegative);
    if (state_->deadline && *state_->deadline <= candidate) {
      return *this;
    }
    return withDeadline(candidate);
  }

  [[nodiscard]] Context withStopToken(std::stop_token token) const & {
    auto s = std::make_shared<ContextStateBase>(*state_);
    s->stopToken = std::move(token);
    return Context(std::move(s));
  }

  [[nodiscard]] Context withStopToken(std::stop_token token) && {
    auto s = takeOrCloneBase();
    s->stopToken = std::move(token);
    return Context(std::move(s));
  }

  [[nodiscard]] Context withExternalCancellation(
      std::stop_token token) const & {
    auto s = std::make_shared<ContextStateBase>(*state_);
    s->externalCancellations.push_back(std::move(token));
    return Context(std::move(s));
  }

  [[nodiscard]] Context withExternalCancellation(std::stop_token token) && {
    auto s = takeOrCloneBase();
    s->externalCancellations.push_back(std::move(token));
    return Context(std::move(s));
  }

  [[nodiscard]] Context withSampling(bool enabled) const & {
    auto s = std::make_shared<ContextStateBase>(*state_);
    s->traceSamplingEnabled = enabled;
    return Context(std::move(s));
  }

  [[nodiscard]] Context withSampling(bool enabled) && {
    auto s = takeOrCloneBase();
    s->traceSamplingEnabled = enabled;
    return Context(std::move(s));
  }

 protected:
  explicit Context(std::shared_ptr<ContextStateBase> s) noexcept
      : state_(std::move(s)) {}

  std::shared_ptr<ContextStateBase> state_;

 private:
  std::shared_ptr<ContextStateBase> takeOrCloneBase() {
    if (state_.unique()) return std::move(state_);
    return std::make_shared<ContextStateBase>(*state_);
  }

  [[nodiscard]] bool externallyCancelled() const noexcept {
    for (const auto& cancellation : state_->externalCancellations) {
      if (cancellation.stop_requested()) {
        return true;
      }
    }
    return false;
  }
};

// Message-specific state: adds streamId/priority/trace on top of the
// generic cancellation+deadline facet.
struct ContextState final : ContextStateBase {
  std::string streamId;
  int priority{};
  bool hasPriority{};
  tracing::SpanContext trace;
  std::shared_ptr<AsyncCompletionState> completion;
};

class MessageContext final : public Context {
 public:
  MessageContext() : Context(std::make_shared<ContextState>()) {}

  [[nodiscard]] std::string_view streamId() const noexcept {
    return derived()->streamId;
  }

  [[nodiscard]] int priority() const noexcept { return derived()->priority; }

  [[nodiscard]] bool hasPriority() const noexcept {
    return derived()->hasPriority;
  }

  [[nodiscard]] const tracing::SpanContext& trace() const noexcept {
    return derived()->trace;
  }

  [[nodiscard]] std::shared_ptr<AsyncCompletionToken> retainCompletion()
      const {
    const auto& completion = derived()->completion;
    return completion ? completion->retain() : nullptr;
  }

  [[nodiscard]] MessageContext withCompletion(
      std::shared_ptr<AsyncCompletionState> completion) const & {
    auto s = cloneDerived();
    s->completion = std::move(completion);
    return MessageContext(std::move(s));
  }

  [[nodiscard]] MessageContext withCompletion(
      std::shared_ptr<AsyncCompletionState> completion) && {
    auto s = takeOrCloneDerived();
    s->completion = std::move(completion);
    return MessageContext(std::move(s));
  }

  [[nodiscard]] MessageContext withoutCompletion() const & {
    return withCompletion({});
  }

  [[nodiscard]] MessageContext withoutCompletion() && {
    return std::move(*this).withCompletion({});
  }

  [[nodiscard]] MessageContext withPriority(int p) const & {
    auto s = cloneDerived();
    s->priority = p;
    s->hasPriority = true;
    return MessageContext(std::move(s));
  }

  [[nodiscard]] MessageContext withPriority(int p) && {
    auto s = takeOrCloneDerived();
    s->priority = p;
    s->hasPriority = true;
    return MessageContext(std::move(s));
  }

  [[nodiscard]] MessageContext withStreamId(std::string id) const & {
    auto s = cloneDerived();
    s->streamId = std::move(id);
    return MessageContext(std::move(s));
  }

  [[nodiscard]] MessageContext withStreamId(std::string id) && {
    auto s = takeOrCloneDerived();
    s->streamId = std::move(id);
    return MessageContext(std::move(s));
  }

  // Hides Context::withDeadline — same effect, but returns MessageContext
  // (and clones the full derived state) instead of slicing to Context.
  [[nodiscard]] MessageContext withDeadline(Deadline d) const & {
    auto s = cloneDerived();
    s->deadline = std::move(d);
    return MessageContext(std::move(s));
  }

  [[nodiscard]] MessageContext withDeadline(Deadline d) && {
    auto s = takeOrCloneDerived();
    s->deadline = std::move(d);
    return MessageContext(std::move(s));
  }

  [[nodiscard]] MessageContext withStopToken(std::stop_token token) const & {
    auto s = cloneDerived();
    s->stopToken = std::move(token);
    return MessageContext(std::move(s));
  }

  [[nodiscard]] MessageContext withStopToken(std::stop_token token) && {
    auto s = takeOrCloneDerived();
    s->stopToken = std::move(token);
    return MessageContext(std::move(s));
  }

  [[nodiscard]] MessageContext withExternalCancellation(
      std::stop_token token) const & {
    auto s = cloneDerived();
    s->externalCancellations.push_back(std::move(token));
    return MessageContext(std::move(s));
  }

  [[nodiscard]] MessageContext withExternalCancellation(
      std::stop_token token) && {
    auto s = takeOrCloneDerived();
    s->externalCancellations.push_back(std::move(token));
    return MessageContext(std::move(s));
  }

  [[nodiscard]] MessageContext withSampling(bool enabled) const & {
    auto s = cloneDerived();
    s->traceSamplingEnabled = enabled;
    return MessageContext(std::move(s));
  }

  [[nodiscard]] MessageContext withSampling(bool enabled) && {
    auto s = takeOrCloneDerived();
    s->traceSamplingEnabled = enabled;
    return MessageContext(std::move(s));
  }

  [[nodiscard]] MessageContext withTrace(tracing::SpanContext tc) const & {
    auto s = cloneDerived();
    s->trace = std::move(tc);
    return MessageContext(std::move(s));
  }

  [[nodiscard]] MessageContext withTrace(tracing::SpanContext tc) && {
    auto s = takeOrCloneDerived();
    s->trace = std::move(tc);
    return MessageContext(std::move(s));
  }

 private:
  explicit MessageContext(std::shared_ptr<ContextState> s) noexcept
      : Context(std::move(s)) {}

  // Safe: state_ is always a ContextState — only this class's constructors
  // ever assign into the inherited Context::state_.
  const ContextState* derived() const noexcept {
    return static_cast<const ContextState*>(state_.get());
  }

  std::shared_ptr<ContextState> cloneDerived() const {
    return std::make_shared<ContextState>(*derived());
  }

  std::shared_ptr<ContextState> takeOrCloneDerived() {
    if (state_.unique()) {
      return std::static_pointer_cast<ContextState>(std::move(state_));
    }
    return cloneDerived();
  }
};

}  // namespace servicelib

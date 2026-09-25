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
#include <mutex>
#include <optional>
#include <stop_token>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <servicelib/runtime/environment/tracing/tracing.hpp>

namespace servicelib {

namespace detail {
struct ContextKeyIdentity final {};
struct LocalContextValue {
  std::shared_ptr<const ContextKeyIdentity> key;
  std::shared_ptr<const LocalContextValue> parent;
  virtual ~LocalContextValue() = default;
};
template <typename T>
struct TypedLocalContextValue final : LocalContextValue {
  std::shared_ptr<T> value;
};
}  // namespace detail

// Identity and value type travel together; values never enter wire metadata.
template <typename T>
class ContextKey final {
  friend class MessageContext;
  std::shared_ptr<const detail::ContextKeyIdentity> identity_{
      std::make_shared<const detail::ContextKeyIdentity>()};
};

class AsyncCompletionState;

// A retained logical call-frame. Asynchronous transport adapters keep one
// token until their operation has really completed; synchronous consumers do
// not need to know that the mechanism exists.
class AsyncCompletionToken final {
 public:
  AsyncCompletionToken() noexcept = default;
  explicit AsyncCompletionToken(
      std::shared_ptr<AsyncCompletionState> state) noexcept;
  AsyncCompletionToken(const AsyncCompletionToken&) = delete;
  AsyncCompletionToken& operator=(const AsyncCompletionToken&) = delete;
  AsyncCompletionToken(AsyncCompletionToken&&) noexcept = default;
  AsyncCompletionToken& operator=(AsyncCompletionToken&& other) noexcept;
  ~AsyncCompletionToken();

  void reset() noexcept;
  [[nodiscard]] explicit operator bool() const noexcept {
    return static_cast<bool>(state_);
  }

 private:
  std::shared_ptr<AsyncCompletionState> state_;
};

class AsyncCompletionState
    : public std::enable_shared_from_this<AsyncCompletionState> {
 public:
  explicit AsyncCompletionState(
      std::function<void()> completion,
      std::shared_ptr<AsyncCompletionToken> parent = {})
      : completion_(std::move(completion)), parent_(std::move(parent)) {}

  [[nodiscard]] std::shared_ptr<AsyncCompletionToken> retain() {
    auto token = retainToken();
    if (!token) return nullptr;
    return std::make_shared<AsyncCompletionToken>(std::move(token));
  }

  // Unique operation owners can keep their lease inline. The shared-token
  // API above remains available for callbacks that share one logical lease.
  [[nodiscard]] AsyncCompletionToken retainToken() {
    auto owner = shared_from_this();
    std::lock_guard lock(pendingMutex_);
    if (pending_ != 0) {
      ++pending_;
      return AsyncCompletionToken(std::move(owner));
    }
    // An asynchronous boundary may still carry a copied MessageContext after
    // the logical call frame has completed. Never revive that frame.
    return {};
  }

  void release() noexcept {
    {
      std::lock_guard lock(pendingMutex_);
      if (--pending_ != 0) return;
    }
    auto completion = std::move(completion_);
    try {
      completion();
    } catch (...) {
      // Completion bookkeeping must never replace business semantics.
    }
    parent_.reset();
    parentToken_.reset();
  }

  [[nodiscard]] static std::shared_ptr<AsyncCompletionState> make(
      std::function<void()> completion,
      std::shared_ptr<AsyncCompletionToken> parent = {}) {
    return std::make_shared<AsyncCompletionState>(std::move(completion),
                                                   std::move(parent));
  }

 protected:
  // A coallocated call frame installs its completion before exposing the
  // state to consumers. Never reinitialize an active or completed frame.
  void initializeCompletion(std::function<void()> completion,
                            AsyncCompletionToken parent) {
    completion_ = std::move(completion);
    parentToken_ = std::move(parent);
  }

 private:
  std::mutex pendingMutex_;
  std::size_t pending_{1};
  std::function<void()> completion_;
  std::shared_ptr<AsyncCompletionToken> parent_;
  AsyncCompletionToken parentToken_;
};

inline AsyncCompletionToken::AsyncCompletionToken(
    std::shared_ptr<AsyncCompletionState> state) noexcept
    : state_(std::move(state)) {}

inline AsyncCompletionToken::~AsyncCompletionToken() {
  reset();
}

inline AsyncCompletionToken& AsyncCompletionToken::operator=(
    AsyncCompletionToken&& other) noexcept {
  if (this != &other) {
    auto previous = std::move(state_);
    state_ = std::move(other.state_);
    if (previous) previous->release();
  }
  return *this;
}

inline void AsyncCompletionToken::reset() noexcept {
  auto state = std::move(state_);
  if (state) state->release();
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
  std::shared_ptr<const detail::LocalContextValue> localValues;
};

class MessageContext final : public Context {
 public:
  MessageContext() : Context(std::make_shared<ContextState>()) {}

  template <typename T>
  [[nodiscard]] MessageContext withLocalValue(
      const ContextKey<T>& key, std::shared_ptr<T> value) const {
    auto binding = std::make_shared<detail::TypedLocalContextValue<T>>();
    binding->key = key.identity_;
    binding->parent = derived()->localValues;
    binding->value = std::move(value);
    auto state = cloneDerived();
    state->localValues = std::move(binding);
    return MessageContext(std::move(state));
  }

  template <typename T>
  [[nodiscard]] std::shared_ptr<T> localValue(const ContextKey<T>& key) const {
    for (auto binding = derived()->localValues; binding; binding = binding->parent) {
      if (binding->key == key.identity_) {
        return static_cast<const detail::TypedLocalContextValue<T>&>(*binding).value;
      }
    }
    return {};
  }

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

  [[nodiscard]] AsyncCompletionToken retainCompletionToken() const {
    const auto& completion = derived()->completion;
    return completion ? completion->retainToken() : AsyncCompletionToken{};
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
    if (!derived()->completion) return *this;
    return withCompletion({});
  }

  [[nodiscard]] MessageContext withoutCompletion() && {
    if (!derived()->completion) return std::move(*this);
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

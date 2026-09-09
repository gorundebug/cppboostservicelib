#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <servicelib/runtime/datasource.hpp>
#include <servicelib/runtime/environment/tracing/tracing.hpp>
#include <servicelib/runtime/detail/sync.hpp>

// Shared result state only; no endpoint or producer lifecycle lives here.
namespace servicelib::datasource::localsource {

template <typename State, typename T, typename R, typename E>
struct PendingResult final {
  using Callback = std::function<bool(
      MessageContext, SourceStreamContext<T, R, E>&, State&, const R&)>;

  PendingResult(State stateValue, std::shared_ptr<tracing::Span> requestSpan)
      : state(std::move(stateValue)), span(std::move(requestSpan)) {}

  State state;
  std::shared_ptr<tracing::Span> span;
  servicelib::detail::SingleUseEvent done;
  std::atomic<bool> wakeSent{false};
  std::atomic<bool> completed{false};
  std::shared_mutex lifetimeMutex;
  std::mutex callbacksMutex;
  std::unordered_map<std::string, std::shared_ptr<Callback>> callbacks;
};

template <typename State, typename T, typename R, typename E>
class ResultContext final {
 public:
  using Callback = typename PendingResult<State, T, R, E>::Callback;

  explicit ResultContext(std::shared_ptr<PendingResult<State, T, R, E>> result)
      : result_(std::move(result)) {}

  // The registered callable is retained and may be invoked concurrently.
  // Handlers must synchronize mutable captures, just as shared handler State.
  void setResultCallback(std::string messageId, Callback callback) {
    auto sharedCallback = std::make_shared<Callback>(std::move(callback));
    std::lock_guard lock(result_->callbacksMutex);
    result_->callbacks[std::move(messageId)] = std::move(sharedCallback);
  }

  void done() noexcept {
    result_->completed.store(true, std::memory_order_release);
    bool expected = false;
    if (result_->wakeSent.compare_exchange_strong(expected, true,
                                                  std::memory_order_acq_rel)) {
      tracing::SpanEvent(result_->span.get(), "done_called");
      result_->done.Send();
    }
  }

 private:
  std::shared_ptr<PendingResult<State, T, R, E>> result_;
};

}  // namespace servicelib::datasource::localsource

namespace servicelib::datasource::kafka {
template <typename State, typename T, typename R, typename E>
using ResultContext = localsource::ResultContext<State, T, R, E>;
template <typename State, typename T, typename R, typename E>
using PendingResult = localsource::PendingResult<State, T, R, E>;
}  // namespace servicelib::datasource::kafka

#pragma once

#include <functional>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>
#include <variant>

#include <boost/config.hpp>
#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/spawn.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/use_future.hpp>

namespace servicelib::detail {

// A stackful boundary for synchronous business code, not a graph-edge queue.
// Only runtime I/O waits suspend this stack; FunctionCall stays direct.
class CooperativeExecution final {
 public:
  [[nodiscard]] static bool Active() noexcept { return Current() != nullptr; }

  [[nodiscard]] static const void* CurrentOwner() noexcept {
    const auto* scope = Current();
    return scope ? scope->owner_ : nullptr;
  }

  // Launching may itself be called from a migrating business stack. Keep
  // Asio's TLS-backed frame allocation inside this non-suspending boundary.
  template <typename Function>
  BOOST_NOINLINE static void Post(boost::asio::any_io_executor executor,
                                  Function function) {
    boost::asio::co_spawn(std::move(executor), Run(std::move(function)),
                         boost::asio::detached);
  }

  template <typename Function>
  static boost::asio::awaitable<std::invoke_result_t<Function>> Run(
      Function function, const void* owner = nullptr) {
    const auto executor = co_await boost::asio::this_coro::executor;
    co_await boost::asio::post(boost::asio::use_awaitable);
    auto scope = std::shared_ptr<CooperativeExecution>(new CooperativeExecution(executor, owner));
    const boost::asio::any_io_executor entryExecutor{ScopedExecutor{executor, scope}};
    co_return co_await boost::asio::spawn(
        entryExecutor,
        [scope, function = std::move(function)](boost::asio::yield_context yield) mutable
            -> std::invoke_result_t<Function> {
          // MessageContext carries cancellation. Drain callbacks before
          // unwinding business frames that their results may still reference.
          yield.reset_cancellation_state(boost::asio::disable_cancellation());
          scope->yield_ = &yield;
          return std::invoke(std::move(function));
        },
        boost::asio::use_awaitable);
  }

  // Construct async frames on the worker's ordinary stack, not on the
  // migrating business stack. Inlined Asio frame allocation uses TLS caches;
  // the compiler may otherwise reuse the previous worker's cache address.
  template <typename Operation>
  static typename std::invoke_result_t<Operation>::value_type Await(Operation operation) {
    using T = typename std::invoke_result_t<Operation>::value_type;
    auto& scope = *Current();
    std::exception_ptr error;
    using Value = std::conditional_t<std::is_void_v<T>, std::monostate, T>;
    std::optional<Value> value;
    // Keep values and exceptions on the suspended caller's stack. The wake-up
    // token carries no exception_ptr (older Asio yield handlers rethrow even a
    // null exception_ptr); only this boundary decides whether to rethrow.
    boost::asio::async_initiate<boost::asio::yield_context, void()>(
        [&](auto ready) {
          // Asio invokes the initiation on the resumer's stack, after the
          // business stack suspends. Independent async work must not inherit
          // the suspended business execution.
          ResumeScope inactive{nullptr};
          if constexpr (std::is_void_v<T>) {
            boost::asio::co_spawn(scope.executor_, std::move(operation),
                [&scope, &error, ready = std::move(ready)](std::exception_ptr failure) mutable {
                  error = std::move(failure);
                  ResumeScope active{&scope};
                  std::move(ready)();
                });
          } else {
            boost::asio::co_spawn(scope.executor_, std::move(operation),
                [&scope, &error, &value, ready = std::move(ready)](
                    std::exception_ptr failure, T result) mutable {
                  error = std::move(failure);
                  if (!error) {
                    try { value.emplace(std::move(result)); }
                    catch (...) { error = std::current_exception(); }
                  }
                  ResumeScope active{&scope};
                  std::move(ready)();
                });
          }
        }, *scope.yield_);
    if (error) std::rethrow_exception(error);
    if constexpr (!std::is_void_v<T>) return std::move(*value);
  }

  // External synchronous callers may wait on their own thread. Runtime
  // executor entry points use Run(); this fallback never pumps the reactor.
  template <typename Operation>
  static typename std::invoke_result_t<Operation>::value_type Await(
      boost::asio::any_io_executor executor, Operation operation) {
    if (Active()) return Await(std::move(operation));
    return boost::asio::co_spawn(std::move(executor), std::move(operation),
                                 boost::asio::use_future).get();
  }

 private:
  explicit CooperativeExecution(boost::asio::any_io_executor executor, const void* owner)
      : executor_(std::move(executor)), owner_(owner) {}

  // Never inline a TLS lookup into a migrating business stack: the compiler
  // may otherwise keep the old thread's TLS base across a context switch.
  BOOST_NOINLINE static CooperativeExecution* Current() noexcept { return current_; }

  // These guards live on a worker's ordinary stack, outside the stackful
  // coroutine. Their construction and destruction always run on one thread.
  struct ResumeScope final {
    explicit ResumeScope(CooperativeExecution* scope)
        : previous(std::exchange(current_, scope)) {}
    ~ResumeScope() { current_ = previous; }
    CooperativeExecution* previous;
  };

  class ScopedExecutor final {
   public:
    ScopedExecutor(boost::asio::any_io_executor executor,
                   std::shared_ptr<CooperativeExecution> scope)
        : executor_(std::move(executor)), scope_(std::move(scope)) {}

    template <typename Function>
    void execute(Function&& function) const {
      executor_.execute(
          [scope = scope_, function = std::forward<Function>(function)]() mutable {
            ResumeScope active{scope.get()};
            std::move(function)();
          });
    }

    template <typename Property>
    auto query(const Property& property) const
        noexcept(noexcept(boost::asio::query(
            std::declval<const boost::asio::any_io_executor&>(), property)))
        -> decltype(boost::asio::query(
            std::declval<const boost::asio::any_io_executor&>(), property)) {
      return boost::asio::query(executor_, property);
    }

    template <typename Property>
      requires boost::asio::can_require<const boost::asio::any_io_executor&, Property>::value
    ScopedExecutor require(const Property& property) const {
      return {boost::asio::require(executor_, property), scope_};
    }

    template <typename Property>
      requires boost::asio::can_prefer<const boost::asio::any_io_executor&, Property>::value
    ScopedExecutor prefer(const Property& property) const {
      return {boost::asio::prefer(executor_, property), scope_};
    }

    friend bool operator==(const ScopedExecutor& left, const ScopedExecutor& right) noexcept {
      return left.executor_ == right.executor_ && left.scope_ == right.scope_;
    }

   private:
    boost::asio::any_io_executor executor_;
    std::shared_ptr<CooperativeExecution> scope_;
  };

  boost::asio::any_io_executor executor_;
  boost::asio::yield_context* yield_{};
  const void* owner_{};
  inline static thread_local CooperativeExecution* current_{};
};

}  // namespace servicelib::detail

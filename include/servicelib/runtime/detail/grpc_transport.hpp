#pragma once

#include <servicelib/runtime/detail/grpc_context.hpp>
#include <servicelib/runtime/detail/sync.hpp>

#include <agrpc/client_rpc.hpp>
#include <agrpc/default_server_rpc_traits.hpp>
#include <agrpc/register_awaitable_rpc_handler.hpp>
#include <agrpc/server_rpc.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/system/error_code.hpp>
#include <grpcpp/support/status.h>

#include <exception>
#include <functional>
#include <memory>
#include <optional>
#include <stop_token>
#include <string>
#include <utility>
#include <vector>

namespace servicelib::grpc_transport {

template <typename Response>
struct UnaryResult final {
  Response response;
  grpc::Status status;

  [[nodiscard]] bool ok() const noexcept { return status.ok(); }
};

namespace detail {

struct NotifyWhenDoneTraits : agrpc::DefaultServerRPCTraits {
  static constexpr bool NOTIFY_WHEN_DONE = true;
};

template <auto RequestMethod>
using ObservableServerRPC =
    agrpc::ServerRPC<RequestMethod, NotifyWhenDoneTraits>;

class ServerCancellation final {
 public:
  ServerCancellation() : state_(std::make_shared<State>()) {}

  [[nodiscard]] std::stop_token token() const noexcept {
    return state_->source.get_token();
  }

  void Complete(bool cancelled) const noexcept {
    if (cancelled) state_->source.request_stop();
    state_->done.Send();
  }

  boost::asio::awaitable<void> Wait() {
    // Keep the completion state alive until the resumed waiter has detached
    // from it. wait_for_done may invoke its callback on a different runtime
    // worker and wake this coroutine before Complete() itself has returned.
    auto state = state_;
    co_await state->done.AsyncWait();
  }

 private:
  struct State final {
    std::stop_source source;
    servicelib::detail::SingleUseEvent done;
  };

  std::shared_ptr<State> state_;
};

template <typename RPC>
class ServerCancellationCallback final {
 public:
  using executor_type = boost::asio::any_io_executor;

  ServerCancellationCallback(
      RPC& rpc, ServerCancellation& cancellation,
      executor_type executor)
      : rpc_(&rpc),
        // The callback owns the same completion state as the coroutine. This
        // is required because wake-up and callback return may run concurrently.
        cancellation_(cancellation),
        executor_(std::move(executor)) {}

  void operator()(const boost::system::error_code&) const noexcept {
    cancellation_.Complete(rpc_->context().IsCancelled());
  }

  [[nodiscard]] executor_type get_executor() const noexcept {
    return executor_;
  }

 private:
  RPC* rpc_;
  ServerCancellation cancellation_;
  executor_type executor_;
};

template <typename RPC>
void ObserveServerCancellation(RPC& rpc, ServerCancellation& cancellation,
                               boost::asio::any_io_executor executor) {
  rpc.wait_for_done(ServerCancellationCallback<RPC>{
      rpc, cancellation, std::move(executor)});
}

class ClientCancellation final {
 public:
  ClientCancellation(const MessageContext& message, grpc::ClientContext& rpc) {
    Add(message.stopToken(), rpc);
    for (const auto& token : message.externalStopTokens()) Add(token, rpc);
  }

 private:
  using Callback = std::stop_callback<std::function<void()>>;
  void Add(std::stop_token token, grpc::ClientContext& rpc) {
    if (!token.stop_possible()) return;
    callbacks_.push_back(std::make_unique<Callback>(
        token, [&rpc] { rpc.TryCancel(); }));
  }
  std::vector<std::unique_ptr<Callback>> callbacks_;
};

}  // namespace detail

template <auto PrepareAsync, typename Stub>
boost::asio::awaitable<
    UnaryResult<typename agrpc::ClientRPC<PrepareAsync>::Response>>
UnaryCall(agrpc::GrpcContext& grpcContext, Stub& stub,
          const MessageContext& message,
          const typename agrpc::ClientRPC<PrepareAsync>::Request& request) {
  using RPC = agrpc::ClientRPC<PrepareAsync>;
  grpc::ClientContext clientContext;
  InjectContext(message, clientContext);
  detail::ClientCancellation cancellation(message, clientContext);
  typename RPC::Response response;
  auto status = co_await RPC::request(grpcContext, stub, clientContext,
                                      request, response,
                                      boost::asio::use_awaitable);
  co_return UnaryResult<typename RPC::Response>{std::move(response),
                                                std::move(status)};
}

template <auto RequestMethod, typename AsyncService, typename Handler>
void RegisterUnarySource(agrpc::GrpcContext& grpcContext,
                         AsyncService& service, Handler handler,
                         boost::asio::any_io_executor handlerExecutor = {}) {
  using RPC = detail::ObservableServerRPC<RequestMethod>;
  if (!handlerExecutor) handlerExecutor = grpcContext.get_executor();
  agrpc::register_awaitable_rpc_handler<RPC>(
      grpcContext, service,
      [handler = std::move(handler)](
          RPC& rpc, typename RPC::Request& request) mutable
          -> boost::asio::awaitable<void> {
        detail::ServerCancellation cancellation;
        detail::ObserveServerCancellation(
            rpc, cancellation, co_await boost::asio::this_coro::executor);
        std::optional<typename RPC::Response> response;
        grpc::Status status = grpc::Status::OK;
        try {
          response.emplace(co_await std::invoke(
              handler,
              ExtractContext(rpc.context()).withExternalCancellation(
                  cancellation.token()),
              request));
        } catch (const std::exception& error) {
          status = grpc::Status(grpc::StatusCode::INTERNAL, error.what());
        } catch (...) {
          status = grpc::Status(grpc::StatusCode::INTERNAL,
                                "unhandled unary handler error");
        }
        if (status.ok())
          (void)co_await rpc.finish(*response, status,
                                    boost::asio::use_awaitable);
        else
          (void)co_await rpc.finish_with_error(status,
                                               boost::asio::use_awaitable);
        co_await cancellation.Wait();
      },
      boost::asio::bind_executor(std::move(handlerExecutor),
                                 boost::asio::detached));
}

}  // namespace servicelib::grpc_transport

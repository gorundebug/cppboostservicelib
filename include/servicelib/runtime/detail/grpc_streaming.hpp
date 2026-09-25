#pragma once

#include <servicelib/datasource/grpc/common.hpp>
#include <servicelib/runtime/detail/asio_dispatch.hpp>
#include <servicelib/runtime/detail/sync.hpp>
#include <servicelib/runtime/detail/grpc_transport.hpp>

#include <agrpc/client_rpc.hpp>
#include <agrpc/server_rpc.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <utility>
#include <vector>

namespace servicelib::grpc_transport {

template <typename Response>
struct StreamResult final {
  std::vector<Response> responses;
  grpc::Status status;
  [[nodiscard]] bool ok() const noexcept { return status.ok(); }
};

template <auto PrepareAsync, typename Stub>
boost::asio::awaitable<
    UnaryResult<typename agrpc::ClientRPC<PrepareAsync>::Response>>
ClientStreamCall(
    agrpc::GrpcContext& grpcContext, Stub& stub,
    const MessageContext& message,
    const std::vector<typename agrpc::ClientRPC<PrepareAsync>::Request>&
        requests) {
  using RPC = agrpc::ClientRPC<PrepareAsync>;
  RPC rpc{grpcContext};
  InjectContext(message, rpc.context());
  detail::ClientCancellation cancellation(message, rpc.context());
  typename RPC::Response response;
  if (co_await rpc.start(stub, response, boost::asio::use_awaitable)) {
    for (const auto& request : requests) {
      if (!co_await rpc.write(request, boost::asio::use_awaitable)) break;
    }
  }
  auto status = co_await rpc.finish(boost::asio::use_awaitable);
  co_return UnaryResult<typename RPC::Response>{std::move(response),
                                                std::move(status)};
}

template <auto PrepareAsync, typename Stub>
boost::asio::awaitable<
    StreamResult<typename agrpc::ClientRPC<PrepareAsync>::Response>>
ServerStreamCall(
    agrpc::GrpcContext& grpcContext, Stub& stub,
    const MessageContext& message,
    const typename agrpc::ClientRPC<PrepareAsync>::Request& request) {
  using RPC = agrpc::ClientRPC<PrepareAsync>;
  RPC rpc{grpcContext};
  InjectContext(message, rpc.context());
  detail::ClientCancellation cancellation(message, rpc.context());
  std::vector<typename RPC::Response> responses;
  if (co_await rpc.start(stub, request, boost::asio::use_awaitable)) {
    typename RPC::Response response;
    while (co_await rpc.read(response, boost::asio::use_awaitable)) {
      responses.push_back(std::move(response));
      response = typename RPC::Response{};
    }
  }
  auto status = co_await rpc.finish(boost::asio::use_awaitable);
  co_return StreamResult<typename RPC::Response>{std::move(responses),
                                                 std::move(status)};
}

template <auto PrepareAsync, typename Stub>
boost::asio::awaitable<
    StreamResult<typename agrpc::ClientRPC<PrepareAsync>::Response>>
BidirectionalStreamCall(
    agrpc::GrpcContext& grpcContext, Stub& stub,
    const MessageContext& message,
    const std::vector<typename agrpc::ClientRPC<PrepareAsync>::Request>&
        requests) {
  using RPC = agrpc::ClientRPC<PrepareAsync>;
  RPC rpc{grpcContext};
  InjectContext(message, rpc.context());
  detail::ClientCancellation cancellation(message, rpc.context());
  std::vector<typename RPC::Response> responses;
  if (co_await rpc.start(stub, boost::asio::use_awaitable)) {
    for (const auto& request : requests) {
      if (!co_await rpc.write(request, boost::asio::use_awaitable)) break;
    }
    (void)co_await rpc.writes_done(boost::asio::use_awaitable);
    typename RPC::Response response;
    while (co_await rpc.read(response, boost::asio::use_awaitable)) {
      responses.push_back(std::move(response));
      response = typename RPC::Response{};
    }
  }
  auto status = co_await rpc.finish(boost::asio::use_awaitable);
  co_return StreamResult<typename RPC::Response>{std::move(responses),
                                                 std::move(status)};
}

template <typename Endpoint, typename RPC>
boost::asio::awaitable<typename RPC::Response> HandleClientStreamingSource(
    Endpoint& endpoint, RPC& rpc, MessageContext transportContext) {
  co_return co_await servicelib::detail::CooperativeExecution::Run(
      [&endpoint, &rpc, transportContext = std::move(transportContext)]() mutable -> typename RPC::Response {
  using Response = typename RPC::Response;
  using Request = typename Endpoint::Request;
  auto startedSpan = endpoint.startTrace(transportContext);
  std::mutex responseMutex;
  std::optional<Response> response;
  auto requestSlot = std::make_shared<std::weak_ptr<Request>>();
  auto sender =
      std::make_shared<datasource::grpc::Sender<Response>>(
      [&, requestSlot](Response result) {
        {
          std::lock_guard lock(responseMutex);
          if (!response) response.emplace(std::move(result));
        }
        if (auto request = requestSlot->lock()) {
          typename Endpoint::ResultCtx resultContext{request};
          resultContext.done();
        }
      },
      startedSpan.sharedSpan());
  auto request = endpoint.begin(transportContext, std::move(sender),
                                startedSpan.sharedSpan());
  *requestSlot = request;
  const auto startedAt = endpoint.metrics().requestStart();
  std::exception_ptr error;
  bool resultWaitFailed = false;
  try {
    endpoint.activate(request);
    typename RPC::Request value;
    std::int64_t messageCount = 0;
    while (servicelib::detail::CooperativeExecution::Await([&] { return rpc.read(value, boost::asio::use_awaitable); })) {
      endpoint.consume(request, value);
      value = typename RPC::Request{};
      ++messageCount;
    }
    endpoint.eof(request, messageCount);
    if (endpoint.hasResult()) {
      resultWaitFailed = true;
      endpoint.waitDone(request);
      resultWaitFailed = false;
    } else {
      request->sender->send(Response{});
    }
  } catch (...) {
    error = std::current_exception();
    if (!resultWaitFailed) endpoint.recordFailure(request, error);
  }
  try {
    endpoint.finish(request, error, [&] {
      return resultWaitFailed && request->done.IsReady();
    });
  } catch (...) {
    if (!error) error = std::current_exception();
  }
  if (error && resultWaitFailed) endpoint.recordFailure(request, error);
  endpoint.metrics().requestEnd(startedAt, error);
  if (error) std::rethrow_exception(error);
  std::lock_guard lock(responseMutex);
  return response ? std::move(*response) : Response{};
      });
}

template <typename Endpoint, typename RPC>
boost::asio::awaitable<typename RPC::Response> HandleClientStreamingSource(
    Endpoint& endpoint, RPC& rpc) {
  co_return co_await HandleClientStreamingSource(
      endpoint, rpc, ExtractContext(rpc.context()));
}

template <typename Endpoint, typename RPC>
boost::asio::awaitable<void> HandleServerStreamingSource(
    Endpoint& endpoint, RPC& rpc, const typename RPC::Request& value,
    MessageContext transportContext) {
  co_return co_await servicelib::detail::CooperativeExecution::Run(
      [&endpoint, &rpc, &value, transportContext = std::move(transportContext)]() mutable -> void {
  using Response = typename RPC::Response;
  auto startedSpan = endpoint.startTrace(transportContext);
  auto sender =
      std::make_shared<datasource::grpc::Sender<Response>>(
      [&rpc](Response response) {
        if (!servicelib::detail::CooperativeExecution::Await(
                [&] { return rpc.write(response, boost::asio::use_awaitable); })) {
          throw std::runtime_error("gRPC stream write cancelled");
        }
      },
      startedSpan.sharedSpan());
  auto request = endpoint.begin(transportContext, std::move(sender),
                                startedSpan.sharedSpan());
  const auto startedAt = endpoint.metrics().requestStart();
  std::exception_ptr error;
  bool resultWaitFailed = false;
  try {
    endpoint.activate(request);
    endpoint.consume(request, value);
    endpoint.eof(request);
    if (endpoint.hasResult()) {
      resultWaitFailed = true;
      endpoint.waitDone(request);
      resultWaitFailed = false;
    }
  } catch (...) {
    error = std::current_exception();
    if (!resultWaitFailed) endpoint.recordFailure(request, error);
  }
  try {
    endpoint.finish(request, error, [&] {
      return resultWaitFailed && request->done.IsReady();
    });
  } catch (...) {
    if (!error) error = std::current_exception();
  }
  if (error && resultWaitFailed) endpoint.recordFailure(request, error);
  endpoint.metrics().requestEnd(startedAt, error);
  if (error) std::rethrow_exception(error);
      });
}

template <typename Endpoint, typename RPC>
boost::asio::awaitable<void> HandleServerStreamingSource(
    Endpoint& endpoint, RPC& rpc, const typename RPC::Request& value) {
  co_await HandleServerStreamingSource(
      endpoint, rpc, value, ExtractContext(rpc.context()));
}

template <typename Endpoint, typename RPC>
boost::asio::awaitable<void> HandleBidirectionalStreamingSource(
    Endpoint& endpoint, RPC& rpc, MessageContext transportContext) {
  co_return co_await servicelib::detail::CooperativeExecution::Run(
      [&endpoint, &rpc, transportContext = std::move(transportContext)]() mutable -> void {
  using Response = typename RPC::Response;
  auto startedSpan = endpoint.startTrace(transportContext);
  auto sender =
      std::make_shared<datasource::grpc::Sender<Response>>(
      [&rpc](Response response) {
        if (!servicelib::detail::CooperativeExecution::Await(
                [&] { return rpc.write(response, boost::asio::use_awaitable); })) {
          throw std::runtime_error("gRPC stream write cancelled");
        }
      },
      startedSpan.sharedSpan());
  auto request = endpoint.begin(transportContext, std::move(sender),
                                startedSpan.sharedSpan());
  const auto startedAt = endpoint.metrics().requestStart();
  std::exception_ptr error;
  bool resultWaitFailed = false;
  try {
    endpoint.activate(request);
    typename RPC::Request value;
    std::int64_t messageCount = 0;
    while (servicelib::detail::CooperativeExecution::Await([&] { return rpc.read(value, boost::asio::use_awaitable); })) {
      endpoint.consume(request, value);
      value = typename RPC::Request{};
      ++messageCount;
    }
    endpoint.eof(request, messageCount);
    if (endpoint.hasResult()) {
      resultWaitFailed = true;
      endpoint.waitDone(request);
      resultWaitFailed = false;
    }
  } catch (...) {
    error = std::current_exception();
    if (!resultWaitFailed) endpoint.recordFailure(request, error);
  }
  try {
    endpoint.finish(request, error, [&] {
      return resultWaitFailed && request->done.IsReady();
    });
  } catch (...) {
    if (!error) error = std::current_exception();
  }
  if (error && resultWaitFailed) endpoint.recordFailure(request, error);
  endpoint.metrics().requestEnd(startedAt, error);
  if (error) std::rethrow_exception(error);
      });
}

template <typename Endpoint, typename RPC>
boost::asio::awaitable<void> HandleBidirectionalStreamingSource(
    Endpoint& endpoint, RPC& rpc) {
  co_await HandleBidirectionalStreamingSource(
      endpoint, rpc, ExtractContext(rpc.context()));
}

template <auto RequestMethod, typename AsyncService, typename Handler>
void RegisterClientStreamingSource(agrpc::GrpcContext& grpcContext,
                                   AsyncService& service, Handler handler,
                                   boost::asio::any_io_executor handlerExecutor = {}) {
  using RPC = detail::ObservableServerRPC<RequestMethod>;
  if (!handlerExecutor) handlerExecutor = grpcContext.get_executor();
  detail::RegisterRpcHandler<RPC>(
      grpcContext, service,
      [handler = std::move(handler)](RPC& rpc) mutable
          -> boost::asio::awaitable<void> {
        detail::ServerCancellation cancellation;
        detail::ObserveServerCancellation(
            rpc, cancellation, co_await boost::asio::this_coro::executor);
        auto context = ExtractContext(rpc.context()).withExternalCancellation(
            cancellation.token());
        std::optional<typename RPC::Response> response;
        grpc::Status status = grpc::Status::OK;
        try {
          if constexpr (std::invocable<Handler&, RPC&, MessageContext>) {
            response.emplace(
                co_await std::invoke(handler, rpc, std::move(context)));
          } else if constexpr (std::invocable<Handler&, RPC&>) {
            response.emplace(co_await std::invoke(handler, rpc));
          } else {
            std::vector<typename RPC::Request> requests;
            typename RPC::Request request;
            while (co_await rpc.read(request, boost::asio::use_awaitable)) {
              requests.push_back(std::move(request));
              request = typename RPC::Request{};
            }
            response.emplace(co_await std::invoke(
                handler, std::move(context), std::move(requests)));
          }
        } catch (const std::exception& error) {
          status = grpc::Status(grpc::StatusCode::INTERNAL, error.what());
        } catch (...) {
          status = grpc::Status(grpc::StatusCode::INTERNAL,
                                "unhandled client-streaming handler error");
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

template <auto RequestMethod, typename AsyncService, typename Handler>
void RegisterServerStreamingSource(agrpc::GrpcContext& grpcContext,
                                   AsyncService& service, Handler handler,
                                   boost::asio::any_io_executor handlerExecutor = {}) {
  using RPC = detail::ObservableServerRPC<RequestMethod>;
  if (!handlerExecutor) handlerExecutor = grpcContext.get_executor();
  detail::RegisterRpcHandler<RPC>(
      grpcContext, service,
      [handler = std::move(handler)](
          RPC& rpc, typename RPC::Request& request) mutable
          -> boost::asio::awaitable<void> {
        detail::ServerCancellation cancellation;
        detail::ObserveServerCancellation(
            rpc, cancellation, co_await boost::asio::this_coro::executor);
        auto context = ExtractContext(rpc.context()).withExternalCancellation(
            cancellation.token());
        grpc::Status status = grpc::Status::OK;
        try {
          if constexpr (std::invocable<Handler&, RPC&,
                                       typename RPC::Request&,
                                       MessageContext>) {
            co_await std::invoke(handler, rpc, request, std::move(context));
          } else if constexpr (std::invocable<Handler&, RPC&,
                                       typename RPC::Request&>) {
            co_await std::invoke(handler, rpc, request);
          } else {
            auto responses = co_await std::invoke(
                handler, std::move(context), request);
            for (const auto& response : responses) {
              if (!co_await rpc.write(response, boost::asio::use_awaitable))
                break;
            }
          }
        } catch (const std::exception& error) {
          status = grpc::Status(grpc::StatusCode::INTERNAL, error.what());
        } catch (...) {
          status = grpc::Status(grpc::StatusCode::INTERNAL,
                                "unhandled server-streaming handler error");
        }
        if (status.ok()) {
          (void)co_await rpc.finish(status, boost::asio::use_awaitable);
        } else {
          (void)co_await rpc.finish(status, boost::asio::use_awaitable);
        }
        co_await cancellation.Wait();
      },
      boost::asio::bind_executor(std::move(handlerExecutor),
                                 boost::asio::detached));
}

template <auto RequestMethod, typename AsyncService, typename Handler>
void RegisterBidirectionalStreamingSource(agrpc::GrpcContext& grpcContext,
                                          AsyncService& service,
                                          Handler handler,
                                          boost::asio::any_io_executor handlerExecutor = {}) {
  using RPC = detail::ObservableServerRPC<RequestMethod>;
  if (!handlerExecutor) handlerExecutor = grpcContext.get_executor();
  detail::RegisterRpcHandler<RPC>(
      grpcContext, service,
      [handler = std::move(handler)](RPC& rpc) mutable
          -> boost::asio::awaitable<void> {
        detail::ServerCancellation cancellation;
        detail::ObserveServerCancellation(
            rpc, cancellation, co_await boost::asio::this_coro::executor);
        auto context = ExtractContext(rpc.context()).withExternalCancellation(
            cancellation.token());
        grpc::Status status = grpc::Status::OK;
        try {
          if constexpr (std::invocable<Handler&, RPC&, MessageContext>) {
            co_await std::invoke(handler, rpc, std::move(context));
          } else if constexpr (std::invocable<Handler&, RPC&>) {
            co_await std::invoke(handler, rpc);
          } else {
            std::vector<typename RPC::Request> requests;
            typename RPC::Request request;
            while (co_await rpc.read(request, boost::asio::use_awaitable)) {
              requests.push_back(std::move(request));
              request = typename RPC::Request{};
            }
            auto responses = co_await std::invoke(
                handler, std::move(context), std::move(requests));
            for (const auto& response : responses) {
              if (!co_await rpc.write(response, boost::asio::use_awaitable))
                break;
            }
          }
        } catch (const std::exception& error) {
          status = grpc::Status(grpc::StatusCode::INTERNAL, error.what());
        } catch (...) {
          status = grpc::Status(grpc::StatusCode::INTERNAL,
                                "unhandled bidi handler error");
        }
        (void)co_await rpc.finish(status, boost::asio::use_awaitable);
        co_await cancellation.Wait();
      },
      boost::asio::bind_executor(std::move(handlerExecutor),
                                 boost::asio::detached));
}

}  // namespace servicelib::grpc_transport

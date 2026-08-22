#include "connector_test.grpc.pb.h"

#include <servicelib/runtime/detail/grpc_client.hpp>
#include <servicelib/runtime/detail/grpc_transport.hpp>
#include <servicelib/runtime/detail/grpc_runtime.hpp>
#include <servicelib/runtime/testmetrics/testmetrics.hpp>

#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>

#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <atomic>
#include <chrono>
#include <future>
#include <iostream>
#include <memory>
#include <stop_token>
#include <stdexcept>
#include <string>
#include <thread>

namespace asio = boost::asio;
using namespace std::chrono_literals;

namespace {

void Require(bool condition, const std::string& message) {
  if (!condition) throw std::runtime_error(message);
}

}  // namespace

int main() {
  servicelib::test::ConnectorTest::AsyncService service;
  grpc::ServerBuilder builder;
  int port{};
  builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(),
                           &port);
  builder.RegisterService(&service);
  auto queue = builder.AddCompletionQueue();
  auto server = builder.BuildAndStart();
  Require(static_cast<bool>(server), "gRPC server did not start");
  Require(port > 0, "gRPC server did not allocate a port");

  servicelib::testmetrics::TestMetrics metrics;
  servicelib::async::GrpcRuntime runtime(
      {.workers = 2, .unhandledException = {}, .metrics = &metrics},
      std::move(queue));
  std::promise<servicelib::MessageContext> receivedContext;
  auto receivedFuture = receivedContext.get_future();
  servicelib::detail::SingleUseEvent releaseFirstRequest;
  std::promise<servicelib::MessageContext> receivedCancelledContext;
  auto receivedCancelledFuture = receivedCancelledContext.get_future();
  servicelib::detail::SingleUseEvent releaseCancelledRequest;
  std::promise<servicelib::MessageContext> receivedPooledCancelledContext;
  auto receivedPooledCancelledFuture =
      receivedPooledCancelledContext.get_future();
  servicelib::detail::SingleUseEvent releasePooledCancelledRequest;
  std::atomic<bool> firstRequest{true};
  servicelib::grpc_transport::RegisterUnarySource<
      &servicelib::test::ConnectorTest::AsyncService::RequestUnary>(
      runtime.grpcContext(), service,
      [&receivedContext, &releaseFirstRequest, &receivedCancelledContext,
       &releaseCancelledRequest, &receivedPooledCancelledContext,
       &releasePooledCancelledRequest, &firstRequest](
          servicelib::MessageContext context,
          const servicelib::test::EchoRequest& request)
          -> asio::awaitable<servicelib::test::EchoResponse> {
        if (firstRequest.exchange(false)) {
          receivedContext.set_value(context);
          co_await releaseFirstRequest.AsyncWait();
        } else if (request.value() == "cancel-midflight") {
          receivedCancelledContext.set_value(context);
          co_await releaseCancelledRequest.AsyncWait(context);
        } else if (request.value() == "cancel-pooled") {
          receivedPooledCancelledContext.set_value(context);
          co_await releasePooledCancelledRequest.AsyncWait(context);
        } else if (request.value() == "fail") {
          throw std::runtime_error("unary handler failed");
        } else if (context.streamId() == "expired") {
          asio::steady_timer timer(co_await asio::this_coro::executor, 250ms);
          boost::system::error_code error;
          co_await timer.async_wait(
              asio::redirect_error(asio::use_awaitable, error));
        }
        servicelib::test::EchoResponse response;
        response.set_value("echo:" + request.value());
        co_return response;
      }, runtime.grpcExecutor());
  runtime.Start();

  auto channel = grpc::CreateChannel("127.0.0.1:" + std::to_string(port),
                                     grpc::InsecureChannelCredentials());
  auto stub = servicelib::test::ConnectorTest::NewStub(channel);
  std::promise<servicelib::grpc_transport::UnaryResult<
      servicelib::test::EchoResponse>> resultPromise;
  auto resultFuture = resultPromise.get_future();
  std::atomic<bool> cancellationVerified{false};
  std::atomic<int> cancelledCode{-1};
  std::atomic<int> expiredCode{-1};
  std::atomic<int> midflightCancelledCode{-1};
  std::atomic<int> failedCode{-1};
  std::stop_source midflightCancellation;

  auto context = servicelib::MessageContext{}
                     .withStreamId("stream-42")
                     .withPriority(7)
                     .withSampling(true)
                     .withTrace(
                         {"4bf92f3577b34da6a3ce929d0e0e4736",
                          "00f067aa0ba902b7", true, "vendor=value",
                          "tenant=acme"})
                     .withDeadline(std::chrono::steady_clock::now() + 2s);
  servicelib::test::EchoRequest request;
  request.set_value("hello");
  auto runCalls = [&]() -> asio::awaitable<void> {
    auto result = co_await servicelib::grpc_transport::UnaryCall<
        &servicelib::test::ConnectorTest::Stub::PrepareAsyncUnary>(
        runtime.grpcContext(), *stub, context, request);

    servicelib::test::EchoRequest midflightRequest;
    midflightRequest.set_value("cancel-midflight");
    auto midflightCancelled =
        co_await servicelib::grpc_transport::UnaryCall<
            &servicelib::test::ConnectorTest::Stub::PrepareAsyncUnary>(
            runtime.grpcContext(), *stub,
            servicelib::MessageContext{}
                .withStreamId("cancel-midflight")
                .withStopToken(midflightCancellation.get_token())
                .withDeadline(std::chrono::steady_clock::now() + 3s),
            midflightRequest);

    std::stop_source cancellation;
    auto cancelledContext =
        servicelib::MessageContext{}
            .withStreamId("cancelled")
            .withStopToken(cancellation.get_token())
            .withDeadline(std::chrono::steady_clock::now() + 2s);
    cancellation.request_stop();
    auto cancelled = co_await servicelib::grpc_transport::UnaryCall<
        &servicelib::test::ConnectorTest::Stub::PrepareAsyncUnary>(
        runtime.grpcContext(), *stub, cancelledContext, request);

    auto expired = co_await servicelib::grpc_transport::UnaryCall<
        &servicelib::test::ConnectorTest::Stub::PrepareAsyncUnary>(
        runtime.grpcContext(), *stub,
        servicelib::MessageContext{}
            .withStreamId("expired")
            .withDeadline(std::chrono::steady_clock::now() + 25ms),
        request);
    servicelib::test::EchoRequest failedRequest;
    failedRequest.set_value("fail");
    auto failed = co_await servicelib::grpc_transport::UnaryCall<
        &servicelib::test::ConnectorTest::Stub::PrepareAsyncUnary>(
        runtime.grpcContext(), *stub,
        servicelib::MessageContext{}.withStreamId("failed"), failedRequest);
    cancelledCode.store(static_cast<int>(cancelled.status.error_code()),
                        std::memory_order_release);
    expiredCode.store(static_cast<int>(expired.status.error_code()),
                      std::memory_order_release);
    midflightCancelledCode.store(
        static_cast<int>(midflightCancelled.status.error_code()),
        std::memory_order_release);
    failedCode.store(static_cast<int>(failed.status.error_code()),
                     std::memory_order_release);
    cancellationVerified.store(
        !midflightCancelled.ok() &&
            midflightCancelled.status.error_code() ==
                grpc::StatusCode::CANCELLED &&
            !cancelled.ok() &&
            cancelled.status.error_code() == grpc::StatusCode::CANCELLED &&
            !expired.ok() &&
            expired.status.error_code() ==
                grpc::StatusCode::DEADLINE_EXCEEDED &&
            !failed.ok() &&
            failed.status.error_code() == grpc::StatusCode::INTERNAL &&
            failed.status.error_message() == "unary handler failed",
        std::memory_order_release);
    resultPromise.set_value(std::move(result));
  };
  runtime.SpawnGrpc(runCalls());

  Require(receivedFuture.wait_for(3s) == std::future_status::ready,
          "unary server did not accept the first request");
  const auto suspensionDeadline = std::chrono::steady_clock::now() + 2s;
  while (metrics.observableGauge("runtime.worker_utilization", {}).value() !=
             0 &&
         std::chrono::steady_clock::now() < suspensionDeadline) {
    std::this_thread::sleep_for(1ms);
  }
  Require(metrics.observableGauge("runtime.worker_utilization", {}).value() ==
              0,
          "a suspended gRPC coroutine inflated worker utilization");
  releaseFirstRequest.Send();

  Require(receivedCancelledFuture.wait_for(3s) == std::future_status::ready,
          "unary server did not accept the cancellable request");
  auto receivedCancelled = receivedCancelledFuture.get();
  midflightCancellation.request_stop();
  const auto cancellationDeadline = std::chrono::steady_clock::now() + 2s;
  while (!receivedCancelled.cancelled() &&
         std::chrono::steady_clock::now() < cancellationDeadline) {
    std::this_thread::sleep_for(1ms);
  }
  Require(receivedCancelled.cancelled(),
          "accepted unary cancellation did not reach MessageContext");
  releaseCancelledRequest.Send();

  Require(resultFuture.wait_for(3s) == std::future_status::ready,
          "unary client calls did not complete");
  auto result = resultFuture.get();
  Require(result.ok(), "successful unary call returned an error");
  Require(result.response.value() == "echo:hello",
          "successful unary response payload differs");
  if (!cancellationVerified.load(std::memory_order_acquire)) {
    std::cerr << "cancelled code="
              << cancelledCode.load(std::memory_order_acquire)
              << ", midflight code="
              << midflightCancelledCode.load(std::memory_order_acquire)
              << ", expired code="
              << expiredCode.load(std::memory_order_acquire)
              << ", failed code="
              << failedCode.load(std::memory_order_acquire) << '\n';
  }
  Require(cancellationVerified.load(std::memory_order_acquire),
          "unary cancellation/deadline status differs");

  auto received = receivedFuture.get();
  Require(received.streamId() == "stream-42",
          "unary stream ID was not propagated");
  Require(!received.hasPriority(),
          "unary priority escaped process-local context");
  Require(received.deadline().has_value(),
          "unary deadline was not propagated");
  Require(received.samplingEnabled(),
          "unary sampling marker was not propagated");
  Require(received.trace().traceId == "4bf92f3577b34da6a3ce929d0e0e4736" &&
              received.trace().spanId == "00f067aa0ba902b7" &&
              received.trace().traceState == "vendor=value" &&
              received.trace().baggage == "tenant=acme",
          "unary W3C trace context or baggage was not propagated");

  {
    servicelib::grpc_transport::ClientPool<
        servicelib::test::ConnectorTest::Stub>
        pool{runtime.grpcContext(), "127.0.0.1:" + std::to_string(port), 2,
             [](std::shared_ptr<grpc::Channel> value) {
               return servicelib::test::ConnectorTest::NewStub(
                   std::move(value));
             }};

    servicelib::test::EchoRequest pooledRequest;
    pooledRequest.set_value("pooled");
    std::promise<std::pair<std::exception_ptr,
                           std::optional<servicelib::test::EchoResponse>>>
        pooledCompletion;
    auto pooledFuture = pooledCompletion.get_future();
    pool.asyncUnary<
        &servicelib::test::ConnectorTest::Stub::PrepareAsyncUnary,
        servicelib::test::EchoRequest, servicelib::test::EchoResponse>(
        std::move(pooledRequest),
        servicelib::datasink::grpc::callOptions(
            servicelib::MessageContext{}.withStreamId("pooled")),
        [&pooledCompletion](
            std::exception_ptr error,
            std::optional<servicelib::test::EchoResponse> response) {
          pooledCompletion.set_value(
              {std::move(error), std::move(response)});
        });
    Require(pooledFuture.wait_for(3s) == std::future_status::ready,
            "pooled unary call did not complete");
    auto [pooledError, pooledResponse] = pooledFuture.get();
    Require(!pooledError, "pooled unary call returned an error");
    Require(pooledResponse && pooledResponse->value() == "echo:pooled",
            "pooled unary response payload differs");

    servicelib::test::EchoRequest failedPooledRequest;
    failedPooledRequest.set_value("fail");
    std::promise<std::exception_ptr> failedPooledCompletion;
    auto failedPooledFuture = failedPooledCompletion.get_future();
    pool.asyncUnary<
        &servicelib::test::ConnectorTest::Stub::PrepareAsyncUnary,
        servicelib::test::EchoRequest, servicelib::test::EchoResponse>(
        std::move(failedPooledRequest),
        servicelib::datasink::grpc::callOptions(servicelib::MessageContext{}),
        [&failedPooledCompletion](
            std::exception_ptr error,
            std::optional<servicelib::test::EchoResponse>) {
          failedPooledCompletion.set_value(std::move(error));
        });
    Require(failedPooledFuture.wait_for(3s) == std::future_status::ready,
            "failed pooled unary call did not complete");
    auto failedPooledError = failedPooledFuture.get();
    Require(static_cast<bool>(failedPooledError),
            "failed pooled unary call returned no error");
    try {
      std::rethrow_exception(failedPooledError);
    } catch (const servicelib::grpc_transport::StatusError& error) {
      Require(error.code() == grpc::StatusCode::INTERNAL,
              "failed pooled unary status differs");
    }

    servicelib::test::EchoRequest cancelledPooledRequest;
    cancelledPooledRequest.set_value("cancel-pooled");
    std::stop_source pooledCancellation;
    std::promise<std::exception_ptr> cancelledPooledCompletion;
    auto cancelledPooledFuture = cancelledPooledCompletion.get_future();
    pool.asyncUnary<
        &servicelib::test::ConnectorTest::Stub::PrepareAsyncUnary,
        servicelib::test::EchoRequest, servicelib::test::EchoResponse>(
        std::move(cancelledPooledRequest),
        servicelib::datasink::grpc::callOptions(
            servicelib::MessageContext{}
                .withStopToken(pooledCancellation.get_token())
                .withDeadline(std::chrono::steady_clock::now() + 3s)),
        [&cancelledPooledCompletion](
            std::exception_ptr error,
            std::optional<servicelib::test::EchoResponse>) {
          cancelledPooledCompletion.set_value(std::move(error));
        });
    Require(receivedPooledCancelledFuture.wait_for(3s) ==
                std::future_status::ready,
            "cancellable pooled unary call was not accepted");
    pooledCancellation.request_stop();
    releasePooledCancelledRequest.Send();
    Require(cancelledPooledFuture.wait_for(3s) == std::future_status::ready,
            "cancelled pooled unary call did not complete");
    auto cancelledPooledError = cancelledPooledFuture.get();
    Require(static_cast<bool>(cancelledPooledError),
            "cancelled pooled unary call returned no error");
    try {
      std::rethrow_exception(cancelledPooledError);
    } catch (const servicelib::grpc_transport::StatusError& error) {
      Require(error.code() == grpc::StatusCode::CANCELLED,
              "cancelled pooled unary status differs");
    }
  }

  server->Shutdown();
  runtime.Stop();
  runtime.Join();
}

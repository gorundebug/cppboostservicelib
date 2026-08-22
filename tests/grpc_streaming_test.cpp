#include "connector_test.grpc.pb.h"

#include <servicelib/runtime/detail/grpc_streaming.hpp>
#include <servicelib/runtime/detail/grpc_client.hpp>
#include <servicelib/runtime/detail/grpc_runtime.hpp>

#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>

#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <chrono>
#include <future>
#include <stop_token>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace asio = boost::asio;
using namespace std::chrono_literals;

namespace {

void Require(bool condition, const std::string& message) {
  if (!condition) throw std::runtime_error(message);
}

struct AcceptedCancellation final {
  std::promise<servicelib::MessageContext> accepted;
  servicelib::detail::SingleUseEvent release;
};

struct MethodTranscript final {
  std::vector<std::string> responses;
  grpc::StatusCode status{grpc::StatusCode::UNKNOWN};
  std::string message;
  std::string details;
  bool eof{};

  bool operator==(const MethodTranscript&) const = default;
};

struct StreamingTranscript final {
  MethodTranscript client;
  MethodTranscript server;
  MethodTranscript bidirectional;

  bool operator==(const StreamingTranscript&) const = default;
};

MethodTranscript Transcript(grpc::Status status,
                            std::vector<std::string> responses,
                            bool eof = true) {
  return {
      .responses = std::move(responses),
      .status = status.error_code(),
      .message = status.error_message(),
      .details = status.error_details(),
      .eof = eof,
  };
}

asio::awaitable<StreamingTranscript> FrameworkStreamingTranscript(
    servicelib::async::GrpcRuntime& runtime,
    servicelib::test::ConnectorTest::Stub& stub,
    const servicelib::MessageContext& context,
    const std::vector<servicelib::test::EchoRequest>& requests) {
  StreamingTranscript transcript;
  auto client = co_await servicelib::grpc_transport::ClientStreamCall<
      &servicelib::test::ConnectorTest::Stub::PrepareAsyncClientStreaming>(
      runtime.grpcContext(), stub, context, requests);
  transcript.client = Transcript(
      std::move(client.status), {client.response.SerializeAsString()});

  auto server = co_await servicelib::grpc_transport::ServerStreamCall<
      &servicelib::test::ConnectorTest::Stub::PrepareAsyncServerStreaming>(
      runtime.grpcContext(), stub, context, requests.front());
  std::vector<std::string> serverResponses;
  for (const auto& response : server.responses) {
    serverResponses.push_back(response.SerializeAsString());
  }
  transcript.server =
      Transcript(std::move(server.status), std::move(serverResponses));

  auto bidi =
      co_await servicelib::grpc_transport::BidirectionalStreamCall<
          &servicelib::test::ConnectorTest::Stub::
              PrepareAsyncBidirectionalStreaming>(
          runtime.grpcContext(), stub, context, requests);
  std::vector<std::string> bidiResponses;
  for (const auto& response : bidi.responses) {
    bidiResponses.push_back(response.SerializeAsString());
  }
  transcript.bidirectional =
      Transcript(std::move(bidi.status), std::move(bidiResponses));
  co_return transcript;
}

StreamingTranscript NativeStreamingTranscript(
    servicelib::test::ConnectorTest::Stub& stub,
    const std::vector<servicelib::test::EchoRequest>& requests) {
  StreamingTranscript transcript;
  {
    grpc::ClientContext context;
    servicelib::test::EchoResponse response;
    auto writer = stub.ClientStreaming(&context, &response);
    for (const auto& request : requests) {
      Require(writer->Write(request), "native client-streaming write failed");
    }
    Require(writer->WritesDone(),
            "native client-streaming writes_done failed");
    transcript.client =
        Transcript(writer->Finish(), {response.SerializeAsString()});
  }
  {
    grpc::ClientContext context;
    auto reader = stub.ServerStreaming(&context, requests.front());
    servicelib::test::EchoResponse response;
    std::vector<std::string> responses;
    while (reader->Read(&response)) {
      responses.push_back(response.SerializeAsString());
    }
    transcript.server = Transcript(reader->Finish(), std::move(responses));
  }
  {
    grpc::ClientContext context;
    auto stream = stub.BidirectionalStreaming(&context);
    for (const auto& request : requests) {
      Require(stream->Write(request), "native bidirectional write failed");
    }
    Require(stream->WritesDone(), "native bidirectional writes_done failed");
    servicelib::test::EchoResponse response;
    std::vector<std::string> responses;
    while (stream->Read(&response)) {
      responses.push_back(response.SerializeAsString());
    }
    transcript.bidirectional =
        Transcript(stream->Finish(), std::move(responses));
  }
  return transcript;
}

void RequirePropagatedContext(const servicelib::MessageContext& context,
                              const std::string& name) {
  Require(context.streamId() == "streaming-1",
          name + " stream ID was not propagated");
  Require(!context.hasPriority(),
          name + " priority escaped process-local context");
  Require(context.deadline().has_value(),
          name + " deadline was not propagated");
  Require(context.samplingEnabled(),
          name + " sampling marker was not propagated");
  Require(context.trace().traceId ==
                  "4bf92f3577b34da6a3ce929d0e0e4736" &&
              context.trace().spanId == "00f067aa0ba902b7" &&
              context.trace().traceState == "vendor=value" &&
              context.trace().baggage == "tenant=acme",
          name + " W3C trace context or baggage was not propagated");
}

void CancelAccepted(std::future<servicelib::MessageContext>& accepted,
                    std::stop_source& cancellation,
                    servicelib::detail::SingleUseEvent& release,
                    const std::string& name) {
  Require(accepted.wait_for(3s) == std::future_status::ready,
          name + " handler did not accept the request");
  auto context = accepted.get();
  cancellation.request_stop();
  const auto deadline = std::chrono::steady_clock::now() + 2s;
  while (!context.cancelled() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  Require(context.cancelled(),
          name + " cancellation did not reach MessageContext");
  release.Send();
}

asio::awaitable<void> RunCancelledServerStream(
    servicelib::async::GrpcRuntime& runtime,
    servicelib::test::ConnectorTest::Stub& stub,
    servicelib::MessageContext context,
    servicelib::test::EchoRequest value,
    std::promise<servicelib::grpc_transport::StreamResult<
        servicelib::test::EchoResponse>>& completed) {
  completed.set_value(
      co_await servicelib::grpc_transport::ServerStreamCall<
          &servicelib::test::ConnectorTest::Stub::PrepareAsyncServerStreaming>(
          runtime.grpcContext(), stub, std::move(context), value));
}

asio::awaitable<void> RunCancelledClientStream(
    servicelib::async::GrpcRuntime& runtime,
    servicelib::test::ConnectorTest::Stub& stub,
    servicelib::MessageContext context,
    std::vector<servicelib::test::EchoRequest> values,
    std::promise<servicelib::grpc_transport::UnaryResult<
        servicelib::test::EchoResponse>>& completed) {
  completed.set_value(
      co_await servicelib::grpc_transport::ClientStreamCall<
          &servicelib::test::ConnectorTest::Stub::PrepareAsyncClientStreaming>(
          runtime.grpcContext(), stub, std::move(context), values));
}

asio::awaitable<void> RunCancelledBidirectionalStream(
    servicelib::async::GrpcRuntime& runtime,
    servicelib::test::ConnectorTest::Stub& stub,
    servicelib::MessageContext context,
    std::vector<servicelib::test::EchoRequest> values,
    std::promise<servicelib::grpc_transport::StreamResult<
        servicelib::test::EchoResponse>>& completed) {
  completed.set_value(
      co_await servicelib::grpc_transport::BidirectionalStreamCall<
          &servicelib::test::ConnectorTest::Stub::
              PrepareAsyncBidirectionalStreaming>(
          runtime.grpcContext(), stub, std::move(context), values));
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
  Require(static_cast<bool>(server), "gRPC streaming server did not start");
  Require(port > 0, "gRPC streaming server did not allocate a port");

  servicelib::async::GrpcRuntime runtime(
      {.workers = 2, .unhandledException = {}}, std::move(queue));
  AcceptedCancellation clientCancellation;
  AcceptedCancellation serverCancellation;
  AcceptedCancellation bidiCancellation;
  AcceptedCancellation shutdownCancellation;
  std::promise<servicelib::MessageContext> clientContext;
  std::promise<servicelib::MessageContext> serverContext;
  std::promise<servicelib::MessageContext> bidiContext;
  std::atomic<bool> clientContextSent{false};
  std::atomic<bool> serverContextSent{false};
  std::atomic<bool> bidiContextSent{false};
  servicelib::grpc_transport::RegisterClientStreamingSource<
      &servicelib::test::ConnectorTest::AsyncService::RequestClientStreaming>(
      runtime.grpcContext(), service,
      [&clientCancellation, &clientContext, &clientContextSent](
         servicelib::MessageContext context,
         std::vector<servicelib::test::EchoRequest> requests)
          -> asio::awaitable<servicelib::test::EchoResponse> {
        if (!clientContextSent.exchange(true)) clientContext.set_value(context);
        if (!requests.empty() && requests.front().value() == "cancel-client") {
          clientCancellation.accepted.set_value(context);
          co_await clientCancellation.release.AsyncWait(context);
        } else if (!requests.empty() &&
                   requests.front().value() == "fail-client") {
          throw std::runtime_error("client-streaming handler failed");
        } else if (!requests.empty() &&
                   requests.front().value() == "deadline-client") {
          asio::steady_timer timer(co_await asio::this_coro::executor, 250ms);
          co_await timer.async_wait(asio::use_awaitable);
        }
        servicelib::test::EchoResponse response;
        std::string joined;
        for (const auto& request : requests) joined += request.value();
        response.set_value(std::move(joined));
        co_return response;
      }, runtime.grpcExecutor());
  servicelib::grpc_transport::RegisterServerStreamingSource<
      &servicelib::test::ConnectorTest::AsyncService::RequestServerStreaming>(
      runtime.grpcContext(), service,
      [&serverCancellation, &shutdownCancellation, &serverContext,
       &serverContextSent](
         servicelib::MessageContext context,
         const servicelib::test::EchoRequest& request)
          -> asio::awaitable<std::vector<servicelib::test::EchoResponse>> {
        if (!serverContextSent.exchange(true)) serverContext.set_value(context);
        if (request.value() == "cancel-server") {
          serverCancellation.accepted.set_value(context);
          co_await serverCancellation.release.AsyncWait(context);
        } else if (request.value() == "shutdown-server") {
          shutdownCancellation.accepted.set_value(context);
          co_await shutdownCancellation.release.AsyncWait(context);
        } else if (request.value() == "fail-server") {
          throw std::runtime_error("server-streaming handler failed");
        } else if (request.value() == "deadline-server") {
          asio::steady_timer timer(co_await asio::this_coro::executor, 250ms);
          co_await timer.async_wait(asio::use_awaitable);
        }
        std::vector<servicelib::test::EchoResponse> responses(3);
        for (std::size_t index = 0; index < responses.size(); ++index)
          responses[index].set_value(request.value() + std::to_string(index));
        co_return responses;
      }, runtime.grpcExecutor());
  servicelib::grpc_transport::RegisterBidirectionalStreamingSource<
      &servicelib::test::ConnectorTest::AsyncService::
          RequestBidirectionalStreaming>(
      runtime.grpcContext(), service,
      [&bidiCancellation, &bidiContext, &bidiContextSent](
         servicelib::MessageContext context,
         std::vector<servicelib::test::EchoRequest> requests)
          -> asio::awaitable<std::vector<servicelib::test::EchoResponse>> {
        if (!bidiContextSent.exchange(true)) bidiContext.set_value(context);
        if (!requests.empty() && requests.front().value() == "cancel-bidi") {
          bidiCancellation.accepted.set_value(context);
          co_await bidiCancellation.release.AsyncWait(context);
        } else if (!requests.empty() &&
                   requests.front().value() == "fail-bidi") {
          throw std::runtime_error("bidirectional handler failed");
        } else if (!requests.empty() &&
                   requests.front().value() == "deadline-bidi") {
          asio::steady_timer timer(co_await asio::this_coro::executor, 250ms);
          co_await timer.async_wait(asio::use_awaitable);
        }
        std::vector<servicelib::test::EchoResponse> responses;
        for (const auto& request : requests) {
          auto& response = responses.emplace_back();
          response.set_value("bidi:" + request.value());
        }
        co_return responses;
      }, runtime.grpcExecutor());
  runtime.Start();

  auto channel = grpc::CreateChannel("127.0.0.1:" + std::to_string(port),
                                     grpc::InsecureChannelCredentials());
  auto stub = servicelib::test::ConnectorTest::NewStub(channel);
  std::promise<bool> finished;
  auto future = finished.get_future();
  std::vector<servicelib::test::EchoRequest> requests(2);
  requests[0].set_value("a");
  requests[1].set_value("b");
  auto context = servicelib::MessageContext{}
                     .withStreamId("streaming-1")
                     .withPriority(7)
                     .withSampling(true)
                     .withTrace(
                         {"4bf92f3577b34da6a3ce929d0e0e4736",
                          "00f067aa0ba902b7", true, "vendor=value",
                          "tenant=acme"})
                     .withDeadline(std::chrono::steady_clock::now() + 3s);

  auto runLegacyCalls = [&]() -> asio::awaitable<void> {
    auto client = co_await servicelib::grpc_transport::ClientStreamCall<
        &servicelib::test::ConnectorTest::Stub::PrepareAsyncClientStreaming>(
        runtime.grpcContext(), *stub, context, requests);
    if (!client.ok() || client.response.value() != "ab") {
      finished.set_value(false);
      co_return;
    }

    auto serverStream = co_await servicelib::grpc_transport::ServerStreamCall<
        &servicelib::test::ConnectorTest::Stub::PrepareAsyncServerStreaming>(
        runtime.grpcContext(), *stub, context, requests[0]);
    if (!serverStream.ok() || serverStream.responses.size() != 3 ||
        serverStream.responses[2].value() != "a2") {
      finished.set_value(false);
      co_return;
    }

    auto bidi =
        co_await servicelib::grpc_transport::BidirectionalStreamCall<
            &servicelib::test::ConnectorTest::Stub::
                PrepareAsyncBidirectionalStreaming>(
            runtime.grpcContext(), *stub, context, requests);
    finished.set_value(bidi.ok() && bidi.responses.size() == 2 &&
                       bidi.responses[0].value() == "bidi:a" &&
                       bidi.responses[1].value() == "bidi:b");
  };
  runtime.SpawnGrpc(runLegacyCalls());

  Require(future.wait_for(5s) == std::future_status::ready,
          "legacy streaming calls did not complete");
  Require(future.get(), "legacy streaming payload/status contract differs");
  RequirePropagatedContext(clientContext.get_future().get(),
                           "client-streaming");
  RequirePropagatedContext(serverContext.get_future().get(),
                           "server-streaming");
  RequirePropagatedContext(bidiContext.get_future().get(),
                           "bidirectional-streaming");

  {
    std::promise<StreamingTranscript> completed;
    auto frameworkFuture = completed.get_future();
    auto runFramework = [&]() -> asio::awaitable<void> {
      completed.set_value(co_await FrameworkStreamingTranscript(
          runtime, *stub, servicelib::MessageContext{}, requests));
    };
    runtime.SpawnGrpc(runFramework());
    Require(frameworkFuture.wait_for(5s) == std::future_status::ready,
            "framework/native streaming transcript did not complete");
    const auto framework = frameworkFuture.get();
    auto nativeFuture = std::async(std::launch::async, [&] {
      return NativeStreamingTranscript(*stub, requests);
    });
    Require(nativeFuture.wait_for(5s) == std::future_status::ready,
            "native synchronous streaming transcript did not complete");
    Require(framework == nativeFuture.get(),
            "framework/native streaming payload, EOF or status differs");
  }

  {
    std::promise<bool> checked;
    auto checkedFuture = checked.get_future();
    auto verifyErrors = [&]() -> asio::awaitable<void> {
      std::vector<servicelib::test::EchoRequest> values(1);
      values[0].set_value("fail-client");
      auto client = co_await servicelib::grpc_transport::ClientStreamCall<
          &servicelib::test::ConnectorTest::Stub::PrepareAsyncClientStreaming>(
          runtime.grpcContext(), *stub, servicelib::MessageContext{}, values);
      servicelib::test::EchoRequest serverRequest;
      serverRequest.set_value("fail-server");
      auto serverStream =
          co_await servicelib::grpc_transport::ServerStreamCall<
              &servicelib::test::ConnectorTest::Stub::
                  PrepareAsyncServerStreaming>(
              runtime.grpcContext(), *stub, servicelib::MessageContext{},
              serverRequest);
      values[0].set_value("fail-bidi");
      auto bidi =
          co_await servicelib::grpc_transport::BidirectionalStreamCall<
              &servicelib::test::ConnectorTest::Stub::
                  PrepareAsyncBidirectionalStreaming>(
              runtime.grpcContext(), *stub, servicelib::MessageContext{},
              values);
      checked.set_value(
          client.status.error_code() == grpc::StatusCode::INTERNAL &&
          client.status.error_message() ==
              "client-streaming handler failed" &&
          serverStream.status.error_code() == grpc::StatusCode::INTERNAL &&
          serverStream.status.error_message() ==
              "server-streaming handler failed" &&
          bidi.status.error_code() == grpc::StatusCode::INTERNAL &&
          bidi.status.error_message() == "bidirectional handler failed");
    };
    runtime.SpawnGrpc(verifyErrors());
    Require(checkedFuture.wait_for(5s) == std::future_status::ready,
            "streaming error mapping did not complete");
    Require(checkedFuture.get(), "streaming INTERNAL status mapping differs");
  }

  {
    std::promise<bool> checked;
    auto checkedFuture = checked.get_future();
    auto verifyDeadlines = [&]() -> asio::awaitable<void> {
      const auto deadlineContext = [] {
        return servicelib::MessageContext{}.withDeadline(
            std::chrono::steady_clock::now() + 25ms);
      };
      std::vector<servicelib::test::EchoRequest> values(1);
      values[0].set_value("deadline-client");
      auto client = co_await servicelib::grpc_transport::ClientStreamCall<
          &servicelib::test::ConnectorTest::Stub::PrepareAsyncClientStreaming>(
          runtime.grpcContext(), *stub, deadlineContext(), values);
      servicelib::test::EchoRequest serverRequest;
      serverRequest.set_value("deadline-server");
      auto serverStream =
          co_await servicelib::grpc_transport::ServerStreamCall<
              &servicelib::test::ConnectorTest::Stub::
                  PrepareAsyncServerStreaming>(
              runtime.grpcContext(), *stub, deadlineContext(), serverRequest);
      values[0].set_value("deadline-bidi");
      auto bidi =
          co_await servicelib::grpc_transport::BidirectionalStreamCall<
              &servicelib::test::ConnectorTest::Stub::
                  PrepareAsyncBidirectionalStreaming>(
              runtime.grpcContext(), *stub, deadlineContext(), values);
      checked.set_value(
          client.status.error_code() ==
                  grpc::StatusCode::DEADLINE_EXCEEDED &&
          serverStream.status.error_code() ==
                  grpc::StatusCode::DEADLINE_EXCEEDED &&
          bidi.status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED);
    };
    runtime.SpawnGrpc(verifyDeadlines());
    Require(checkedFuture.wait_for(5s) == std::future_status::ready,
            "streaming deadline mapping did not complete");
    Require(checkedFuture.get(),
            "streaming DEADLINE_EXCEEDED status mapping differs");
  }

  {
    std::stop_source cancellation;
    auto accepted = serverCancellation.accepted.get_future();
    std::promise<servicelib::grpc_transport::StreamResult<
        servicelib::test::EchoResponse>> completed;
    auto result = completed.get_future();
    servicelib::test::EchoRequest value;
    value.set_value("cancel-server");
    runtime.SpawnGrpc(RunCancelledServerStream(
        runtime, *stub,
        servicelib::MessageContext{}
            .withStopToken(cancellation.get_token())
            .withDeadline(std::chrono::steady_clock::now() + 3s),
        std::move(value), completed));
    CancelAccepted(accepted, cancellation, serverCancellation.release,
                   "server-streaming");
    Require(result.wait_for(3s) == std::future_status::ready,
            "cancelled server-streaming call did not complete");
    Require(result.get().status.error_code() == grpc::StatusCode::CANCELLED,
            "cancelled server-streaming status differs");
  }

  {
    std::stop_source cancellation;
    auto accepted = clientCancellation.accepted.get_future();
    std::promise<servicelib::grpc_transport::UnaryResult<
        servicelib::test::EchoResponse>> completed;
    auto result = completed.get_future();
    std::vector<servicelib::test::EchoRequest> values(1);
    values[0].set_value("cancel-client");
    runtime.SpawnGrpc(RunCancelledClientStream(
        runtime, *stub,
        servicelib::MessageContext{}
            .withStopToken(cancellation.get_token())
            .withDeadline(std::chrono::steady_clock::now() + 3s),
        std::move(values), completed));
    CancelAccepted(accepted, cancellation, clientCancellation.release,
                   "client-streaming");
    Require(result.wait_for(3s) == std::future_status::ready,
            "cancelled client-streaming call did not complete");
    Require(result.get().status.error_code() == grpc::StatusCode::CANCELLED,
            "cancelled client-streaming status differs");
  }

  {
    std::stop_source cancellation;
    auto accepted = bidiCancellation.accepted.get_future();
    std::promise<servicelib::grpc_transport::StreamResult<
        servicelib::test::EchoResponse>> completed;
    auto result = completed.get_future();
    std::vector<servicelib::test::EchoRequest> values(1);
    values[0].set_value("cancel-bidi");
    runtime.SpawnGrpc(RunCancelledBidirectionalStream(
        runtime, *stub,
        servicelib::MessageContext{}
            .withStopToken(cancellation.get_token())
            .withDeadline(std::chrono::steady_clock::now() + 3s),
        std::move(values), completed));
    CancelAccepted(accepted, cancellation, bidiCancellation.release,
                   "bidirectional-streaming");
    Require(result.wait_for(3s) == std::future_status::ready,
            "cancelled bidirectional call did not complete");
    Require(result.get().status.error_code() == grpc::StatusCode::CANCELLED,
            "cancelled bidirectional status differs");
  }

  {
    std::atomic<std::size_t> createdClients{0};
    servicelib::grpc_transport::ClientPool<
        servicelib::test::ConnectorTest::Stub>
        pool{runtime.grpcContext(), "127.0.0.1:" + std::to_string(port), 3,
             [&createdClients](std::shared_ptr<grpc::Channel> value) {
               createdClients.fetch_add(1, std::memory_order_relaxed);
               return servicelib::test::ConnectorTest::NewStub(
                   std::move(value));
             }};
    Require(createdClients.load(std::memory_order_relaxed) == 3,
            "gRPC connectionsCount did not create the configured channels");

    {
      std::vector<std::string> values;
      auto completed = std::make_shared<std::promise<std::exception_ptr>>();
      auto result = completed->get_future();
      pool.asyncServerStreaming<
          &servicelib::test::ConnectorTest::Stub::PrepareAsyncServerStreaming,
          servicelib::test::EchoRequest, servicelib::test::EchoResponse>(
          requests[0], servicelib::datasink::grpc::callOptions(context),
          [&](servicelib::test::EchoResponse response) {
            values.push_back(response.value());
          },
          [completed](std::exception_ptr error) {
            completed->set_value(error);
          });
      Require(result.wait_for(5s) == std::future_status::ready,
              "pooled server-streaming call did not complete");
      Require(!result.get(), "pooled server-streaming call returned an error");
      Require((values == std::vector<std::string>{"a0", "a1", "a2"}),
              "pooled server-streaming payload/EOF contract differs");
    }

    {
      std::string value;
      auto completed = std::make_shared<std::promise<std::exception_ptr>>();
      auto result = completed->get_future();
      auto writer = pool.asyncClientStreaming<
          &servicelib::test::ConnectorTest::Stub::PrepareAsyncClientStreaming,
          servicelib::test::EchoRequest, servicelib::test::EchoResponse>(
          servicelib::datasink::grpc::callOptions(context),
          [&](servicelib::test::EchoResponse response) {
            value = response.value();
          },
          [completed](std::exception_ptr error) {
            completed->set_value(error);
          });
      writer->write(requests[0]);
      writer->write(requests[1]);
      writer->done();
      Require(result.wait_for(5s) == std::future_status::ready,
              "pooled client-streaming call did not complete");
      Require(!result.get(), "pooled client-streaming call returned an error");
      Require(value == "ab",
              "pooled client-streaming payload/EOF contract differs");
    }

    {
      std::vector<std::string> values;
      auto completed = std::make_shared<std::promise<std::exception_ptr>>();
      auto result = completed->get_future();
      auto writer = pool.asyncBidirectionalStreaming<
          &servicelib::test::ConnectorTest::Stub::
              PrepareAsyncBidirectionalStreaming,
          servicelib::test::EchoRequest, servicelib::test::EchoResponse>(
          servicelib::datasink::grpc::callOptions(context),
          [&](servicelib::test::EchoResponse response) {
            values.push_back(response.value());
          },
          [completed](std::exception_ptr error) {
            completed->set_value(error);
          });
      writer->write(requests[0]);
      writer->write(requests[1]);
      writer->done();
      Require(result.wait_for(5s) == std::future_status::ready,
              "pooled bidirectional call did not complete");
      Require(!result.get(), "pooled bidirectional call returned an error");
      Require((values == std::vector<std::string>{"bidi:a", "bidi:b"}),
              "pooled bidirectional payload/EOF contract differs");
    }
  }

  {
    auto accepted = shutdownCancellation.accepted.get_future();
    std::promise<servicelib::grpc_transport::StreamResult<
        servicelib::test::EchoResponse>> completed;
    auto result = completed.get_future();
    servicelib::test::EchoRequest value;
    value.set_value("shutdown-server");
    runtime.SpawnGrpc(RunCancelledServerStream(
        runtime, *stub,
        servicelib::MessageContext{}.withDeadline(
            std::chrono::steady_clock::now() + 5s),
        std::move(value), completed));
    Require(accepted.wait_for(3s) == std::future_status::ready,
            "shutdown streaming handler did not accept the request");
    auto shutdownContext = accepted.get();
    server->Shutdown(std::chrono::system_clock::now() + 100ms);
    const auto cancellationDeadline = std::chrono::steady_clock::now() + 2s;
    while (!shutdownContext.cancelled() &&
           std::chrono::steady_clock::now() < cancellationDeadline) {
      std::this_thread::sleep_for(1ms);
    }
    Require(shutdownContext.cancelled(),
            "server shutdown did not cancel accepted streaming context");
    shutdownCancellation.release.Send();
    Require(result.wait_for(3s) == std::future_status::ready,
            "shutdown streaming client did not complete");
    Require(!result.get().ok(),
            "forced server shutdown returned an OK streaming status");
  }
  runtime.Stop();
  runtime.Join();
}

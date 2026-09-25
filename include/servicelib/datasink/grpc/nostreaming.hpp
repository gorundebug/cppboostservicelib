#pragma once

#include <functional>
#include <servicelib/runtime/detail/sync.hpp>

#include <servicelib/datasink/grpc/common.hpp>

namespace servicelib::datasink::grpc {

template <typename Req, typename Res, typename T, typename R, typename Handler,
          typename ClientFunction, typename E = std::exception_ptr>
class NoStreamingEndpoint final : public Endpoint<T, R, Handler, E> {
 public:
 private:
  using AsyncCompletion =
      std::function<void(std::exception_ptr, std::optional<Res>)>;

 public:

  NoStreamingEndpoint(SinkEndpointStream<T, R, E>& stream, Handler handler,
                      ClientFunction client)
      : Endpoint<T, R, Handler, E>(stream,
                                  api::GrpcMethodType::kNoStreaming,
                                  std::move(handler)),
        client_(std::move(client)) {}

  void consume(MessageContext context, Payload<T> payload) {
    auto operation = this->asyncOperations_.acquire();
    auto completion = context.retainCompletionToken();
      auto startedSpan = this->startTrace(context);
    std::optional<servicelib::BeginResult<typename Handler::State>> begin;
    try {
      begin.emplace(this->handler_.beginRequest(context, this->streamContext_));
    } catch (...) {
      const auto error = std::current_exception();
      this->traceError(startedSpan.span(), error, "begin_request.error");
      this->metrics_.beginRequestFailed(tracing::ExceptionMessage(error));
      return;
    }
    if (auto* traceSpan = startedSpan.span()) traceSpan->addEvent("begin_request");
    context = std::move(begin->context);
    const auto requestContext = this->newRequestStreamId(context);
    const auto startedAt = this->metrics_.requestStart();
    std::exception_ptr error;
    std::optional<Req> request;
    try {
      Sender<Req> sender{[&](Req value) { request.emplace(std::move(value)); }};
      this->handler_.consumeMessage(context, this->streamContext_, begin->state,
                                    payload.get(), sender, ResultContext{});
      if (!request) {
        throw std::runtime_error("gRPC sink handler sent no request");
      }
      if (auto* traceSpan = startedSpan.span()) traceSpan->addEvent("consume_message");
    } catch (...) {
      error = std::current_exception();
      this->traceError(startedSpan.span(), error, "consume_message.error");
    }
    std::optional<Res> response;
    if (!error) {
      try {
        if (!operation) throw std::runtime_error("gRPC sink endpoint is stopped");
        response.emplace(callClient(std::move(*request),
                                     callOptions(requestContext, this->tracingEnabled())));
        if (auto* traceSpan = startedSpan.span()) traceSpan->addEvent("grpc_call");
      } catch (...) {
        error = std::current_exception();
        this->traceError(startedSpan.span(), error, "grpc_call.error");
      }
    }
    if (!error) {
      try {
        this->handler_.handleResponse(context, this->streamContext_,
                                      begin->state, *response);
        if (auto* traceSpan = startedSpan.span()) traceSpan->addEvent("handle_response");
      } catch (...) {
        error = std::current_exception();
        this->traceError(startedSpan.span(), error, "handle_response.error");
      }
    }
    this->callEnd(context, error, begin->state);
      this->metrics_.requestEnd(startedAt, error);
  }

 private:
  Res callClient(Req request, CallOptions options) {
    if constexpr (requires(ClientFunction& client, AsyncCompletion completion) {
                    client.async(std::move(request), std::move(options),
                                 std::move(completion));
                  }) {
      struct Pending final {
        servicelib::detail::SingleUseEvent done;
        std::exception_ptr error;
        std::optional<Res> response;
      };
      auto pending = std::make_shared<Pending>();
      client_.async(std::move(request), std::move(options),
          [pending](std::exception_ptr error, std::optional<Res> response) {
            pending->error = std::move(error);
            pending->response = std::move(response);
            pending->done.Send();
          });
      // The completion callback only publishes the transport result. Business
      // handlers resume on the caller's stack, never on a CQ callback worker.
      pending->done.Wait();
      if (pending->error) std::rethrow_exception(pending->error);
      if (!pending->response)
        throw std::runtime_error("gRPC call returned no response");
      return std::move(*pending->response);
    } else {
      return std::invoke(client_, std::move(request), std::move(options));
    }
  }

  ClientFunction client_;
};

}  // namespace servicelib::datasink::grpc

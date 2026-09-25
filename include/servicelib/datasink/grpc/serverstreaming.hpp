#pragma once

#include <functional>
#include <servicelib/runtime/detail/sync.hpp>

#include <servicelib/datasink/grpc/common.hpp>

namespace servicelib::datasink::grpc {

template <typename Req, typename Res, typename T, typename R, typename Handler,
          typename ClientFunction, typename E = std::exception_ptr>
class ServerStreamingEndpoint final : public Endpoint<T, R, Handler, E> {
 public:
 private:
  using AsyncResponse = std::function<void(Res)>;
  using AsyncCompletion = std::function<void(std::exception_ptr)>;

 public:

  ServerStreamingEndpoint(SinkEndpointStream<T, R, E>& stream,
                          Handler handler, ClientFunction client)
      : Endpoint<T, R, Handler, E>(stream,
                                   api::GrpcMethodType::kServerStreaming,
                                   std::move(handler)),
        client_(std::move(client)) {}

  void consume(MessageContext context, Payload<T> payload) {
    if constexpr (requires(ClientFunction& client, Req request,
                           CallOptions options, AsyncResponse response,
                           AsyncCompletion completion) {
                    client.async(std::move(request), std::move(options),
                                 std::move(response), std::move(completion));
                  }) {
      consumeAsync(std::move(context), std::move(payload));
      return;
    } else {
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
    if (!error) {
      try {
        auto rpc = std::invoke(client_, std::move(*request),
                               callOptions(requestContext, this->tracingEnabled()));
        if (auto* traceSpan = startedSpan.span()) traceSpan->addEvent("grpc_call");
        error =
            receiveResponses(context, begin->state, rpc, startedSpan.span());
      } catch (...) {
        error = std::current_exception();
        this->traceError(startedSpan.span(), error, "grpc_call.error");
      }
    }
    this->callEnd(context, error, begin->state);
    this->metrics_.requestEnd(startedAt, error);
    }
  }

 private:
  struct AsyncState final {
    AsyncState(MessageContext contextValue, typename Handler::State stateValue,
               DataSinkEndpointMetrics::Clock::time_point started,
               std::shared_ptr<tracing::Span> requestSpan,
               std::shared_ptr<servicelib::detail::AsyncOperations::Token>
                   token,
               servicelib::AsyncCompletionToken completionToken)
        : context(std::move(contextValue)),
          state(std::move(stateValue)),
          startedAt(started),
          span(std::move(requestSpan)),
          operation(std::move(token)),
          completion(std::move(completionToken)) {}
    MessageContext context;
    typename Handler::State state;
    DataSinkEndpointMetrics::Clock::time_point startedAt;
    std::shared_ptr<tracing::Span> span;
    std::shared_ptr<servicelib::detail::AsyncOperations::Token> operation;
    servicelib::AsyncCompletionToken completion;
    servicelib::detail::CooperativeMutex mutex;
    servicelib::detail::SingleUseEvent done;
    bool finished{};
    std::exception_ptr callError;
    std::exception_ptr responseError;
    std::int64_t messageCount{};
  };

  void consumeAsync(MessageContext context, Payload<T> payload) {
    auto completion = context.retainCompletionToken();
    auto trace = this->startDetachedTrace(std::move(context));
    context = std::move(trace.context);
    std::optional<servicelib::BeginResult<typename Handler::State>> begin;
    try {
      begin.emplace(this->handler_.beginRequest(context, this->streamContext_));
    } catch (...) {
      const auto error = std::current_exception();
      this->traceError(trace.span.get(), error, "begin_request.error");
      this->metrics_.beginRequestFailed(tracing::ExceptionMessage(error));
      if (trace.span) tracing::SpanEnd(trace.span.get());
      return;
    }
    if (auto* traceSpan = trace.span.get()) traceSpan->addEvent("begin_request");
    context = std::move(begin->context);
    const auto requestContext = this->newRequestStreamId(context);
    const auto startedAt = this->metrics_.requestStart();
    std::optional<Req> request;
    std::exception_ptr error;
    try {
      Sender<Req> sender{[&](Req value) { request.emplace(std::move(value)); }};
      this->handler_.consumeMessage(context, this->streamContext_, begin->state,
                                    payload.get(), sender, ResultContext{});
      if (!request) {
        throw std::runtime_error("gRPC sink handler sent no request");
      }
      if (auto* traceSpan = trace.span.get()) traceSpan->addEvent("consume_message");
    } catch (...) {
      error = std::current_exception();
      this->traceError(trace.span.get(), error, "consume_message.error");
    }
    if (error) {
      this->callEnd(context, error, begin->state);
      this->metrics_.requestEnd(startedAt, error);
      if (trace.span) tracing::SpanEnd(trace.span.get());
      return;
    }
    auto operation = this->asyncOperations_.acquire();
    if (!operation) {
      error = std::make_exception_ptr(
          std::runtime_error("gRPC sink endpoint is stopped"));
      this->traceError(trace.span.get(), error, "grpc_call.error");
      this->callEnd(context, error, begin->state);
      this->metrics_.requestEnd(startedAt, error);
      if (trace.span) tracing::SpanEnd(trace.span.get());
      return;
    }
    auto state = std::make_shared<AsyncState>(
        std::move(context), std::move(begin->state), startedAt,
        std::move(trace.span), std::move(operation), std::move(completion));
    const auto finish = [state](std::exception_ptr callError) {
      {
        std::lock_guard lock(state->mutex);
        if (state->finished) return;
        state->finished = true;
        state->callError = std::move(callError);
      }
      state->done.Send();
    };
    try {
      client_.async(
          std::move(*request), callOptions(requestContext, this->tracingEnabled()),
          [this, state](Res response) {
            std::lock_guard lock(state->mutex);
            if (state->finished || state->responseError) return;
            try {
              this->handler_.handleResponse(
                  state->context, this->streamContext_, state->state, response);
              ++state->messageCount;
            } catch (...) {
              state->responseError = std::current_exception();
              this->traceError(state->span.get(), state->responseError,
                               "handle_response.error");
              throw;
            }
          },
          finish);
      if (auto* traceSpan = state->span.get()) traceSpan->addEvent("grpc_call");
    } catch (...) {
      finish(std::current_exception());
    }
    // A FunctionCall completes after the RPC and its business handlers, not
    // merely after starting the transport. Waiting releases a reactor worker.
    state->done.Wait();
    {
      std::lock_guard lock(state->mutex);
      error = state->responseError ? state->responseError : state->callError;
    }
    if (error) {
      this->traceError(state->span.get(), error, "grpc_call.error");
    } else if (auto* traceSpan = state->span.get()) {
      traceSpan->addEvent("eof", {tracing::Attribute::Int64(
          "messages_received", state->messageCount)});
    }
    this->callEnd(state->context, error, state->state);
    this->metrics_.requestEnd(state->startedAt, error);
    if (state->span) tracing::SpanEnd(state->span.get());
    state->completion.reset();
    state->operation.reset();
  }

  template <typename Rpc>
  std::exception_ptr receiveResponses(const MessageContext& context,
                                      typename Handler::State& state, Rpc& rpc,
                                      tracing::Span* span) {
    Res response;
    std::int64_t messageCount = 0;
    for (;;) {
      bool received = false;
      try {
        received = rpc.Read(response);
      } catch (...) {
        const auto error = std::current_exception();
        this->traceError(span, error, "recv.error");
        return error;
      }
      if (!received) {
        if (auto* traceSpan = 
            span) traceSpan->addEvent("eof",
            {tracing::Attribute::Int64("messages_received", messageCount)});
        return {};
      }
      try {
        this->handler_.handleResponse(context, this->streamContext_, state,
                                      response);
        ++messageCount;
      } catch (...) {
        const auto error = std::current_exception();
        this->traceError(span, error, "handle_response.error");
        return error;
      }
    }
  }

  ClientFunction client_;
};

}  // namespace servicelib::datasink::grpc

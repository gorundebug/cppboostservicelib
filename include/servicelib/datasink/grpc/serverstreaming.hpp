#pragma once

#include <functional>

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
    tracing::SpanEvent(startedSpan.span(), "begin_request");
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
      tracing::SpanEvent(startedSpan.span(), "consume_message");
    } catch (...) {
      error = std::current_exception();
      this->traceError(startedSpan.span(), error, "consume_message.error");
    }
    if (!error) {
      try {
        auto rpc = std::invoke(client_, std::move(*request),
                               callOptions(requestContext));
        tracing::SpanEvent(startedSpan.span(), "grpc_call");
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
               std::shared_ptr<servicelib::AsyncCompletionToken>
                   completionToken)
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
    std::shared_ptr<servicelib::AsyncCompletionToken> completion;
    std::mutex mutex;
    std::exception_ptr responseError;
    std::int64_t messageCount{};
  };

  void consumeAsync(MessageContext context, Payload<T> payload) {
    auto completion = context.retainCompletion();
    auto trace = this->startDetachedTrace(std::move(context));
    context = std::move(trace.context);
    std::optional<servicelib::BeginResult<typename Handler::State>> begin;
    try {
      begin.emplace(this->handler_.beginRequest(context, this->streamContext_));
    } catch (...) {
      const auto error = std::current_exception();
      this->traceError(trace.span.get(), error, "begin_request.error");
      this->metrics_.beginRequestFailed(tracing::ExceptionMessage(error));
      tracing::SpanEnd(trace.span.get());
      return;
    }
    tracing::SpanEvent(trace.span.get(), "begin_request");
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
      tracing::SpanEvent(trace.span.get(), "consume_message");
    } catch (...) {
      error = std::current_exception();
      this->traceError(trace.span.get(), error, "consume_message.error");
    }
    if (error) {
      this->callEnd(context, error, begin->state);
      this->metrics_.requestEnd(startedAt, error);
      tracing::SpanEnd(trace.span.get());
      return;
    }
    auto operation = this->asyncOperations_.acquire();
    if (!operation) {
      error = std::make_exception_ptr(
          std::runtime_error("gRPC sink endpoint is stopped"));
      this->traceError(trace.span.get(), error, "grpc_call.error");
      this->callEnd(context, error, begin->state);
      this->metrics_.requestEnd(startedAt, error);
      tracing::SpanEnd(trace.span.get());
      return;
    }
    auto state = std::make_shared<AsyncState>(
        std::move(context), std::move(begin->state), startedAt,
        std::move(trace.span), std::move(operation), std::move(completion));
    try {
      client_.async(
          std::move(*request), callOptions(requestContext),
          [this, state](Res response) {
            std::lock_guard lock(state->mutex);
            if (state->responseError) return;
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
          [this, state](std::exception_ptr callError) mutable noexcept {
            {
              std::lock_guard lock(state->mutex);
              if (!callError) callError = state->responseError;
              if (!callError) {
                tracing::SpanEvent(
                    state->span.get(), "eof",
                    {tracing::Attribute::Int64("messages_received",
                                               state->messageCount)});
              }
            }
            if (callError) {
              this->traceError(state->span.get(), callError,
                               "grpc_call.error");
            }
            this->callEnd(state->context, callError, state->state);
            this->metrics_.requestEnd(state->startedAt, callError);
            tracing::SpanEnd(state->span.get());
            state->completion.reset();
          });
      tracing::SpanEvent(state->span.get(), "grpc_call");
    } catch (...) {
      error = std::current_exception();
      this->traceError(state->span.get(), error, "grpc_call.error");
      this->callEnd(state->context, error, state->state);
      this->metrics_.requestEnd(state->startedAt, error);
      tracing::SpanEnd(state->span.get());
      state->completion.reset();
    }
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
        tracing::SpanEvent(
            span, "eof",
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

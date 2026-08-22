#pragma once

#include <functional>
#include <shared_mutex>
#include <type_traits>


#include <servicelib/datasink/grpc/common.hpp>
#include <servicelib/runtime/store/rotatingmap.hpp>
#include <servicelib/runtime/detail/sync.hpp>
#include <condition_variable>
#include <mutex>

namespace servicelib::datasink::grpc {

namespace detail {
template <typename ClientFunction,
          bool = requires { typename ClientFunction::AsyncSession; }>
struct ClientStreamingRpcType final {
  using type = std::remove_cvref_t<
      std::invoke_result_t<ClientFunction&, CallOptions>>;
};

template <typename ClientFunction>
struct ClientStreamingRpcType<ClientFunction, true> final {
  using type = typename ClientFunction::AsyncSession;
};
}  // namespace detail

template <typename Req, typename Res, typename T, typename R, typename Handler,
          typename ClientFunction, typename E = std::exception_ptr>
class ClientStreamingEndpoint final : public Endpoint<T, R, Handler, E> {
 public:
  using State = typename Handler::State;
  using Rpc = typename detail::ClientStreamingRpcType<ClientFunction>::type;
  struct Session final {
    Session(MessageContext contextValue, State stateValue, Rpc rpcValue,
            DataSinkEndpointMetrics::Clock::time_point started,
            std::shared_ptr<tracing::Span> requestSpan)
        : context(std::move(contextValue)),
          state(std::move(stateValue)),
          rpc(std::move(rpcValue)),
          startedAt(started),
          span(std::move(requestSpan)) {}
    MessageContext context;
    State state;
    Rpc rpc;
    DataSinkEndpointMetrics::Clock::time_point startedAt;
    std::shared_ptr<tracing::Span> span;
    std::shared_ptr<servicelib::detail::AsyncOperations::Token> operation;
    servicelib::detail::SingleUseEvent done;
    std::atomic<bool> doneSent{false};
    std::shared_mutex lifetimeMutex;
  };

  // SessionCell reserves a pending-map slot for a streamId before the
  // corresponding Session can be built (building it requires beginRequest and
  // the actual gRPC call to be established, which may involve network I/O).
  // session stays null until markReady() is called, so it is never
  // observable in a partially-constructed state: a concurrent consume() for
  // the same still-being-created streamId waits via wait() and only ever
  // sees a fully built Session, exactly as if it had been serialized behind
  // a single endpoint-wide lock. Unlike servicelib::detail::SingleUseEvent
  // (single-consumer only), this supports any number of concurrent waiters.
  struct SessionCell final {
    std::mutex mu;
    std::condition_variable cv;
    bool ready = false;
    std::shared_ptr<Session> session;
    std::exception_ptr error;

    void markReady() {
      std::lock_guard<std::mutex> lock(mu);
      ready = true;
      cv.notify_all();
    }

    void wait() {
      std::unique_lock<std::mutex> lock(mu);
      static_cast<void>(cv.wait(lock, [this] { return ready; }));
    }
  };

  ClientStreamingEndpoint(SinkEndpointStream<T, R, E>& stream,
                          Handler handler, ClientFunction client)
      : Endpoint<T, R, Handler, E>(stream,
                                   api::GrpcMethodType::kClientStreaming,
                                   std::move(handler)),
        client_(std::move(client)),
        pending_(std::chrono::seconds{30}) {}

  void start(Context context) override {
    Endpoint<T, R, Handler, E>::start(context);
    pending_.start(std::move(context));
  }

  void stop(Context context) override {
    if constexpr (requires { typename ClientFunction::AsyncSession; }) {
      std::vector<std::shared_ptr<Session>> sessions;
      {
        std::lock_guard lock(sessionsMutex_);
        for (auto& weak : sessions_) {
          if (auto session = weak.lock()) sessions.push_back(std::move(session));
        }
      }
      for (auto& session : sessions) {
        if (session->rpc) session->rpc->cancel();
      }
    }
    tasks_.CancelAndWait();
    Endpoint<T, R, Handler, E>::stop(context);
    pending_.stop(std::move(context));
  }

  void consume(MessageContext context, Payload<T> payload) {
    if constexpr (requires { typename ClientFunction::AsyncSession; }) {
      consumeAsync(std::move(context), std::move(payload));
      return;
    } else {
    context = this->ensureStreamId(std::move(context));
    const std::string streamId{context.streamId()};

    auto [cell, loaded] = pending_.getOrCreate(
        streamId, [] { return std::make_shared<SessionCell>(); });

    std::shared_ptr<Session> session;
    if (!loaded) {
      auto detachedTrace = this->startDetachedTrace(std::move(context));
      context = std::move(detachedTrace.context);
      std::optional<servicelib::BeginResult<State>> begin;
      try {
        begin.emplace(
            this->handler_.beginRequest(context, this->streamContext_));
      } catch (...) {
        const auto error = std::current_exception();
        this->traceError(detachedTrace.span.get(), error,
                         "begin_request.error");
        tracing::SpanEnd(detachedTrace.span.get());
        this->metrics_.beginRequestFailed(tracing::ExceptionMessage(error));
        cell->error = error;
        cell->markReady();
        dropReservation(streamId, cell);
        return;
      }
      tracing::SpanEvent(detachedTrace.span.get(), "begin_request");
      context = std::move(begin->context);
      const auto requestContext = this->newRequestStreamId(context);
      const auto startedAt = this->metrics_.requestStart();
      try {
        session = std::make_shared<Session>(
            context, std::move(begin->state),
            std::invoke(client_, callOptions(requestContext)), startedAt,
            detachedTrace.span);
        tracing::SpanEvent(session->span.get(), "grpc_call");
      } catch (...) {
        const auto error = std::current_exception();
        this->traceError(detachedTrace.span.get(), error, "grpc_call.error");
        this->callEnd(context, error, begin->state);
        this->metrics_.requestEnd(startedAt, error);
        tracing::SpanEnd(detachedTrace.span.get());
        cell->error = error;
        cell->markReady();
        dropReservation(streamId, cell);
        return;
      }
      cell->session = session;
      cell->markReady();
      tasks_.CriticalAsyncDetach(
          "servicelib-grpc-client-stream",
          [this, streamId, session] { complete(streamId, session); });
    } else {
      cell->wait();
      if (cell->error) {
        // Creation failed on another coroutine; it has already reported and
        // cleaned up, so this message is simply dropped.
        return;
      }
      session = cell->session;
    }

    std::shared_lock lifetimeLock(session->lifetimeMutex);
    const auto current = pending_.get(streamId);
    if (!current || *current != cell) {
      this->metrics_.lateResult(streamId);
      return;
    }
    Sender<Req> sender{
        [session](Req request) { session->rpc.WriteAndCheck(request); },
        session->span};
    ResultContext result{[session] {
      bool expected = false;
      if (session->doneSent.compare_exchange_strong(
              expected, true, std::memory_order_acq_rel)) {
        tracing::SpanEvent(session->span.get(), "done_called");
        session->done.Send();
      }
    }};
    try {
      this->handler_.consumeMessage(session->context, this->streamContext_,
                                    session->state, payload.get(), sender,
                                    result);
      tracing::SpanEvent(session->span.get(), "consume_message");
    } catch (...) {
      this->traceError(session->span.get(), std::current_exception(),
                       "consume_message.error");
      result.done();
    }
    }
  }

 private:
  void consumeAsync(MessageContext context, Payload<T> payload) {
    context = this->ensureStreamId(std::move(context));
    const std::string streamId{context.streamId()};
    auto [cell, loaded] = pending_.getOrCreate(
        streamId, [] { return std::make_shared<SessionCell>(); });
    std::shared_ptr<Session> session;
    if (!loaded) {
      auto trace = this->startDetachedTrace(std::move(context));
      context = std::move(trace.context);
      std::optional<servicelib::BeginResult<State>> begin;
      try {
        begin.emplace(
            this->handler_.beginRequest(context, this->streamContext_));
      } catch (...) {
        const auto error = std::current_exception();
        this->traceError(trace.span.get(), error, "begin_request.error");
        this->metrics_.beginRequestFailed(tracing::ExceptionMessage(error));
        tracing::SpanEnd(trace.span.get());
        cell->error = error;
        cell->markReady();
        dropReservation(streamId, cell);
        return;
      }
      tracing::SpanEvent(trace.span.get(), "begin_request");
      context = std::move(begin->context);
      const auto requestContext = this->newRequestStreamId(context);
      const auto startedAt = this->metrics_.requestStart();
      auto operation = this->asyncOperations_.acquire();
      if (!operation) {
        const auto error = std::make_exception_ptr(
            std::runtime_error("gRPC sink endpoint is stopped"));
        this->traceError(trace.span.get(), error, "grpc_call.error");
        this->callEnd(context, error, begin->state);
        this->metrics_.requestEnd(startedAt, error);
        tracing::SpanEnd(trace.span.get());
        cell->error = error;
        cell->markReady();
        dropReservation(streamId, cell);
        return;
      }
      session = std::make_shared<Session>(
          context, std::move(begin->state), Rpc{}, startedAt, trace.span);
      session->operation = std::move(operation);
      try {
        std::weak_ptr<Session> weakSession = session;
        session->rpc = client_.start(
            callOptions(requestContext),
            [this, weakSession](Res response) {
              const auto current = weakSession.lock();
              if (!current) return;
              std::shared_lock lifetimeLock(current->lifetimeMutex);
              this->handler_.handleResponse(
                  current->context, this->streamContext_, current->state,
                  response);
              tracing::SpanEvent(current->span.get(), "handle_response");
            },
            [this, streamId, weakSession](std::exception_ptr error) noexcept {
              const auto current = weakSession.lock();
              if (!current) return;
              std::unique_lock lifetimeLock(current->lifetimeMutex);
              static_cast<void>(pending_.pop(streamId));
              if (error) this->traceError(current->span.get(), error);
              this->callEnd(current->context, error, current->state);
              this->metrics_.requestEnd(current->startedAt, error);
              tracing::SpanEnd(current->span.get());
            });
        tracing::SpanEvent(session->span.get(), "grpc_call");
      } catch (...) {
        const auto error = std::current_exception();
        this->traceError(session->span.get(), error, "grpc_call.error");
        this->callEnd(session->context, error, session->state);
        this->metrics_.requestEnd(session->startedAt, error);
        tracing::SpanEnd(session->span.get());
        cell->error = error;
        cell->markReady();
        dropReservation(streamId, cell);
        return;
      }
      cell->session = session;
      cell->markReady();
      {
        std::lock_guard lock(sessionsMutex_);
        sessions_.push_back(session);
      }
    } else {
      cell->wait();
      if (cell->error) return;
      session = cell->session;
    }

    std::shared_lock lifetimeLock(session->lifetimeMutex);
    const auto current = pending_.get(streamId);
    if (!current || *current != cell) {
      this->metrics_.lateResult(streamId);
      return;
    }
    Sender<Req> sender{
        [session](Req request) { session->rpc->write(std::move(request)); },
        session->span};
    ResultContext result{[session] {
      bool expected = false;
      if (session->doneSent.compare_exchange_strong(
              expected, true, std::memory_order_acq_rel)) {
        tracing::SpanEvent(session->span.get(), "done_called");
        session->rpc->done();
        tracing::SpanEvent(session->span.get(), "done_received");
      }
    }};
    try {
      this->handler_.consumeMessage(session->context, this->streamContext_,
                                    session->state, payload.get(), sender,
                                    result);
      tracing::SpanEvent(session->span.get(), "consume_message");
    } catch (...) {
      this->traceError(session->span.get(), std::current_exception(),
                       "consume_message.error");
      try {
        result.done();
      } catch (...) {
      }
    }
  }

  void dropReservation(const std::string& streamId,
                       const std::shared_ptr<SessionCell>& cell) {
    const auto current = pending_.get(streamId);
    if (current && *current == cell) {
      static_cast<void>(pending_.pop(streamId));
    }
  }

  void complete(const std::string& streamId,
                const std::shared_ptr<Session>& session) {
    struct EndSpan final {
      std::shared_ptr<tracing::Span> span;
      ~EndSpan() {
        if (span) span->end();
      }
    } endSpan{session->span};
    std::exception_ptr error;
    std::unique_lock<std::shared_mutex> lifetimeLock(session->lifetimeMutex,
                                                     std::defer_lock);
    try {
      session->done.Wait();
      tracing::SpanEvent(session->span.get(), "done_received");
      lifetimeLock.lock();
      static_cast<void>(pending_.pop(streamId));
      std::optional<Res> response;
      try {
        response.emplace(session->rpc.Finish());
        tracing::SpanEvent(session->span.get(), "close_and_recv");
      } catch (...) {
        this->traceError(session->span.get(), std::current_exception(),
                         "close_and_recv.error");
        throw;
      }
      try {
        this->handler_.handleResponse(session->context, this->streamContext_,
                                      session->state, *response);
        tracing::SpanEvent(session->span.get(), "handle_response");
      } catch (...) {
        this->traceError(session->span.get(), std::current_exception(),
                         "handle_response.error");
        throw;
      }
    } catch (...) {
      error = std::current_exception();
      this->traceError(session->span.get(), error);
      if (!lifetimeLock.owns_lock()) lifetimeLock.lock();
      static_cast<void>(pending_.pop(streamId));
    }
    this->callEnd(session->context, error, session->state);
    this->metrics_.requestEnd(session->startedAt, error);
  }

  ClientFunction client_;
  store::RotatingMap<std::string, std::shared_ptr<SessionCell>> pending_;
  // Last: detached tasks are cancelled and joined before captured fields die.
  servicelib::detail::TaskStorage tasks_;
  std::mutex sessionsMutex_;
  std::vector<std::weak_ptr<Session>> sessions_;
};

}  // namespace servicelib::datasink::grpc

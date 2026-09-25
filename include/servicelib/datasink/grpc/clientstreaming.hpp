#pragma once

#include <functional>
#include <mutex>
#include <shared_mutex>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <servicelib/datasink/grpc/streaming_lifecycle.hpp>

#include <type_traits>


#include <servicelib/datasink/grpc/common.hpp>
#include <servicelib/runtime/store/rotatingmap.hpp>
#include <servicelib/runtime/detail/sync.hpp>



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
    std::string streamId;
    MessageContext context;
    State state;
    Rpc rpc;
    DataSinkEndpointMetrics::Clock::time_point startedAt;
    std::shared_ptr<tracing::Span> span;
    std::shared_ptr<detail::StreamingActivity::Token> operation;
    servicelib::detail::SingleUseEvent done;
    std::atomic<bool> doneSent{false};
    detail::StreamingActivity lifetime;
    std::mutex responseErrorMutex;
    std::exception_ptr responseError;
    servicelib::detail::CooperativeSharedMutex handlerMutex;
    std::atomic<bool> terminalStarted{false};
    bool responseHandled{};
  };

  using SessionCell = detail::StreamingCell<Session>;

  ClientStreamingEndpoint(SinkEndpointStream<T, R, E>& stream,
                          Handler handler, ClientFunction client)
      : Endpoint<T, R, Handler, E>(stream,
                                   api::GrpcMethodType::kClientStreaming,
                                   std::move(handler)),
        client_(std::move(client)),
        pending_(std::chrono::seconds{30}),
        executor_(servicelib::detail::ParallelExecutorRegistry::Get()) {}

  void start(Context context) override {
    Endpoint<T, R, Handler, E>::start(context);
    pending_.start(std::move(context));
  }

  void stop(Context context) override {
    operations_.close();
    sessions_.close();
    operations_.wait();
    tasks_.CancelAndWait();
    Endpoint<T, R, Handler, E>::stop(context);
    pending_.stop(std::move(context));
  }

  void consume(MessageContext context, Payload<T> payload) {
    auto operation = operations_.acquire();
    if (!operation) return;
    if constexpr (requires { typename ClientFunction::AsyncSession; }) {
      consumeAsync(std::move(context), std::move(payload), std::move(operation));
      return;
    } else {
      context = this->ensureStreamId(std::move(context));
      const std::string streamId{context.streamId()};

      auto [cell, loaded] = pending_.getOrCreate(
          streamId, [] { return std::make_shared<SessionCell>(); });
      if (rejectCompletingSession(cell, loaded)) return;

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
          if (detachedTrace.span) tracing::SpanEnd(detachedTrace.span.get());
          this->metrics_.beginRequestFailed(tracing::ExceptionMessage(error));
          cell->error = error;
          cell->markReady();
          dropReservation(streamId, cell);
          return;
        }
        if (auto* traceSpan = detachedTrace.span.get()) traceSpan->addEvent("begin_request");
        context = std::move(begin->context);
        const auto requestContext = this->newRequestStreamId(context);
        const auto startedAt = this->metrics_.requestStart();
        try {
          session = std::make_shared<Session>(
              context, std::move(begin->state),
              std::invoke(client_, callOptions(requestContext, this->tracingEnabled())), startedAt,
              detachedTrace.span);
          if (auto* traceSpan = session->span.get()) traceSpan->addEvent("grpc_call");
        } catch (...) {
          const auto error = std::current_exception();
          this->traceError(detachedTrace.span.get(), error, "grpc_call.error");
          cell->error = error;
          cell->markReady();
          this->callEnd(context, error, begin->state);
          this->metrics_.requestEnd(startedAt, error);
          if (detachedTrace.span) tracing::SpanEnd(detachedTrace.span.get());
          dropReservation(streamId, cell);
          return;
        }
        session->streamId = streamId;
        cell->session = session;
        cell->markReady();
        tasks_.CriticalAsyncDetach(
            "servicelib-grpc-client-stream",
            [this, streamId, session] { complete(streamId, session); });
      }
      deliverOrWait(cell, std::move(context), std::move(payload), std::move(operation));
    }
  }

 private:
  bool rejectCompletingSession(const std::shared_ptr<SessionCell>& cell,
                               bool loaded) {
    // Messages may share an open RPC, but cannot reopen its ID while response
    // delivery or EndRequest still owns the previous invocation. Check before
    // taking handlerMutex, including a reentrant call from HandleResponse.
    if (loaded && cell->ready.IsReady() &&
        (cell->error || (cell->session &&
                        cell->session->terminalStarted.load(std::memory_order_acquire)))) {
      this->metrics_.beginRequestFailed("gRPC client-streaming session is still completing");
      return true;
    }
    return false;
  }

  void consumeAsync(MessageContext context, Payload<T> payload,
                    std::shared_ptr<detail::StreamingActivity::Token> operation) {
    context = this->ensureStreamId(std::move(context));
    const std::string streamId{context.streamId()};
    auto [cell, loaded] = pending_.getOrCreate(
        streamId, [] { return std::make_shared<SessionCell>(); });
    if (rejectCompletingSession(cell, loaded)) return;
    std::shared_ptr<Session> session;
    if (!loaded) {
      cell->registration = sessions_.add(cell);
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
        if (trace.span) tracing::SpanEnd(trace.span.get());
        cell->error = error;
        cell->markReady();
        dropReservation(streamId, cell);
        return;
      }
      if (auto* traceSpan = trace.span.get()) traceSpan->addEvent("begin_request");
      context = std::move(begin->context);
      const auto requestContext = this->newRequestStreamId(context);
      const auto startedAt = this->metrics_.requestStart();
      session = std::make_shared<Session>(
          context, std::move(begin->state), Rpc{}, startedAt, trace.span);
      session->streamId = streamId;
      session->operation = operation;
      auto initializing = session->lifetime.acquire();
      try {
        std::weak_ptr<Session> weakSession = session;
        session->rpc = client_.start(
            callOptions(requestContext, this->tracingEnabled()),
            [this, weakSession, weakCell = std::weak_ptr<SessionCell>{cell}](Res response) {
              const auto current = weakSession.lock();
              if (!current) return;
              auto active = current->lifetime.acquire();
              if (!active) return;
              auto cell = weakCell.lock();
              if (!cell) return;
              // Like Go's receive goroutine, the single response is independent
              // of ConsumeMessage, including transports that callback inline.
              servicelib::detail::CooperativeExecution::Post(executor_,
                  [this, cell = std::move(cell), current,
                   response = std::move(response), active = std::move(active)]() mutable {
                    respondWhenReady(this, std::move(cell), current,
                                     std::move(response), std::move(active));
                  });
            },
            [this, weakSession](std::exception_ptr error) noexcept {
              const auto current = weakSession.lock();
              if (!current) return;
              finishAsync(current, error);
            });
        if (auto* traceSpan = session->span.get()) traceSpan->addEvent("grpc_call");
      } catch (...) {
        const auto error = std::current_exception();
        this->traceError(session->span.get(), error, "grpc_call.error");
        cell->error = error;
        cell->markReady();
        finishAsync(session, error);
        return;
      }
      session->streamId = streamId;
      cell->session = session;
      cell->markReady();
    }
    deliverOrWait(cell, std::move(context), std::move(payload), std::move(operation));
  }

  void deliverOrWait(const std::shared_ptr<SessionCell>& cell,
                     [[maybe_unused]] MessageContext context, Payload<T> payload,
                     [[maybe_unused]] std::shared_ptr<detail::StreamingActivity::Token> operation) {
    // Like Go, wait for the shared creation result before processing this message.
    // This suspends a cooperative caller without detaching its business work.
    cell->ready.Wait();
    if (!cell->error) deliver(cell, std::move(payload));
  }

  void deliver(const std::shared_ptr<SessionCell>& cell, Payload<T> payload) {
    const auto& session = cell->session;
    auto active = session->lifetime.acquire();
    if (!active) return;
    const auto& streamId = session->streamId;
    if constexpr (requires { typename ClientFunction::AsyncSession; }) {
      std::shared_lock handlerLock(session->handlerMutex);
      const auto current = pending_.get(streamId);
      if (!current || *current != cell) {
        this->metrics_.lateResult(streamId);
        return;
      }
      Sender<Req> sender{
          [session, admitted = std::weak_ptr{active}](Req request) {
            auto active = admitted.lock();
            if (!active) active = session->lifetime.acquire();
            if (!active) return;
            try {
              session->rpc->write(std::move(request));
              if (auto* traceSpan = session->span.get()) traceSpan->addEvent("send");
            } catch (...) {
              if (auto* traceSpan = session->span.get()) {
                const auto message = tracing::ExceptionMessage(std::current_exception());
                tracing::SpanError(traceSpan, message);
                traceSpan->addEvent("send.error",
                                    {tracing::Attribute::String("error", message)});
              }
              throw;
            }
          }};
      ResultContext result{[session, admitted = std::weak_ptr{active}] {
        auto active = admitted.lock();
        if (!active) active = session->lifetime.acquire();
        if (!active) return;
        bool expected = false;
        if (session->doneSent.compare_exchange_strong(
                expected, true, std::memory_order_acq_rel)) {
          if (auto* traceSpan = session->span.get()) traceSpan->addEvent("done_called");
          session->rpc->done();
          if (auto* traceSpan = session->span.get()) traceSpan->addEvent("done_received");
        }
      }};
      try {
        this->handler_.consumeMessage(session->context, this->streamContext_,
                                      session->state, payload.get(), sender,
                                      result);
        if (auto* traceSpan = session->span.get()) traceSpan->addEvent("consume_message");
      } catch (...) {
        this->traceError(session->span.get(), std::current_exception(),
                         "consume_message.error");
        try {
          result.done();
        } catch (...) {
        }
      }
    } else {
      const auto current = pending_.get(streamId);
      if (!current || *current != cell) {
        this->metrics_.lateResult(streamId);
        return;
      }
      Sender<Req> sender{
          [session, admitted = std::weak_ptr{active}](Req request) {
            auto active = admitted.lock();
            if (!active) active = session->lifetime.acquire();
            if (!active) return;
            try {
              session->rpc.WriteAndCheck(request);
              if (auto* traceSpan = session->span.get()) traceSpan->addEvent("send");
            } catch (...) {
              if (auto* traceSpan = session->span.get()) {
                const auto message = tracing::ExceptionMessage(std::current_exception());
                tracing::SpanError(traceSpan, message);
                traceSpan->addEvent("send.error",
                                    {tracing::Attribute::String("error", message)});
              }
              throw;
            }
          }};
      ResultContext result{[session, admitted = std::weak_ptr{active}] {
        auto active = admitted.lock();
        if (!active) active = session->lifetime.acquire();
        if (!active) return;
        bool expected = false;
        if (session->doneSent.compare_exchange_strong(
                expected, true, std::memory_order_acq_rel)) {
          if (auto* traceSpan = session->span.get()) traceSpan->addEvent("done_called");
          session->done.Send();
        }
      }};
      try {
        this->handler_.consumeMessage(session->context, this->streamContext_,
                                      session->state, payload.get(), sender,
                                      result);
        if (auto* traceSpan = session->span.get()) traceSpan->addEvent("consume_message");
      } catch (...) {
        this->traceError(session->span.get(), std::current_exception(),
                         "consume_message.error");
        result.done();
      }

    }
  }

  void respond(const std::shared_ptr<Session>& session, Res response) {
    std::unique_lock handlerLock(session->handlerMutex);
    if (session->responseHandled) return;
    session->terminalStarted.store(true, std::memory_order_release);
    session->responseHandled = true;
    try {
      this->handler_.handleResponse(session->context, this->streamContext_,
                                    session->state, response);
    } catch (...) {
      // Transport completion may already be waiting for this admitted callback.
      // Preserve its failure before releasing the lifetime token.
      std::lock_guard lock(session->responseErrorMutex);
      if (!session->responseError) session->responseError = std::current_exception();
      throw;
    }
    if (auto* traceSpan = session->span.get()) traceSpan->addEvent("handle_response");
  }

  static void respondWhenReady(
      ClientStreamingEndpoint* self, std::shared_ptr<SessionCell> cell,
      std::shared_ptr<Session> session, Res response,
      std::shared_ptr<detail::StreamingActivity::Token> active) {
    cell->ready.Wait();
    if (!cell->error) {
      try { self->respond(session, std::move(response)); }
      catch (...) {
        const auto error = std::current_exception();
        self->traceError(session->span.get(), error, "handle_response.error");
        self->finishAsync(session, error);
        session->rpc->cancel();
      }
    }
    static_cast<void>(active);
  }

  void finishAsync(const std::shared_ptr<Session>& session, std::exception_ptr error) {
    session->terminalStarted.store(true, std::memory_order_release);
    if (!session->lifetime.close()) return;
    servicelib::detail::CooperativeExecution::Post(executor_,
        [this, session, error] { finalize(this, session, error); });
  }

  static void finalize(
      ClientStreamingEndpoint* self, std::shared_ptr<Session> session, std::exception_ptr error) {
    session->lifetime.wait();
    {
      std::lock_guard lock(session->responseErrorMutex);
      if (session->responseError) error = session->responseError;
    }
    if (error) self->traceError(session->span.get(), error);
    self->callEnd(session->context, error, session->state);
    self->metrics_.requestEnd(session->startedAt, error);
    if (session->span) tracing::SpanEnd(session->span.get());
    static_cast<void>(self->pending_.pop(session->streamId));
    session->operation.reset();
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

    try {
      session->done.Wait();
      if (auto* traceSpan = session->span.get()) traceSpan->addEvent("done_received");
      session->terminalStarted.store(true, std::memory_order_release);
      session->lifetime.close();
      session->lifetime.wait();
      std::optional<Res> response;
      try {
        response.emplace(session->rpc.Finish());
        if (auto* traceSpan = session->span.get()) traceSpan->addEvent("close_and_recv");
      } catch (...) {
        this->traceError(session->span.get(), std::current_exception(),
                         "close_and_recv.error");
        throw;
      }
      try {
        this->handler_.handleResponse(session->context, this->streamContext_,
                                      session->state, *response);
        if (auto* traceSpan = session->span.get()) traceSpan->addEvent("handle_response");
      } catch (...) {
        this->traceError(session->span.get(), std::current_exception(),
                         "handle_response.error");
        throw;
      }
    } catch (...) {
      error = std::current_exception();
      this->traceError(session->span.get(), error);
      session->terminalStarted.store(true, std::memory_order_release);
      session->lifetime.close();
      session->lifetime.wait();
    }
    this->callEnd(session->context, error, session->state);
    this->metrics_.requestEnd(session->startedAt, error);
    static_cast<void>(pending_.pop(streamId));
  }

  ClientFunction client_;
  store::RotatingMap<std::string, std::shared_ptr<SessionCell>> pending_;
  // Legacy synchronous RPC work is joined explicitly by stop().
  servicelib::detail::TaskStorage tasks_;
  detail::StreamingRegistry<SessionCell> sessions_;
  detail::StreamingActivity operations_;
  boost::asio::any_io_executor executor_;
};

}  // namespace servicelib::datasink::grpc

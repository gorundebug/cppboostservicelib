#pragma once

#include <functional>
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
  };

  using SessionCell = detail::StreamingCell<Session>;

  ClientStreamingEndpoint(SinkEndpointStream<T, R, E>& stream,
                          Handler handler, ClientFunction client)
      : Endpoint<T, R, Handler, E>(stream,
                                   api::GrpcMethodType::kClientStreaming,
                                   std::move(handler)),
        client_(std::move(client)),
        pending_(std::chrono::seconds{30}),
        executor_(servicelib::detail::ParallelExecutorRegistry::Get()),
        continuationExecutor_([this] {
          if constexpr (requires { typename ClientFunction::AsyncSession; })
            return executor_;
          else
            return servicelib::detail::BlockingExecutorRegistry::Get();
        }()) {}

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
  void consumeAsync(MessageContext context, Payload<T> payload,
                    std::shared_ptr<detail::StreamingActivity::Token> operation) {
    context = this->ensureStreamId(std::move(context));
    const std::string streamId{context.streamId()};
    auto [cell, loaded] = pending_.getOrCreate(
        streamId, [] { return std::make_shared<SessionCell>(); });
    std::shared_ptr<Session> session;
    if (!loaded) {
      sessions_.add(cell);
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
      session = std::make_shared<Session>(
          context, std::move(begin->state), Rpc{}, startedAt, trace.span);
      session->streamId = streamId;
      session->operation = operation;
      auto initializing = session->lifetime.acquire();
      try {
        std::weak_ptr<Session> weakSession = session;
        session->rpc = client_.start(
            callOptions(requestContext),
            [this, weakSession, weakCell = std::weak_ptr<SessionCell>{cell}](Res response) {
              const auto current = weakSession.lock();
              if (!current) return;
              auto active = current->lifetime.acquire();
              if (!active) return;
              auto cell = weakCell.lock();
              if (!cell) return;
              if (cell->ready.IsReady()) {
                respond(current, std::move(response));
              } else {
                boost::asio::co_spawn(executor_, respondWhenReady(
                    this, std::move(cell), current, std::move(response),
                    std::move(active)), boost::asio::detached);
              }
            },
            [this, weakSession](std::exception_ptr error) noexcept {
              const auto current = weakSession.lock();
              if (!current) return;
              finishAsync(current, error);
            });
        tracing::SpanEvent(session->span.get(), "grpc_call");
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
                     MessageContext context, Payload<T> payload,
                     std::shared_ptr<detail::StreamingActivity::Token> operation) {
    if (cell->ready.IsReady()) {
      if (!cell->error && !context.cancelled()) deliver(cell, std::move(payload));
      return;
    }
    // Each message resumes in its own task; no strand serializes handlers.
    boost::asio::co_spawn(executor_, consumeWhenReady(
        this, cell, std::move(context), std::move(payload), std::move(operation)),
        boost::asio::detached);
  }

  static boost::asio::awaitable<void> consumeWhenReady(
      ClientStreamingEndpoint* self, std::shared_ptr<SessionCell> cell,
      MessageContext context, Payload<T> payload,
      std::shared_ptr<detail::StreamingActivity::Token> operation) {
    co_await cell->ready.AsyncWait(context);
    if (!cell->ready.IsReady() || cell->error || context.cancelled()) co_return;
    if constexpr (requires { typename ClientFunction::AsyncSession; }) {
      self->deliver(cell, std::move(payload));
    } else {
      // A legacy RPC's Write/Finish may block. Resume it on the existing
      // blocking executor, never on the reactor that awaited readiness.
      boost::asio::post(self->continuationExecutor_,
          [self, cell = std::move(cell), payload = std::move(payload),
           operation = std::move(operation)]() mutable {
            self->deliver(cell, std::move(payload));
          });
    }
  }

  void deliver(const std::shared_ptr<SessionCell>& cell, Payload<T> payload) {
    const auto& session = cell->session;
    auto active = session->lifetime.acquire();
    if (!active) return;
    const auto& streamId = session->streamId;
    if constexpr (requires { typename ClientFunction::AsyncSession; }) {
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
              tracing::SpanEvent(session->span.get(), "send");
            } catch (...) {
              const auto message = tracing::ExceptionMessage(std::current_exception());
              tracing::SpanError(session->span.get(), message);
              tracing::SpanEvent(session->span.get(), "send.error",
                                 {tracing::Attribute::String("error", message)});
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
              tracing::SpanEvent(session->span.get(), "send");
            } catch (...) {
              const auto message = tracing::ExceptionMessage(std::current_exception());
              tracing::SpanError(session->span.get(), message);
              tracing::SpanEvent(session->span.get(), "send.error",
                                 {tracing::Attribute::String("error", message)});
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

  void respond(const std::shared_ptr<Session>& session, Res response) {
    this->handler_.handleResponse(session->context, this->streamContext_,
                                  session->state, response);
    tracing::SpanEvent(session->span.get(), "handle_response");
  }

  static boost::asio::awaitable<void> respondWhenReady(
      ClientStreamingEndpoint* self, std::shared_ptr<SessionCell> cell,
      std::shared_ptr<Session> session, Res response,
      std::shared_ptr<detail::StreamingActivity::Token> active) {
    co_await cell->ready.AsyncWait();
    if (!cell->error) {
      try { self->respond(session, std::move(response)); }
      catch (...) {
        const auto error = std::current_exception();
        self->traceError(session->span.get(), error, "handle_response.error");
        self->finishAsync(session, error);
        session->rpc->cancel();
      }
    }
  }

  void finishAsync(const std::shared_ptr<Session>& session, std::exception_ptr error) {
    if (!session->lifetime.close()) return;
    boost::asio::co_spawn(executor_, finalize(this, session, error), boost::asio::detached);
  }

  static boost::asio::awaitable<void> finalize(
      ClientStreamingEndpoint* self, std::shared_ptr<Session> session, std::exception_ptr error) {
    co_await session->lifetime.asyncWait();
    static_cast<void>(self->pending_.pop(session->streamId));
    if (error) self->traceError(session->span.get(), error);
    self->callEnd(session->context, error, session->state);
    self->metrics_.requestEnd(session->startedAt, error);
    tracing::SpanEnd(session->span.get());
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
      tracing::SpanEvent(session->span.get(), "done_received");
      session->lifetime.close();
      session->lifetime.wait();
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
      session->lifetime.close();
      session->lifetime.wait();
      static_cast<void>(pending_.pop(streamId));
    }
    this->callEnd(session->context, error, session->state);
    this->metrics_.requestEnd(session->startedAt, error);
  }

  ClientFunction client_;
  store::RotatingMap<std::string, std::shared_ptr<SessionCell>> pending_;
  // Legacy synchronous RPC work is joined explicitly by stop().
  servicelib::detail::TaskStorage tasks_;
  detail::StreamingRegistry<SessionCell> sessions_;
  detail::StreamingActivity operations_;
  boost::asio::any_io_executor executor_;
  boost::asio::any_io_executor continuationExecutor_;
};

}  // namespace servicelib::datasink::grpc

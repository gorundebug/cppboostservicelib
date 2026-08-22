#pragma once

#include <utility>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/ip/address.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>

#include <servicelib/datasource/http/router.hpp>
#include <servicelib/runtime/common.hpp>
#include <servicelib/runtime/config/dataconnector_types.hpp>
#include <servicelib/runtime/config/endpoint_types.hpp>
#include <servicelib/runtime/datasource.hpp>
#include <servicelib/runtime/detail/sync.hpp>
#include <servicelib/runtime/environment/environment.hpp>
#include <servicelib/runtime/store/rotatingmap.hpp>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace servicelib::http {

class Server final {
 public:
  struct Options final {
    std::string address{"0.0.0.0"};
    std::uint16_t port{};
    std::size_t bodyLimit{1024 * 1024};
    std::chrono::seconds idleTimeout{30};
    std::chrono::milliseconds shutdownTimeout{};
  };

  Server(boost::asio::any_io_executor executor, std::shared_ptr<Router> router)
      : Server(std::move(executor), std::move(router), Options{}) {}

  Server(boost::asio::any_io_executor executor, std::shared_ptr<Router> router,
         Options options)
      : executor_(std::move(executor)),
        router_(std::move(router)),
        options_(Validate(std::move(options))),
        acceptor_(std::make_shared<boost::asio::ip::tcp::acceptor>(executor_)),
        running_(std::make_shared<std::atomic<bool>>()),
        acceptedConnections_(
            std::make_shared<std::atomic<std::uint64_t>>()) {
    if (!router_) throw std::invalid_argument("HTTP router is required");
  }

  Server(const Server&) = delete;
  Server& operator=(const Server&) = delete;
  ~Server() { Stop(); }

  void Start() {
    bool expected{};
    if (!running_->compare_exchange_strong(expected, true))
      throw std::logic_error("HTTP server is already running");
    try {
      router_->Freeze();
      const auto endpoint = boost::asio::ip::tcp::endpoint(
          boost::asio::ip::make_address(options_.address), options_.port);
      acceptor_->open(endpoint.protocol());
      acceptor_->set_option(boost::asio::socket_base::reuse_address(true));
      acceptor_->bind(endpoint);
      acceptor_->listen(boost::asio::socket_base::max_listen_connections);
      boundPort_.store(acceptor_->local_endpoint().port(),
                       std::memory_order_release);
      boost::asio::co_spawn(
          executor_,
          AcceptLoop(acceptor_, running_, acceptedConnections_, router_,
                     options_, sessionRegistry_),
          boost::asio::detached);
    } catch (...) {
      running_->store(false, std::memory_order_release);
      boost::system::error_code ignored;
      acceptor_->close(ignored);
      throw;
    }
  }

  void Stop() noexcept {
    if (!running_->exchange(false, std::memory_order_acq_rel)) return;
    boost::system::error_code ignored;
    acceptor_->cancel(ignored);
    acceptor_->close(ignored);
    std::vector<std::shared_ptr<Session>> sessions;
    {
      std::lock_guard lock(sessionRegistry_->mutex);
      sessions.reserve(sessionRegistry_->sessions.size());
      for (const auto& [_, session] : sessionRegistry_->sessions) {
        sessions.push_back(std::static_pointer_cast<Session>(session));
      }
    }
    for (const auto& session : sessions) session->BeginShutdown();
    // Do not retain Session ownership on the stopping thread while the
    // coroutine and Beast timeout handlers drain on the session executor.
    // The registry keeps every live session reachable until its completion
    // callback erases it; releasing these temporary references here also
    // ensures normal Session destruction stays serialized with those handlers.
    sessions.clear();
    std::unique_lock lock(sessionRegistry_->mutex);
    if (!sessionRegistry_->drained.wait_for(
            lock, options_.shutdownTimeout,
            [this] { return sessionRegistry_->sessions.empty(); })) {
      sessions.clear();
      sessions.reserve(sessionRegistry_->sessions.size());
      for (const auto& [_, session] : sessionRegistry_->sessions) {
        sessions.push_back(std::static_pointer_cast<Session>(session));
      }
      lock.unlock();
      for (const auto& session : sessions) session->Stop();
      lock.lock();
      sessions.clear();
      // The timeout only changes graceful shutdown into forced socket
      // cancellation. It must never allow a session coroutine to outlive the
      // router and the execution graph it can call. The executor is still
      // running here, so wait until every completion handler has erased its
      // session before returning to ServiceGenerated::releaseRuntime().
      sessionRegistry_->drained.wait(
          lock, [this] { return sessionRegistry_->sessions.empty(); });
    }
  }

  [[nodiscard]] bool running() const noexcept {
    return running_->load(std::memory_order_acquire);
  }
  [[nodiscard]] std::uint16_t port() const noexcept {
    return boundPort_.load(std::memory_order_acquire);
  }
  [[nodiscard]] std::uint64_t acceptedConnections() const noexcept {
    return acceptedConnections_->load(std::memory_order_relaxed);
  }

 private:
  struct SessionRegistry final {
    std::mutex mutex;
    std::condition_variable drained;
    std::unordered_map<const void*, std::shared_ptr<void>> sessions;
  };

  class Session final : public std::enable_shared_from_this<Session> {
   public:
    Session(boost::asio::ip::tcp::socket socket,
            boost::asio::any_io_executor workerExecutor,
            std::shared_ptr<Router> router, const Options& options)
        : stream_(std::move(socket)),
          workerExecutor_(std::move(workerExecutor)),
          router_(std::move(router)),
          options_(options) {
      boost::system::error_code error;
      stream_.socket().non_blocking(true, error);
      if (error) {
        throw boost::system::system_error(error,
                                          "set HTTP socket non-blocking");
      }
    }

    void Start(std::shared_ptr<SessionRegistry> registry) {
      boost::asio::co_spawn(
          stream_.get_executor(), Run(),
          [self = shared_from_this(), registry = std::move(registry)](
              std::exception_ptr) {
            {
              std::lock_guard lock(registry->mutex);
              registry->sessions.erase(self.get());
            }
            registry->drained.notify_all();
          });
    }

    void BeginShutdown() noexcept {
      auto self = shared_from_this();
      boost::asio::dispatch(stream_.get_executor(), [self = std::move(self)] {
        self->shuttingDown_ = true;
        if (!self->requestInProgress_) self->StopOnExecutor();
      });
    }

    void Stop() noexcept {
      auto self = shared_from_this();
      boost::asio::dispatch(stream_.get_executor(), [self = std::move(self)] {
        self->StopOnExecutor();
      });
    }

   private:
    void StopOnExecutor() noexcept {
      boost::system::error_code ignored;
      stream_.socket().cancel(ignored);
      stream_.socket().shutdown(boost::asio::ip::tcp::socket::shutdown_both,
                                ignored);
      stream_.socket().close(ignored);
    }

    struct RequestCancellation final {
      std::stop_source source;
      boost::asio::cancellation_signal observer;
      std::atomic<bool> disarmed{};
    };

    boost::asio::awaitable<void> Run() {
      boost::beast::flat_buffer buffer;
      for (;;) {
        boost::beast::http::request_parser<
            boost::beast::http::string_body> parser;
        parser.body_limit(options_.bodyLimit);
        stream_.expires_after(options_.idleTimeout);
        boost::system::error_code error;
        co_await boost::beast::http::async_read(
            stream_, buffer, parser,
            boost::asio::redirect_error(boost::asio::use_awaitable, error));
        if (error == boost::beast::http::error::end_of_stream) break;
        if (error == boost::beast::http::error::body_limit) {
          co_await Write(Response{413, {}, "request body is too large\n",
                                  "text/plain; charset=utf-8", false},
                         11, false);
          break;
        }
        if (error) break;

        requestInProgress_ = true;

        auto message = parser.release();
        Request request;
        request.method = std::string(message.method_string());
        request.target = std::string(message.target());
        request.path = request.target.substr(0, request.target.find('?'));
        request.body = std::move(message.body());
        request.keepAlive = message.keep_alive();
        for (const auto& field : message.base())
          request.headers[std::string(field.name_string())] =
              std::string(field.value());
        const auto version = message.version();
        auto context = ContextFromHeaders(request.headers);
        std::shared_ptr<RequestCancellation> requestCancellation;
        if (router_->RequiresDisconnectObservation(request.method,
                                                   request.path)) {
          requestCancellation = std::make_shared<RequestCancellation>();
          context = std::move(context).withExternalCancellation(
              requestCancellation->source.get_token());
          ObserveDisconnect(requestCancellation);
        }

        Response response;
        try {
          response = co_await boost::asio::co_spawn(
              workerExecutor_,
              DispatchOnWorker(router_, std::move(request),
                               std::move(context)),
              boost::asio::use_awaitable);
        } catch (const std::exception& exception) {
          response = {500, {}, exception.what(),
                      "text/plain; charset=utf-8", false};
        } catch (...) {
          response = {500, {}, "internal server error\n",
                      "text/plain; charset=utf-8", false};
        }
        if (requestCancellation) {
          requestCancellation->disarmed.store(true, std::memory_order_release);
          requestCancellation->observer.emit(
              boost::asio::cancellation_type::all);
        }
        const bool keepAlive = response.keepAlive && message.keep_alive();
        if (!(co_await Write(std::move(response), version, keepAlive))) break;
        requestInProgress_ = false;
        if (shuttingDown_) break;
        if (!keepAlive) break;
      }
      requestInProgress_ = false;
      boost::system::error_code ignored;
      stream_.socket().shutdown(boost::asio::ip::tcp::socket::shutdown_send,
                                ignored);
    }

    void ObserveDisconnect(
        const std::shared_ptr<RequestCancellation>& cancellation) {
      stream_.socket().async_wait(
          boost::asio::ip::tcp::socket::wait_read,
          boost::asio::bind_cancellation_slot(
              cancellation->observer.slot(),
              [self = shared_from_this(), cancellation](
                  const boost::system::error_code& error) {
                self->CheckDisconnect(cancellation, error);
              }));
    }

    void CheckDisconnect(
        const std::shared_ptr<RequestCancellation>& cancellation,
        const boost::system::error_code& observerError) noexcept {
      if (cancellation->disarmed.load(std::memory_order_acquire)) {
        return;
      }
      if (observerError) {
        // While the handler is still active, cancellation of the socket wait
        // means the session was closed by shutdown or by the peer. The normal
        // response path sets disarmed before cancelling only this observer.
        cancellation->source.request_stop();
        return;
      }

      // Peek without consuming bytes. The wait is active only while the
      // handler owns the request, before the next Beast read starts. A
      // successful zero-byte receive is EOF; reset/closed errors are also a
      // disconnect. Readable application data is a pipelined next request and
      // proves that the peer is still present, so it must not be consumed or
      // repeatedly re-armed while that data remains readable.
      std::byte byte{};
      boost::system::error_code receiveError;
      const auto received = stream_.socket().receive(
          boost::asio::buffer(&byte, sizeof(byte)),
          boost::asio::socket_base::message_peek, receiveError);
      const bool wouldBlock =
          receiveError == boost::asio::error::would_block ||
          receiveError == boost::asio::error::try_again;
      if ((!receiveError && received == 0) ||
          (receiveError && !wouldBlock)) {
        cancellation->source.request_stop();
        return;
      }
      if (wouldBlock) ObserveDisconnect(cancellation);
    }

    boost::asio::awaitable<bool> Write(Response response, unsigned version,
                                       bool keepAlive) {
      boost::beast::http::response<boost::beast::http::string_body> message{
          static_cast<boost::beast::http::status>(response.status), version};
      message.set(boost::beast::http::field::server, "cppboostservicelib");
      message.set(boost::beast::http::field::content_type, response.contentType);
      for (const auto& [name, value] : response.headers) message.set(name, value);
      message.keep_alive(keepAlive);
      message.body() = std::move(response.body);
      message.prepare_payload();
      stream_.expires_after(options_.idleTimeout);
      boost::system::error_code error;
      co_await boost::beast::http::async_write(
          stream_, message,
          boost::asio::redirect_error(boost::asio::use_awaitable, error));
      co_return !error;
    }

    static boost::asio::awaitable<Response> DispatchOnWorker(
        std::shared_ptr<Router> router, Request request,
        MessageContext context) {
      // co_spawn may enter inline when the worker io_context is already
      // running on this thread. The explicit post ends the socket strand
      // handler before the business graph starts on the shared worker queue.
      co_await boost::asio::post(boost::asio::use_awaitable);
      co_return co_await router->Dispatch(std::move(request),
                                         std::move(context));
    }

    boost::beast::tcp_stream stream_;
    boost::asio::any_io_executor workerExecutor_;
    std::shared_ptr<Router> router_;
    Options options_;
    bool requestInProgress_{};
    bool shuttingDown_{};
  };

  static Options Validate(Options options) {
    if (options.address.empty())
      throw std::invalid_argument("HTTP listen address is required");
    if (options.bodyLimit == 0)
      throw std::invalid_argument("HTTP body limit must be greater than zero");
    if (options.idleTimeout <= std::chrono::seconds::zero())
      throw std::invalid_argument("HTTP idle timeout must be positive");
    if (options.shutdownTimeout < std::chrono::milliseconds::zero())
      throw std::invalid_argument("HTTP shutdown timeout must not be negative");
    return options;
  }

  static boost::asio::awaitable<void> AcceptLoop(
      std::shared_ptr<boost::asio::ip::tcp::acceptor> acceptor,
      std::shared_ptr<std::atomic<bool>> running,
      std::shared_ptr<std::atomic<std::uint64_t>> acceptedConnections,
      std::shared_ptr<Router> router, Options options,
      std::shared_ptr<SessionRegistry> sessionRegistry) {
    while (running->load(std::memory_order_acquire)) {
      boost::system::error_code error;
      boost::asio::ip::tcp::socket socket(
          boost::asio::make_strand(acceptor->get_executor()));
      co_await acceptor->async_accept(
          socket,
          boost::asio::redirect_error(boost::asio::use_awaitable, error));
      if (error) {
        if (!running->load(std::memory_order_acquire) ||
            error == boost::asio::error::operation_aborted)
          break;
        continue;
      }
      if (!running->load(std::memory_order_acquire)) {
        boost::system::error_code ignored;
        socket.close(ignored);
        break;
      }
      auto session = std::make_shared<Session>(
          std::move(socket), acceptor->get_executor(), router, options);
      acceptedConnections->fetch_add(1, std::memory_order_relaxed);
      {
        std::lock_guard lock(sessionRegistry->mutex);
        sessionRegistry->sessions.emplace(session.get(), session);
      }
      session->Start(sessionRegistry);
    }
  }

  boost::asio::any_io_executor executor_;
  std::shared_ptr<Router> router_;
  Options options_;
  std::shared_ptr<boost::asio::ip::tcp::acceptor> acceptor_;
  std::shared_ptr<std::atomic<bool>> running_;
  std::atomic<std::uint16_t> boundPort_{};
  std::shared_ptr<std::atomic<std::uint64_t>> acceptedConnections_;
  std::shared_ptr<SessionRegistry> sessionRegistry_{
      std::make_shared<SessionRegistry>()};
};

}  // namespace servicelib::http

namespace servicelib {
template <typename T, typename R, typename E, typename Context>
class InputStream;
}

namespace servicelib::datasource::http {

inline constexpr auto kPendingRotationInterval = std::chrono::seconds{30};

class HttpRequestCancelledError final : public std::runtime_error {
 public:
  HttpRequestCancelledError() : std::runtime_error("HTTP request cancelled") {}
};

struct HandlerData final {
  const servicelib::http::Request& request;
  servicelib::http::Response& response;
  std::string responseBody;

  void setResponseBody(std::string body) { responseBody = std::move(body); }
};

template <typename HandlerState, typename ReqT, typename ResR, typename T,
          typename R, typename E>
struct PendingResult final {
  using StreamContext = servicelib::SourceStreamContext<T, R, E>;
  using Callback = std::function<bool(MessageContext, StreamContext&,
                                      HandlerState&, const R&, HandlerData&)>;

  PendingResult(HandlerState stateValue, HandlerData& handlerData,
                std::shared_ptr<tracing::Span> requestSpan)
      : state(std::move(stateValue)),
        data(handlerData),
        span(std::move(requestSpan)) {}

  HandlerState state;
  HandlerData& data;
  std::shared_ptr<tracing::Span> span;
  servicelib::detail::SingleUseEvent done;
  std::atomic<bool> doneSent{false};
  std::shared_mutex lifetimeMutex;
  std::mutex callbacksMutex;
  std::unordered_map<std::string, Callback> callbacks;
};

template <typename HandlerState, typename ReqT, typename ResR, typename T,
          typename R, typename E>
class ResultContext final {
 public:
  using Pending = PendingResult<HandlerState, ReqT, ResR, T, R, E>;
  using Callback = typename Pending::Callback;

  explicit ResultContext(std::shared_ptr<Pending> result)
      : result_(std::move(result)) {}

  void setResultCallback(std::string messageId, Callback callback) {
    std::lock_guard lock(result_->callbacksMutex);
    result_->callbacks[std::move(messageId)] = std::move(callback);
  }

  void done() noexcept {
    tracing::SpanEvent(result_->span.get(), "done_called");
    bool expected = false;
    if (result_->doneSent.compare_exchange_strong(expected, true,
                                                  std::memory_order_acq_rel)) {
      result_->done.Send();
    }
  }

 private:
  std::shared_ptr<Pending> result_;
};

class IBeastEndpoint {
 public:
  virtual ~IBeastEndpoint() = default;
  [[nodiscard]] virtual int id() const noexcept = 0;
  virtual void start(Context context) = 0;
  virtual void stop(Context context) = 0;
  [[nodiscard]] virtual config::HttpEndpointConfig endpointConfig() const = 0;
  virtual boost::asio::awaitable<servicelib::http::Response> handle(
      servicelib::http::Request request, MessageContext context) = 0;
};

template <typename T, typename R, typename Handler,
          typename E = std::exception_ptr>
class BeastEndpoint final : public IBeastEndpoint {
 public:
  using State = typename Handler::State;
  using Request = typename Handler::Request;
  using Response = typename Handler::Response;
  using StreamContext = servicelib::SourceStreamContext<T, R, E>;
  using Result = PendingResult<State, Request, Response, T, R, E>;
  using HandlerResultContext = ResultContext<State, Request, Response, T, R, E>;
  using Output = typename StreamContext::Output;
  using ErrorOutput = typename StreamContext::ErrorOutput;

  BeastEndpoint(IServiceEnvironment& environment, int endpointId,
                Handler handler, Output output, bool hasResult,
                ErrorOutput errorOutput = {})
      : BeastEndpoint(environment, endpointId, 0, std::move(handler),
                      std::move(output), hasResult, std::move(errorOutput)) {}

  BeastEndpoint(IServiceEnvironment& environment, int endpointId,
                int streamConfigId, Handler handler, Output output,
                bool hasResult, ErrorOutput errorOutput = {})
      : environment_(environment),
        endpointId_(endpointId),
        streamName_(resolveStreamName(environment, streamConfigId)),
        endpointName_(endpointConfig(environment, endpointId).name),
        method_(endpointConfig(environment, endpointId).httpMethodType),
        path_(endpointConfig(environment, endpointId).path),
        handler_(std::move(handler)),
        streamContext_(std::move(output), std::move(errorOutput)),
        hasResult_(hasResult),
        pending_(kPendingRotationInterval),
        metrics_(environment.getMetrics(), environment.getLogger(),
                 connectorConfig(environment, endpointId).name,
                 endpointName_) {
    if (method_ != api::HTTPMethodType::kGET &&
        method_ != api::HTTPMethodType::kPOST) {
      throw std::invalid_argument(
          "HTTP datasource endpoint method is undefined");
    }
    if (path_.empty()) {
      throw std::invalid_argument("HTTP datasource endpoint path is empty");
    }
  }

  [[nodiscard]] int id() const noexcept override { return endpointId_; }
  void start(Context context) override {
    accepting_.store(true, std::memory_order_release);
    if (hasResult_) pending_.start(std::move(context));
  }
  void stop(Context context) override {
    accepting_.store(false, std::memory_order_release);
    std::vector<std::shared_ptr<std::stop_source>> cancellations;
    {
      std::lock_guard lock(activeMutex_);
      for (const auto& [_, cancellation] : active_) {
        cancellations.push_back(cancellation);
      }
    }
    for (const auto& cancellation : cancellations) {
      cancellation->request_stop();
    }
    if (hasResult_) pending_.stop(std::move(context));
  }

  [[nodiscard]] config::HttpEndpointConfig endpointConfig() const override {
    return endpointConfig(environment_, endpointId_);
  }
  [[nodiscard]] config::HttpDataConnectorConfig connectorConfig() const {
    return connectorConfig(environment_, endpointId_);
  }

  boost::asio::awaitable<servicelib::http::Response> handle(
      servicelib::http::Request request, MessageContext requestContext) override {
    servicelib::http::Response httpResponse;
    httpResponse.keepAlive = request.keepAlive;
    auto admission = admit();
    if (!admission) {
      httpResponse.status = 503;
      co_return httpResponse;
    }
    if (!methodMatches(request.method)) {
      metrics_.invalidHttpMethod(request.method, request.path);
      httpResponse.status = 405;
      co_return httpResponse;
    }

    auto& externalCancellation = *admission->cancellation;
    requestContext = std::move(requestContext).withExternalCancellation(
        externalCancellation.get_token());
    std::shared_ptr<tracing::Tracer> tracer;
    if (tracing::SamplingEnabled(requestContext)) {
      if (auto* tracingEngine = environment_.getTracing()) {
        tracer = tracingEngine->tracer(environment_.getServiceName());
      }
    }
    tracing::ActiveSpan startedSpan;
    if (tracer) {
      startedSpan = tracing::StartSpanInPlace(
          requestContext, tracer.get(), "http.input",
          {tracing::Attribute::String("stream", streamName_),
           tracing::Attribute::String("endpoint", endpointName_),
           tracing::Attribute::String("method", request.method),
           tracing::Attribute::String("path", path_)});
    }
    HandlerData data{request, httpResponse, {}};
    std::optional<servicelib::BeginResult<State>> beginResult;
    try {
      beginResult.emplace(
          handler_.beginRequest(requestContext, streamContext_, data));
    } catch (...) {
      const auto error = std::current_exception();
      traceError(startedSpan.span(), error, "begin_request.error");
      metrics_.beginRequestFailed(tracing::ExceptionMessage(error));
      httpResponse.body = std::move(data.responseBody);
      co_return httpResponse;
    }
    tracing::SpanEvent(startedSpan.span(), "begin_request");
    auto begin = std::move(*beginResult);
    auto context = std::move(begin.context);
    if (context.streamId().empty()) {
      context = std::move(context).withStreamId(servicelib::http::NewStreamId());
    }
    const std::string streamId{context.streamId()};
    if (startedSpan.span()) {
      tracing::SpanAttrs(
          startedSpan.span(),
          {tracing::Attribute::String("stream_id", streamId),
           tracing::Attribute::Bool("has_result", hasResult_)});
    }
    auto result = std::make_shared<Result>(std::move(begin.state), data,
                                           startedSpan.sharedSpan());
    const auto startedAt = metrics_.requestStart();
    std::exception_ptr error;
    bool pendingInserted = false;
    bool resultWaitFailed = false;
    bool doneReceived = false;
    servicelib::detail::SingleUseEvent consumeCompleted;
    std::shared_ptr<servicelib::AsyncCompletionState> consumeCompletion;
    if (startedSpan.span()) {
      consumeCompletion = servicelib::AsyncCompletionState::make(
          [&consumeCompleted] { consumeCompleted.Send(); });
      context = std::move(context).withCompletion(consumeCompletion);
    }
    try {
      if (hasResult_) {
        pending_.set(streamId, result);
        pendingInserted = true;
        metrics_.pendingAdd(streamId);
      }
      try {
        handler_.consumeMessage(context, streamContext_, result->state, data,
                                HandlerResultContext{result});
      } catch (...) {
        const auto consumeError = std::current_exception();
        traceError(startedSpan.span(), consumeError, "consume_message.error");
        if (consumeCompletion) {
          consumeCompletion->release();
          consumeCompletion.reset();
        }
        std::rethrow_exception(consumeError);
      }
      if (consumeCompletion) {
        // FunctionCall retains this frame through every asynchronous adapter;
        // pooled callers deliberately detach from it. Thus this event denotes
        // the same logical ConsumeMessage boundary as in Go for both profiles.
        consumeCompletion->release();
        consumeCompletion.reset();
        co_await consumeCompleted.AsyncWait();
      }
      tracing::SpanEvent(startedSpan.span(), "consume_message");
      if (hasResult_) {
        try {
          co_await result->done.AsyncWait(context);
          if (context.cancelled() && !result->done.IsReady()) {
            throw HttpRequestCancelledError{};
          }
          doneReceived = true;
        } catch (...) {
          resultWaitFailed = true;
          throw;
        }
      }
    } catch (...) {
      if (consumeCompletion) {
        consumeCompletion->release();
        consumeCompletion.reset();
      }
      error = std::current_exception();
      if (context.cancelled()) {
        externalCancellation.request_stop();
      }
      if (!resultWaitFailed) {
        tracing::SpanError(startedSpan.span(),
                           tracing::ExceptionMessage(error));
      }
    }

    if (hasResult_) {
      std::unique_lock lifetimeLock(result->lifetimeMutex);
      if (pendingInserted) {
        static_cast<void>(pending_.pop(streamId));
        metrics_.pendingRemove(streamId);
      }
      if (resultWaitFailed && result->doneSent.load(std::memory_order_acquire)) {
        error = nullptr;
        doneReceived = true;
      } else if (resultWaitFailed) {
        const auto message = tracing::ExceptionMessage(error);
        tracing::SpanError(startedSpan.span(), message);
        tracing::SpanEvent(
            startedSpan.span(), "context_cancelled",
            {tracing::Attribute::String("error", message)});
      }
      if (doneReceived) {
        tracing::SpanEvent(startedSpan.span(), "done_received");
      }
      if (!result->done.IsReady() && !error) {
        error = std::make_exception_ptr(HttpRequestCancelledError{});
      }
      // Canonical handlers commonly capture ResultContext in their callbacks.
      // Once the request is retired those callbacks are no longer reachable by
      // correlation, so clear them while the lifetime lock excludes an
      // in-flight consumeResult. This breaks PendingResult -> callback ->
      // ResultContext -> PendingResult ownership cycles on every exit path.
      {
        std::lock_guard callbacksLock(result->callbacksMutex);
        result->callbacks.clear();
      }
      callEndRequest(context, error, *result, data);
    } else {
      callEndRequest(context, error, *result, data);
    }
    externalCancellation.request_stop();
    metrics_.requestEnd(startedAt, error);
    httpResponse.body = std::move(data.responseBody);
    co_return httpResponse;
  }

  void consumeResult(MessageContext context, Payload<R> payload) {
    if (context.streamId().empty()) {
      metrics_.missingStreamId();
      return;
    }
    const std::string streamId{context.streamId()};
    const auto found = pending_.get(streamId);
    if (!found) {
      metrics_.lateResult(streamId);
      return;
    }
    const auto result = *found;
    std::shared_lock lifetimeLock(result->lifetimeMutex);
    const auto current = pending_.get(streamId);
    if (!current || *current != result) {
      metrics_.lateResult(streamId);
      tracing::SpanEvent(result->span.get(), "late_result");
      return;
    }
    const std::string messageId = handler_.getMessageId(
        context, streamContext_, result->state, payload.get());
    typename Result::Callback callback;
    {
      std::lock_guard callbacksLock(result->callbacksMutex);
      const auto it = result->callbacks.find(messageId);
      if (it != result->callbacks.end()) callback = it->second;
    }
    if (!callback) {
      metrics_.unknownMessageId(streamId, messageId);
      tracing::SpanEvent(result->span.get(), "unknown_message_id",
                         {tracing::Attribute::String("message_id", messageId)});
      return;
    }
    if (callback(context, streamContext_, result->state, payload.get(),
                 result->data)) {
      bool duplicate = false;
      {
        std::lock_guard callbacksLock(result->callbacksMutex);
        duplicate = result->callbacks.erase(messageId) == 0;
      }
      if (duplicate) {
        metrics_.duplicateMessageId(streamId, messageId);
        tracing::SpanEvent(
            result->span.get(), "duplicate_message_id",
            {tracing::Attribute::String("message_id", messageId)});
      }
    }
    tracing::SpanEvent(result->span.get(), "result_consumed",
                       {tracing::Attribute::String("message_id", messageId)});
  }

 private:
  struct Admission final {
    Admission(BeastEndpoint* endpoint,
              std::shared_ptr<std::stop_source> stopSource)
        : owner(endpoint), cancellation(std::move(stopSource)) {}
    Admission(const Admission&) = delete;
    Admission& operator=(const Admission&) = delete;
    Admission(Admission&& other) noexcept
        : owner(std::exchange(other.owner, nullptr)),
          cancellation(std::move(other.cancellation)) {}
    Admission& operator=(Admission&&) = delete;
    BeastEndpoint* owner;
    std::shared_ptr<std::stop_source> cancellation;
    ~Admission() {
      if (owner) owner->release(cancellation.get());
    }
  };

  [[nodiscard]] std::optional<Admission> admit() {
    if (!accepting_.load(std::memory_order_acquire)) return std::nullopt;
    auto cancellation = std::make_shared<std::stop_source>();
    std::lock_guard lock(activeMutex_);
    if (!accepting_.load(std::memory_order_relaxed)) return std::nullopt;
    active_.emplace(cancellation.get(), cancellation);
    return std::optional<Admission>{std::in_place, this,
                                    std::move(cancellation)};
  }

  void release(const std::stop_source* cancellation) noexcept {
    std::lock_guard lock(activeMutex_);
    active_.erase(cancellation);
  }

  bool methodMatches(std::string_view method) const noexcept {
    return (method_ == api::HTTPMethodType::kGET && method == "GET") ||
           (method_ == api::HTTPMethodType::kPOST && method == "POST");
  }
  static config::HttpEndpointConfig endpointConfig(
      const IServiceEnvironment& environment, int endpointId) {
    const auto runtime = environment.getRuntimeConfigSnapshot();
    if (!runtime) throw std::invalid_argument("runtime config is null");
    const auto endpoint =
        runtime->GetEndpointConfigByID(endpointId);
    const auto* http =
        endpoint ? endpoint->template As<config::HttpEndpointConfig>() : nullptr;
    if (!http) {
      throw std::invalid_argument("HTTP endpoint config not found for id=" +
                                  std::to_string(endpointId));
    }
    return *http;
  }
  static config::HttpDataConnectorConfig connectorConfig(
      const IServiceEnvironment& environment, int endpointId) {
    const auto runtime = environment.getRuntimeConfigSnapshot();
    const auto endpoint = endpointConfig(environment, endpointId);
    const auto connector = runtime
                               ? runtime->GetDataConnectorByID(
                                     endpoint.idDataConnector)
                               : std::nullopt;
    const auto* http = connector
                           ? connector->template As<
                                 config::HttpDataConnectorConfig>()
                           : nullptr;
    if (!http) {
      throw std::invalid_argument(
          "HTTP data connector config not found for endpoint id=" +
          std::to_string(endpointId));
    }
    return *http;
  }
  static std::string resolveStreamName(
      const IServiceEnvironment& environment, int streamConfigId) {
    const auto runtime = environment.getRuntimeConfigSnapshot();
    const auto stream = runtime && streamConfigId != 0
                            ? runtime->GetStreamConfigByID(streamConfigId)
                            : std::nullopt;
    return stream ? stream->GetName() : std::string{};
  }
  static void traceError(tracing::Span* span, std::exception_ptr error,
                         std::string_view event) {
    const auto message = tracing::ExceptionMessage(error);
    tracing::SpanError(span, message);
    tracing::SpanEvent(span, event,
                       {tracing::Attribute::String("error", message)});
  }
  void callEndRequest(MessageContext context, const std::exception_ptr& error,
                      Result& result, HandlerData& data) noexcept {
    try {
      handler_.endRequest(std::move(context), streamContext_, error,
                          result.state, data);
    } catch (...) {
    }
  }

  IServiceEnvironment& environment_;
  int endpointId_;
  std::string streamName_;
  std::string endpointName_;
  api::HTTPMethodType method_;
  std::string path_;
  Handler handler_;
  StreamContext streamContext_;
  bool hasResult_;
  store::RotatingMap<std::string, std::shared_ptr<Result>> pending_;
  servicelib::DataSourceEndpointMetrics metrics_;
  std::atomic<bool> accepting_{true};
  std::mutex activeMutex_;
  std::unordered_map<const std::stop_source*,
                     std::shared_ptr<std::stop_source>> active_;
};

template <typename T, typename R, typename E, typename Context,
          typename Handler>
class BeastEndpointConsumer final {
 public:
  using Input = servicelib::InputStream<T, R, E, Context>;
  using Endpoint = BeastEndpoint<T, R, Handler, E>;

  static std::shared_ptr<BeastEndpointConsumer> make(
      IServiceEnvironment& environment, Input& input,
      Handler handler) {
    auto consumer = std::shared_ptr<BeastEndpointConsumer>(
        new BeastEndpointConsumer(environment, input,
                                  std::move(handler)));
    if (consumer->input_.getResultStream() != nullptr) consumer->bindResult();
    return consumer;
  }
  void consume(MessageContext context, Payload<T> payload) {
    input_.consume(std::move(context), std::move(payload));
  }
  [[nodiscard]] Input& stream() noexcept { return input_; }
  [[nodiscard]] const Input& stream() const noexcept { return input_; }
  [[nodiscard]] const std::shared_ptr<Endpoint>& endpoint() const noexcept {
    return endpoint_;
  }

 private:
  BeastEndpointConsumer(IServiceEnvironment& environment,
                        Input& input, Handler handler)
      : input_(input),
        endpoint_(std::make_shared<Endpoint>(
            environment, input_.getEndpointId(),
            static_cast<int>(input_.getConfigId()), std::move(handler),
            [input = &input_](MessageContext context, Payload<T> payload) {
              input->consume(std::move(context), std::move(payload));
            },
            input_.getResultStream() != nullptr,
            [input = &input_](MessageContext context, Payload<E> error) {
              input->consumeError(std::move(context), std::move(error));
            })) {}
  void bindResult() {
    auto* endpointObserver = endpoint_.get();
    input_.setResultConsumer([endpointObserver](
                                  MessageContext context, Payload<R> result) {
      endpointObserver->consumeResult(std::move(context), std::move(result));
    });
  }
  Input& input_;
  std::shared_ptr<Endpoint> endpoint_;
};

class BeastDataSource final {
 public:
  template <typename Input>
  [[nodiscard]] static std::shared_ptr<BeastDataSource> make(
      IServiceEnvironment& environment, const Input& input) {
    return std::shared_ptr<BeastDataSource>(new BeastDataSource(
        environment, connectorIdForEndpoint(environment, input.getEndpointId())));
  }
  [[nodiscard]] int id() const noexcept { return connectorId_; }
  [[nodiscard]] config::HttpDataConnectorConfig config() const {
    const auto runtime = environment_.getRuntimeConfigSnapshot();
    const auto connector =
        runtime ? runtime->GetDataConnectorByID(connectorId_) : std::nullopt;
    const auto* http = connector
                           ? connector->template As<
                                 config::HttpDataConnectorConfig>()
                           : nullptr;
    if (!http) throw std::invalid_argument("HTTP datasource config not found");
    return *http;
  }
  void addEndpoint(std::shared_ptr<IBeastEndpoint> endpoint) {
    if (!endpoint) throw std::invalid_argument("HTTP endpoint is null");
    const auto runtime = environment_.getRuntimeConfigSnapshot();
    const auto configured =
        runtime ? runtime->GetEndpointConfigByID(endpoint->id()) : std::nullopt;
    if (!configured || configured->GetIdDataConnector() != connectorId_) {
      throw std::invalid_argument("HTTP endpoint belongs to another connector");
    }
    if (!endpoints_.emplace(endpoint->id(), std::move(endpoint)).second) {
      throw std::invalid_argument("duplicate HTTP endpoint id");
    }
  }
  [[nodiscard]] std::shared_ptr<IBeastEndpoint> endpoint(int id) const {
    const auto it = endpoints_.find(id);
    return it == endpoints_.end() ? nullptr : it->second;
  }
  void registerRoutes(servicelib::http::Router& router) {
    for (const auto& [_, endpoint] : endpoints_) {
      const auto config = endpoint->endpointConfig();
      const std::string method =
          config.httpMethodType == api::HTTPMethodType::kGET ? "GET" : "POST";
      router.Add(method, config.path,
                 [endpoint](servicelib::http::Request request,
                            MessageContext context) {
                   return endpoint->handle(std::move(request),
                                           std::move(context));
                 });
    }
  }
  void start(Context context) {
    std::vector<IBeastEndpoint*> started;
    try {
      for (const auto& [_, endpoint] : endpoints_) {
        endpoint->start(context);
        started.push_back(endpoint.get());
      }
    } catch (...) {
      for (auto it = started.rbegin(); it != started.rend(); ++it) {
        (*it)->stop(context);
      }
      throw;
    }
  }
  void stop(Context context) {
    for (const auto& [_, endpoint] : endpoints_) endpoint->stop(context);
  }

 private:
  BeastDataSource(IServiceEnvironment& environment, int connectorId)
      : environment_(environment), connectorId_(connectorId) {
    static_cast<void>(config());
  }

  [[nodiscard]] static int connectorIdForEndpoint(
      IServiceEnvironment& environment, int endpointId) {
    const auto runtime = environment.getRuntimeConfigSnapshot();
    const auto endpoint =
        runtime ? runtime->GetEndpointConfigByID(endpointId) : std::nullopt;
    if (!endpoint) {
      throw std::invalid_argument("HTTP datasource endpoint config not found");
    }
    return endpoint->GetIdDataConnector();
  }

  IServiceEnvironment& environment_;
  int connectorId_;
  std::unordered_map<int, std::shared_ptr<IBeastEndpoint>> endpoints_;
};

}  // namespace servicelib::datasource::http

#pragma once

#include <utility>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>

#include <servicelib/runtime/detail/http_types.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <future>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/experimental/concurrent_channel.hpp>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>

#include <optional>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace servicelib::http {

enum class ClientErrorCode {
  kStopped,
  kCancelled,
  kTimeout,
  kPoolTimeout,
  kResolve,
  kConnect,
  kWrite,
  kRead,
  kBodyLimit,
};

class ClientError final : public std::runtime_error {
 public:
  ClientError(ClientErrorCode code, std::string message)
      : std::runtime_error(std::move(message)), code_(code) {}
  [[nodiscard]] ClientErrorCode code() const noexcept { return code_; }
 private:
  ClientErrorCode code_;
};

class Client final {
 public:
  struct Options final {
    std::size_t connections{4};
    std::size_t responseBodyLimit{4 * 1024 * 1024};
    std::chrono::milliseconds timeout{5000};
    std::chrono::milliseconds acquirePollInterval{1};
  };

  explicit Client(boost::asio::any_io_executor executor)
      : Client(std::move(executor), Options{}) {}
  Client(boost::asio::any_io_executor executor, Options options)
      : executor_(std::move(executor)), options_(Validate(std::move(options))),
        state_(std::make_shared<PoolState>(executor_, options_)) {}
  Client(const Client&) = delete;
  Client& operator=(const Client&) = delete;
  ~Client() { Stop(); }

  boost::asio::awaitable<Response> Send(std::string host, std::string port,
                                        Request request,
                                        MessageContext context = {}) {
    Operation operation{*this};
    InjectContext(context, request.headers);
    const auto deadline = EffectiveDeadline(context);
    auto connection = co_await boost::asio::co_spawn(
        state_->strand, Acquire(state_, host, port, context, deadline),
        boost::asio::use_awaitable);

    co_await boost::asio::dispatch(connection->stream.get_executor(),
                                   boost::asio::use_awaitable);
    ConnectionCancellation cancellation{*this, connection, context};
    ThrowIfCancelled(context, deadline, "HTTP request admission");

    if (!connection->stream.socket().is_open() || connection->host != host ||
        connection->port != port) {
      Close(*connection);
      auto resolver = std::make_shared<boost::asio::ip::tcp::resolver>(
          connection->stream.get_executor());
      ResolveCancellation resolveCancellation{resolver, context};
      auto resolveTimer = std::make_shared<boost::asio::steady_timer>(
          connection->stream.get_executor(), deadline);
      resolveTimer->async_wait(
          [resolver](const boost::system::error_code& timerError) {
            if (!timerError) resolver->cancel();
          });
      boost::system::error_code error;
      const auto endpoints = co_await resolver->async_resolve(
          host, port,
          boost::asio::redirect_error(boost::asio::use_awaitable, error));
      static_cast<void>(resolveTimer->cancel());
      if (error) {
        Close(*connection);
        ThrowOperationError(context, deadline, ClientErrorCode::kResolve,
                            "HTTP DNS resolution failed", error);
      }
      ThrowIfCancelled(context, deadline, "HTTP connection");
      connection->stream.expires_at(deadline);
      co_await connection->stream.async_connect(
          endpoints,
          boost::asio::redirect_error(boost::asio::use_awaitable, error));
      if (error) {
        Close(*connection);
        ThrowOperationError(context, deadline, ClientErrorCode::kConnect,
                            "HTTP connection failed", error);
      }
      connection->host = host;
      connection->port = port;
    }

    ThrowIfCancelled(context, deadline, "HTTP request write");
    boost::beast::http::request<boost::beast::http::string_body> message{
        ParseVerb(request.method), request.target.empty() ? request.path
                                                          : request.target,
        11};
    message.set(boost::beast::http::field::host, host);
    message.set(boost::beast::http::field::user_agent, "cppboostservicelib");
    for (const auto& [name, value] : request.headers) message.set(name, value);
    message.keep_alive(request.keepAlive);
    message.body() = std::move(request.body);
    message.prepare_payload();
    boost::system::error_code error;
    connection->stream.expires_at(deadline);
    co_await boost::beast::http::async_write(
        connection->stream, message,
        boost::asio::redirect_error(boost::asio::use_awaitable, error));
    if (error) {
      Close(*connection);
      ThrowOperationError(context, deadline, ClientErrorCode::kWrite,
                          "HTTP request write failed", error);
    }

    ThrowIfCancelled(context, deadline, "HTTP response read");
    boost::beast::http::response_parser<boost::beast::http::string_body> parser;
    parser.body_limit(options_.responseBodyLimit);
    connection->stream.expires_at(deadline);
    co_await boost::beast::http::async_read(
        connection->stream, connection->buffer, parser,
        boost::asio::redirect_error(boost::asio::use_awaitable, error));
    if (error) {
      Close(*connection);
      if (error == boost::beast::http::error::body_limit)
        throw ClientError(ClientErrorCode::kBodyLimit,
                          "HTTP response body is too large");
      ThrowOperationError(context, deadline, ClientErrorCode::kRead,
                          "HTTP response read failed", error);
    }
    auto received = parser.release();
    Response response;
    response.status = static_cast<int>(received.result_int());
    response.body = std::move(received.body());
    response.keepAlive = received.keep_alive();
    for (const auto& field : received.base())
      response.headers[std::string(field.name_string())] =
          std::string(field.value());
    if (const auto contentType = response.headers.find("content-type");
        contentType != response.headers.end())
      response.contentType = contentType->second;
    if (!received.keep_alive()) Close(*connection);
    co_return response;
  }

  // Preserve the synchronous shutdown contract for existing callers.
  void Stop() noexcept {
    BeginStop(state_);
    state_->drained.wait();
  }

  [[nodiscard]] std::size_t connectionCount() const {
    return state_->connectionCount.load(std::memory_order_acquire);
  }

 private:
  static constexpr std::uint64_t kClosed = std::uint64_t{1} << 63;
  struct PoolState;
  class Operation final {
   public:
    explicit Operation(Client& owner) : state_(owner.state_) {
      auto value = state_->operations.load(std::memory_order_acquire);
      for (;;) {
        if (value & kClosed)
          throw ClientError(ClientErrorCode::kStopped, "HTTP client is stopped");
        if (state_->operations.compare_exchange_weak(value, value + 1,
                                                     std::memory_order_acq_rel)) break;
      }
    }
    Operation(const Operation&) = delete;
    Operation& operator=(const Operation&) = delete;
    ~Operation() {
      if (state_->operations.fetch_sub(1, std::memory_order_acq_rel) == kClosed + 1)
        boost::asio::post(state_->strand, [state = state_] { Finish(state); });
    }
   private:
    std::shared_ptr<PoolState> state_;
  };

  struct Connection final {
    explicit Connection(boost::asio::any_io_executor executor)
        : stream(boost::asio::make_strand(std::move(executor))) {}
    boost::beast::tcp_stream stream;
    boost::beast::flat_buffer buffer;
    std::string host;
    std::string port;
    bool busy{};
    std::atomic<std::uint64_t> cancellationGeneration{};
  };

  using ConnectionSignal = boost::asio::experimental::concurrent_channel<
      void(boost::system::error_code, std::shared_ptr<Connection>)>;
  struct PoolWaiter final {
    PoolWaiter(boost::asio::any_io_executor executor, std::string h, std::string p)
        : signal(executor, 1), host(std::move(h)), port(std::move(p)) {}
    ConnectionSignal signal;
    std::string host, port;
  };
  struct PoolState final {
    PoolState(boost::asio::any_io_executor executor, Options o)
        : strand(boost::asio::make_strand(executor)), options(o),
          drained(drainPromise.get_future().share()) {}
    boost::asio::strand<boost::asio::any_io_executor> strand;
    Options options;
    std::atomic<std::uint64_t> operations{};
    std::atomic<std::size_t> connectionCount{};
    std::vector<std::shared_ptr<Connection>> connections;
    std::deque<std::shared_ptr<PoolWaiter>> waiters;
    std::atomic<bool> finishing{}, done{};
    std::size_t closing{};
    std::promise<void> drainPromise;
    std::shared_future<void> drained;
    bool stopped() const { return operations.load(std::memory_order_acquire) & kClosed; }
  };

  static void CompleteStop(const std::shared_ptr<PoolState>& state) {
    state->done.store(true, std::memory_order_release);
    state->drainPromise.set_value();
  }
  static void Finish(const std::shared_ptr<PoolState>& state) {
    if (state->operations.load() != kClosed || state->finishing.exchange(true)) return;
    state->closing = state->connections.size();
    if (!state->closing) { CompleteStop(state); return; }
    // Closing on the socket executor also orders it after outstanding cancel
    // notifications. Pool bookkeeping never runs application callbacks.
    for (const auto& connection : state->connections) {
      boost::asio::post(connection->stream.get_executor(), [state, connection] {
        Close(*connection);
        boost::asio::post(state->strand, [state] {
          if (--state->closing == 0) CompleteStop(state);
        });
      });
    }
  }
  static void BeginStop(const std::shared_ptr<PoolState>& state) {
    const auto before = state->operations.fetch_or(kClosed, std::memory_order_acq_rel);
    if (before & kClosed) return;
    if (before == 0 && !state->finishing.exchange(true)) {
      // No operation can still access sockets, including when an external
      // caller has already stopped its io_context.
      for (const auto& connection : state->connections) Close(*connection);
      state->done.store(true, std::memory_order_release);
      state->drainPromise.set_value();
      return;
    }
    boost::asio::post(state->strand, [state] {
      for (const auto& waiter : state->waiters)
        static_cast<void>(waiter->signal.try_send(boost::system::error_code{},
                                                  std::shared_ptr<Connection>{}));
      for (const auto& connection : state->connections)
        if (connection->busy) Interrupt(connection);
      Finish(state);
    });
  }
  using StopCallback = std::stop_callback<std::function<void()>>;

  class ResolveCancellation final {
   public:
    ResolveCancellation(
        std::shared_ptr<boost::asio::ip::tcp::resolver> resolver,
        const MessageContext& context) {
      const auto cancel = [resolver] {
        boost::asio::dispatch(resolver->get_executor(),
                              [resolver] { resolver->cancel(); });
      };
      Add(context.stopToken(), cancel);
      for (const auto& token : context.externalStopTokens()) Add(token, cancel);
    }

   private:
    void Add(std::stop_token token, const std::function<void()>& cancel) {
      if (token.stop_possible()) {
        callbacks_.push_back(std::make_unique<StopCallback>(token, cancel));
      }
    }
    std::vector<std::unique_ptr<StopCallback>> callbacks_;
  };

  class ConnectionCancellation final {
   public:
    ConnectionCancellation(Client& owner,
                           std::shared_ptr<Connection> connection,
                           const MessageContext& context)
        : connection_(std::move(connection)),
          generation_(connection_->cancellationGeneration.fetch_add(
                          1, std::memory_order_acq_rel) +
                      1) {
      const auto cancel = [connection = connection_, generation = generation_] {
        boost::asio::dispatch(
            connection->stream.get_executor(), [connection, generation] {
              if (connection->cancellationGeneration.load(
                      std::memory_order_acquire) != generation) {
                return;
              }
              boost::system::error_code ignored;
              connection->stream.socket().cancel(ignored);
            });
      };
      Add(context.stopToken(), cancel);
      for (const auto& token : context.externalStopTokens()) Add(token, cancel);
      if (owner.state_->stopped()) cancel();
    }
    ConnectionCancellation(const ConnectionCancellation&) = delete;
    ConnectionCancellation& operator=(const ConnectionCancellation&) = delete;
    ~ConnectionCancellation() {
      connection_->cancellationGeneration.fetch_add(1,
                                                     std::memory_order_acq_rel);
    }

   private:
    void Add(std::stop_token token, const std::function<void()>& cancel) {
      if (token.stop_possible()) {
        callbacks_.push_back(std::make_unique<StopCallback>(token, cancel));
      }
    }
    std::shared_ptr<Connection> connection_;
    std::uint64_t generation_{};
    std::vector<std::unique_ptr<StopCallback>> callbacks_;
  };

  static Options Validate(Options options) {
    if (options.connections == 0 || options.responseBodyLimit == 0 ||
        options.timeout <= std::chrono::milliseconds::zero() ||
        options.acquirePollInterval <= std::chrono::milliseconds::zero())
      throw std::invalid_argument("invalid HTTP client pool options");
    return options;
  }

  static boost::beast::http::verb ParseVerb(const std::string& method) {
    const auto verb = boost::beast::http::string_to_verb(method);
    if (verb == boost::beast::http::verb::unknown)
      throw std::invalid_argument("unknown HTTP method: " + method);
    return verb;
  }

  std::chrono::steady_clock::time_point EffectiveDeadline(
      const MessageContext& context) const {
    auto deadline = std::chrono::steady_clock::now() + options_.timeout;
    if (context.deadline()) deadline = std::min(deadline, *context.deadline());
    return deadline;
  }

  static void Pump(const std::shared_ptr<PoolState>& state) {
    if (state->stopped()) return;
    while (!state->waiters.empty()) {
      const auto waiter = state->waiters.front();
      std::shared_ptr<Connection> available;
      for (const auto& connection : state->connections) {
        if (!connection->busy && connection->host == waiter->host &&
            connection->port == waiter->port) { available = connection; break; }
      }
      if (!available) {
        for (const auto& connection : state->connections)
          if (!connection->busy) { available = connection; break; }
      }
      if (!available && state->connections.size() < state->options.connections) {
        available = std::make_shared<Connection>(state->strand.get_inner_executor());
        state->connections.push_back(available);
        state->connectionCount.store(state->connections.size());
      }
      if (!available) return;
      state->waiters.pop_front();
      // The lease releases admission even if cancellation discards the
      // co_spawn result before Send resumes and takes ownership.
      available->busy = true;
      auto lease = std::shared_ptr<Connection>(available.get(),
          [state, available](Connection*) {
            boost::asio::post(state->strand, [state, available] {
              available->busy = false;
              Pump(state);
            });
          });
      static_cast<void>(waiter->signal.try_send(boost::system::error_code{},
                                               std::move(lease)));
    }
  }

  static boost::asio::awaitable<std::shared_ptr<Connection>> Acquire(
      std::shared_ptr<PoolState> state, std::string host, std::string port,
      MessageContext context, std::chrono::steady_clock::time_point deadline) {
    if (state->stopped())
      throw ClientError(ClientErrorCode::kStopped, "HTTP client is stopped");
    auto waiter = std::make_shared<PoolWaiter>(state->strand, std::move(host), std::move(port));
    auto wake = [waiter] {
      static_cast<void>(waiter->signal.try_send(boost::system::error_code{},
                                               std::shared_ptr<Connection>{}));
    };
    std::vector<std::unique_ptr<StopCallback>> callbacks;
    if (context.stopToken().stop_possible())
      callbacks.push_back(std::make_unique<StopCallback>(context.stopToken(), wake));
    for (const auto& token : context.externalStopTokens())
      if (token.stop_possible()) callbacks.push_back(std::make_unique<StopCallback>(token, wake));
    boost::asio::steady_timer timer(state->strand, deadline);
    timer.async_wait([wake](boost::system::error_code error) { if (!error) wake(); });
    struct RemoveWaiter final {
      std::shared_ptr<PoolState> state;
      std::shared_ptr<PoolWaiter> waiter;
      ~RemoveWaiter() {
        const auto position = std::find(state->waiters.begin(), state->waiters.end(), waiter);
        if (position != state->waiters.end()) state->waiters.erase(position);
        Pump(state);
      }
    } removeWaiter{state, waiter};
    state->waiters.push_back(waiter);
    Pump(state);
    auto connection = co_await waiter->signal.async_receive(boost::asio::use_awaitable);
    timer.cancel();
    if (connection) co_return connection;
    if (state->stopped())
      throw ClientError(ClientErrorCode::kStopped, "HTTP client is stopped");
    if (std::chrono::steady_clock::now() >= deadline)
      throw ClientError(ClientErrorCode::kPoolTimeout, "HTTP connection pool acquisition timed out");
    throw ClientError(ClientErrorCode::kCancelled, "HTTP connection pool acquisition cancelled");
  }

  [[noreturn]] void ThrowOperationError(
      const MessageContext& context,
      std::chrono::steady_clock::time_point deadline,
      ClientErrorCode fallbackCode, std::string_view operation,
      const boost::system::error_code& error) const {
    if (state_->stopped()) {
      throw ClientError(ClientErrorCode::kStopped,
                        "HTTP client stopped during " + std::string(operation));
    }
    const auto now = std::chrono::steady_clock::now();
    if (now >= deadline || error == boost::beast::error::timeout) {
      throw ClientError(ClientErrorCode::kTimeout,
                        std::string(operation) + ": timeout");
    }
    if (context.cancelled()) {
      throw ClientError(ClientErrorCode::kCancelled,
                        std::string(operation) + ": cancelled");
    }
    throw ClientError(fallbackCode,
                      std::string(operation) + ": " + error.message());
  }

  void ThrowIfCancelled(
      const MessageContext& context,
      std::chrono::steady_clock::time_point deadline,
      std::string_view operation) const {
    if (state_->stopped()) {
      throw ClientError(ClientErrorCode::kStopped,
                        "HTTP client stopped during " + std::string(operation));
    }
    const auto now = std::chrono::steady_clock::now();
    if (now >= deadline ||
        (context.deadline() && *context.deadline() <= now)) {
      throw ClientError(ClientErrorCode::kTimeout,
                        std::string(operation) + ": timeout");
    }
    if (context.cancelled()) {
      throw ClientError(ClientErrorCode::kCancelled,
                        std::string(operation) + ": cancelled");
    }
  }

  static void Close(Connection& connection) noexcept {
    boost::system::error_code ignored;
    connection.stream.socket().cancel(ignored);
    connection.stream.socket().shutdown(
        boost::asio::ip::tcp::socket::shutdown_both, ignored);
    connection.stream.socket().close(ignored);
    connection.buffer.consume(connection.buffer.size());
    connection.host.clear();
    connection.port.clear();
  }

  static void Interrupt(std::shared_ptr<Connection> connection) noexcept {
    const auto generation = connection->cancellationGeneration.load(
        std::memory_order_acquire);
    boost::asio::dispatch(
        connection->stream.get_executor(),
        [connection = std::move(connection), generation] {
          if (connection->cancellationGeneration.load(
                  std::memory_order_acquire) != generation) {
            return;
          }
          boost::system::error_code ignored;
          connection->stream.socket().cancel(ignored);
        });
  }

  boost::asio::any_io_executor executor_;
  Options options_;
  std::shared_ptr<PoolState> state_;
};

}  // namespace servicelib::http

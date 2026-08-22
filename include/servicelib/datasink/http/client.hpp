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
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
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
      : executor_(std::move(executor)), options_(Validate(std::move(options))) {}
  Client(const Client&) = delete;
  Client& operator=(const Client&) = delete;
  ~Client() { Stop(); }

  boost::asio::awaitable<Response> Send(std::string host, std::string port,
                                        Request request,
                                        MessageContext context = {}) {
    Operation operation{*this};
    InjectContext(context, request.headers);
    const auto deadline = EffectiveDeadline(context);
    auto connection = co_await Acquire(context, deadline);
    struct Release final {
      Client* owner;
      std::shared_ptr<Connection> connection;
      ~Release() { owner->ReleaseConnection(connection); }
    } release{this, connection};

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

  void Stop() noexcept {
    const bool firstStop =
        !stopped_.exchange(true, std::memory_order_acq_rel);
    std::vector<std::shared_ptr<Connection>> connections;
    if (firstStop) {
      std::lock_guard lock(mutex_);
      connections = connections_;
      for (const auto& connection : connections) {
        if (connection->busy) Interrupt(connection);
      }
    }
    {
      std::unique_lock lock(operationsMutex_);
      operationsDrained_.wait(lock, [this] { return activeOperations_ == 0; });
    }
    if (firstStop) {
      for (const auto& connection : connections) Close(*connection);
    }
  }

  [[nodiscard]] std::size_t connectionCount() const {
    std::lock_guard lock(mutex_);
    return connections_.size();
  }

 private:
  class Operation final {
   public:
    explicit Operation(Client& owner) : owner_(&owner) {
      std::lock_guard lock(owner_->operationsMutex_);
      if (owner_->stopped_.load(std::memory_order_acquire)) {
        throw ClientError(ClientErrorCode::kStopped, "HTTP client is stopped");
      }
      ++owner_->activeOperations_;
    }
    Operation(const Operation&) = delete;
    Operation& operator=(const Operation&) = delete;
    ~Operation() {
      std::lock_guard lock(owner_->operationsMutex_);
      if (--owner_->activeOperations_ == 0) {
        owner_->operationsDrained_.notify_all();
      }
    }

   private:
    Client* owner_;
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

  struct PoolWaiter final {};

  class WaiterRegistration final {
   public:
    explicit WaiterRegistration(Client& owner) : owner_(&owner) {}
    WaiterRegistration(const WaiterRegistration&) = delete;
    WaiterRegistration& operator=(const WaiterRegistration&) = delete;
    ~WaiterRegistration() {
      if (waiter_) owner_->RemoveWaiter(waiter_);
    }

    void Set(std::shared_ptr<PoolWaiter> waiter) {
      waiter_ = std::move(waiter);
    }
    void Release() noexcept { waiter_.reset(); }
    [[nodiscard]] const std::shared_ptr<PoolWaiter>& Get() const noexcept {
      return waiter_;
    }

   private:
    Client* owner_;
    std::shared_ptr<PoolWaiter> waiter_;
  };

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
      if (owner.stopped_.load(std::memory_order_acquire)) cancel();
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

  boost::asio::awaitable<std::shared_ptr<Connection>> Acquire(
      const MessageContext& context,
      std::chrono::steady_clock::time_point deadline) {
    boost::asio::steady_timer timer(executor_);
    WaiterRegistration registration{*this};
    for (;;) {
      if (stopped_.load(std::memory_order_acquire))
        throw ClientError(ClientErrorCode::kStopped, "HTTP client is stopped");
      if (context.cancelled()) {
        if (context.deadline() &&
            *context.deadline() <= std::chrono::steady_clock::now()) {
          throw ClientError(ClientErrorCode::kPoolTimeout,
                            "HTTP connection pool deadline expired");
        }
        throw ClientError(ClientErrorCode::kCancelled,
                          "HTTP connection pool acquisition cancelled");
      }
      if (std::chrono::steady_clock::now() >= deadline) {
        throw ClientError(ClientErrorCode::kPoolTimeout,
                          "HTTP connection pool acquisition timed out");
      }
      {
        std::lock_guard lock(mutex_);
        const bool mayAcquire =
            waiters_.empty() ||
            (registration.Get() && waiters_.front() == registration.Get());
        if (mayAcquire) {
          for (const auto& connection : connections_) {
            if (!connection->busy) {
              connection->busy = true;
              if (registration.Get()) {
                waiters_.pop_front();
                registration.Release();
              }
              co_return connection;
            }
          }
          if (connections_.size() < options_.connections) {
            auto connection = std::make_shared<Connection>(executor_);
            connection->busy = true;
            connections_.push_back(connection);
            if (registration.Get()) {
              waiters_.pop_front();
              registration.Release();
            }
            co_return connection;
          }
        }
        if (!registration.Get()) {
          auto waiter = std::make_shared<PoolWaiter>();
          waiters_.push_back(waiter);
          registration.Set(std::move(waiter));
        }
      }
      timer.expires_at(std::min(deadline, std::chrono::steady_clock::now() +
                                             options_.acquirePollInterval));
      co_await timer.async_wait(boost::asio::use_awaitable);
    }
  }

  void RemoveWaiter(const std::shared_ptr<PoolWaiter>& waiter) noexcept {
    std::lock_guard lock(mutex_);
    const auto position = std::find(waiters_.begin(), waiters_.end(), waiter);
    if (position != waiters_.end()) waiters_.erase(position);
  }

  [[noreturn]] void ThrowOperationError(
      const MessageContext& context,
      std::chrono::steady_clock::time_point deadline,
      ClientErrorCode fallbackCode, std::string_view operation,
      const boost::system::error_code& error) const {
    if (stopped_.load(std::memory_order_acquire)) {
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
    if (stopped_.load(std::memory_order_acquire)) {
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

  void ReleaseConnection(const std::shared_ptr<Connection>& connection) noexcept {
    std::lock_guard lock(mutex_);
    connection->busy = false;
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
  std::atomic<bool> stopped_{};
  mutable std::mutex mutex_;
  std::vector<std::shared_ptr<Connection>> connections_;
  std::deque<std::shared_ptr<PoolWaiter>> waiters_;
  std::mutex operationsMutex_;
  std::condition_variable operationsDrained_;
  std::size_t activeOperations_{};
};

}  // namespace servicelib::http

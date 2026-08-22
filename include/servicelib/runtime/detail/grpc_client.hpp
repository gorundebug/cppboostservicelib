#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <deque>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <agrpc/grpc_context.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

#include <servicelib/datasink/grpc/common.hpp>
#include <servicelib/runtime/detail/asio_dispatch.hpp>
#include <servicelib/runtime/detail/grpc_transport.hpp>
#include <servicelib/runtime/detail/sync.hpp>

namespace servicelib::grpc_transport {

class StatusError final : public std::runtime_error {
 public:
  explicit StatusError(const ::grpc::Status& status)
      : std::runtime_error(status.error_message().empty()
                               ? "gRPC call failed"
                               : status.error_message()),
        code_(status.error_code()) {}

  [[nodiscard]] ::grpc::StatusCode code() const noexcept { return code_; }

 private:
  ::grpc::StatusCode code_;
};

// Non-blocking write side of a generated client-streaming RPC.  The public
// sink endpoint keeps the canonical Sender/ResultContext contract; this small
// transport handle only replaces userver's synchronous stream object.
template <typename Request>
class AsyncWriter final {
 public:
  using Write = std::function<void(Request)>;
  using Action = std::function<void()>;

  AsyncWriter(Write write, Action done, Action cancel)
      : write_(std::move(write)),
        done_(std::move(done)),
        cancel_(std::move(cancel)) {
    if (!write_ || !done_ || !cancel_) {
      throw std::invalid_argument("gRPC async writer callback is empty");
    }
  }

  void write(Request request) { write_(std::move(request)); }
  void done() { done_(); }
  void cancel() noexcept {
    try {
      cancel_();
    } catch (...) {
    }
  }

 private:
  Write write_;
  Action done_;
  Action cancel_;
};

namespace detail {

template <typename T>
class ClientWriteQueue final {
 public:
  void push(T value) {
    std::vector<std::shared_ptr<boost::asio::steady_timer>> waiters;
    {
      std::lock_guard lock(mutex_);
      if (closed_) throw std::runtime_error("gRPC stream is already closed");
      values_.push_back(std::move(value));
      collectWaiters(waiters);
    }
    wake(std::move(waiters));
  }

  void close() noexcept {
    std::vector<std::shared_ptr<boost::asio::steady_timer>> waiters;
    {
      std::lock_guard lock(mutex_);
      if (closed_) return;
      closed_ = true;
      collectWaiters(waiters);
    }
    wake(std::move(waiters));
  }

  boost::asio::awaitable<std::optional<T>> pop() {
    const auto executor = co_await boost::asio::this_coro::executor;
    for (;;) {
      auto timer = std::make_shared<boost::asio::steady_timer>(
          executor, std::chrono::steady_clock::time_point::max());
      {
        std::lock_guard lock(mutex_);
        if (!values_.empty()) {
          auto value = std::move(values_.front());
          values_.pop_front();
          co_return value;
        }
        if (closed_) co_return std::nullopt;
        waiters_.push_back(timer);
      }
      boost::system::error_code error;
      co_await timer->async_wait(
          boost::asio::redirect_error(boost::asio::use_awaitable, error));
    }
  }

 private:
  void collectWaiters(
      std::vector<std::shared_ptr<boost::asio::steady_timer>>& result) {
    for (auto& waiter : waiters_) {
      if (auto timer = waiter.lock()) result.push_back(std::move(timer));
    }
    waiters_.clear();
  }

  static void wake(
      std::vector<std::shared_ptr<boost::asio::steady_timer>> waiters) {
    for (auto& waiter : waiters) {
      boost::asio::dispatch(waiter->get_executor(),
                            [waiter] { waiter->cancel(); });
    }
  }

  std::mutex mutex_;
  std::deque<T> values_;
  std::vector<std::weak_ptr<boost::asio::steady_timer>> waiters_;
  bool closed_{false};
};

}  // namespace detail

template <auto PrepareAsync, typename Stub, typename Request,
          typename Response>
boost::asio::awaitable<void> RunServerStreamingClient(
    agrpc::GrpcContext& context, Stub& client, MessageContext message,
    Request request, std::function<void(Response)> response) {
  using RPC = agrpc::ClientRPC<PrepareAsync>;
  RPC rpc{context};
  InjectContext(message, rpc.context());
  detail::ClientCancellation cancellation(message, rpc.context());
  if (co_await rpc.start(client, request, boost::asio::use_awaitable)) {
    typename RPC::Response value;
    while (co_await rpc.read(value, boost::asio::use_awaitable)) {
      response(std::move(value));
      value = typename RPC::Response{};
    }
  }
  auto status = co_await rpc.finish(boost::asio::use_awaitable);
  if (!status.ok()) throw StatusError(status);
}

template <auto PrepareAsync, typename Stub, typename Request,
          typename Response>
boost::asio::awaitable<void> RunClientStreamingClient(
    std::shared_ptr<agrpc::ClientRPC<PrepareAsync>> rpc,
    std::shared_ptr<detail::ClientWriteQueue<Request>> queue, Stub& client,
    MessageContext message, std::function<void(Response)> response) {
  using RPC = agrpc::ClientRPC<PrepareAsync>;
  InjectContext(message, rpc->context());
  detail::ClientCancellation cancellation(message, rpc->context());
  typename RPC::Response value;
  if (co_await rpc->start(client, value, boost::asio::use_awaitable)) {
    while (auto request = co_await queue->pop()) {
      if (!co_await rpc->write(*request, boost::asio::use_awaitable)) {
        throw std::runtime_error("gRPC stream write cancelled");
      }
    }
  }
  auto status = co_await rpc->finish(boost::asio::use_awaitable);
  if (!status.ok()) throw StatusError(status);
  response(std::move(value));
}

template <typename RPC, typename Request>
boost::asio::awaitable<void> RunBidirectionalWrites(
    std::shared_ptr<RPC> rpc,
    std::shared_ptr<detail::ClientWriteQueue<Request>> queue) {
  while (auto request = co_await queue->pop()) {
    if (!co_await rpc->write(*request, boost::asio::use_awaitable)) {
      throw std::runtime_error("gRPC stream write cancelled");
    }
  }
  if (!co_await rpc->writes_done(boost::asio::use_awaitable)) {
    throw std::runtime_error("gRPC WritesDone failed");
  }
}

template <auto PrepareAsync, typename Stub, typename Request,
          typename Response>
boost::asio::awaitable<void> RunBidirectionalStreamingClient(
    std::shared_ptr<agrpc::ClientRPC<PrepareAsync>> rpc,
    std::shared_ptr<detail::ClientWriteQueue<Request>> queue, Stub& client,
    MessageContext message, std::function<void(Response)> response) {
  try {
    using RPC = agrpc::ClientRPC<PrepareAsync>;
    InjectContext(message, rpc->context());
    detail::ClientCancellation cancellation(message, rpc->context());
    if (!co_await rpc->start(client, boost::asio::use_awaitable)) {
      throw std::runtime_error("gRPC stream start failed");
    }

    auto writesDone = std::make_shared<servicelib::detail::SingleUseEvent>();
    auto writeError = std::make_shared<std::exception_ptr>();
    auto writeMutex = std::make_shared<std::mutex>();
    boost::asio::co_spawn(
        servicelib::detail::ParallelExecutorRegistry::Get(),
        RunBidirectionalWrites<RPC, Request>(rpc, queue),
        [rpc, writesDone, writeError,
         writeMutex](std::exception_ptr error) noexcept {
          bool failed{};
          {
            std::lock_guard lock(*writeMutex);
            *writeError = std::move(error);
            failed = static_cast<bool>(*writeError);
          }
          if (failed) rpc->context().TryCancel();
          writesDone->Send();
        });

    typename RPC::Response value;
    while (co_await rpc->read(value, boost::asio::use_awaitable)) {
      response(std::move(value));
      value = typename RPC::Response{};
    }
    queue->close();
    co_await writesDone->AsyncWait();
    {
      std::lock_guard lock(*writeMutex);
      if (*writeError) std::rethrow_exception(*writeError);
    }
    auto status = co_await rpc->finish(boost::asio::use_awaitable);
    if (!status.ok()) throw StatusError(status);
  } catch (...) {
    queue->close();
    rpc->context().TryCancel();
    throw;
  }
}

// A generated connector owns one standard gRPC Stub per configured
// connection. Calls are distributed round-robin, matching the canonical
// connectionsCount contract without introducing extra worker threads.
template <typename Stub>
class ClientPool final {
 public:
  using Factory =
      std::function<std::unique_ptr<Stub>(std::shared_ptr<::grpc::Channel>)>;

  ClientPool(agrpc::GrpcContext& context, std::string address,
             std::size_t connections, Factory factory)
      : context_(context) {
    if (address.empty()) {
      throw std::invalid_argument("gRPC connector address is empty");
    }
    if (connections == 0) {
      throw std::invalid_argument(
          "gRPC connector connectionsCount must be at least 1");
    }
    if (!factory) throw std::invalid_argument("gRPC stub factory is empty");
    clients_.reserve(connections);
    for (std::size_t index = 0; index < connections; ++index) {
      auto channel = ::grpc::CreateChannel(
          address, ::grpc::InsecureChannelCredentials());
      auto client = factory(std::move(channel));
      if (!client) throw std::runtime_error("gRPC stub factory returned null");
      clients_.push_back(std::move(client));
    }
  }

  ~ClientPool() { operations_.stopAndWait(); }

  template <auto PrepareAsync, typename Request, typename Response>
  void asyncUnary(
      Request request, datasink::grpc::CallOptions options,
      std::function<void(std::exception_ptr, std::optional<Response>)>
          completion) {
    if (!completion) {
      throw std::invalid_argument("gRPC completion is empty");
    }
    auto operation = operations_.acquire();
    if (!operation) throw std::runtime_error("gRPC client pool is stopped");
    Stub* client = &next();
    using RPC = agrpc::ClientRPC<PrepareAsync>;
    struct State final {
      State(MessageContext messageValue,
            std::function<void(std::exception_ptr, std::optional<Response>)>
                completionValue,
            std::shared_ptr<servicelib::detail::AsyncOperations::Token>
                operationValue)
          : cancellation(messageValue, context),
            completion(std::move(completionValue)),
            operation(std::move(operationValue)) {
        InjectContext(messageValue, context);
      }

      ::grpc::ClientContext context;
      detail::ClientCancellation cancellation;
      Response response;
      std::function<void(std::exception_ptr, std::optional<Response>)>
          completion;
      std::shared_ptr<servicelib::detail::AsyncOperations::Token> operation;
    };
    auto state = std::make_shared<State>(
        std::move(options.context), std::move(completion),
        std::move(operation));
    try {
      RPC::request(
          context_, *client, state->context, request, state->response,
          boost::asio::bind_executor(
              servicelib::detail::ParallelExecutorRegistry::Get(),
              [state](::grpc::Status status) mutable noexcept {
                if (!status.ok()) {
                  try {
                    throw StatusError(status);
                  } catch (...) {
                    state->completion(std::current_exception(), std::nullopt);
                  }
                } else {
                  state->completion({}, std::move(state->response));
                }
              }));
    } catch (...) {
      state->completion(std::current_exception(), std::nullopt);
    }
  }

  template <auto PrepareAsync, typename Request, typename Response>
  void asyncServerStreaming(
      Request request, datasink::grpc::CallOptions options,
      std::function<void(Response)> response,
      std::function<void(std::exception_ptr)> completion) {
    if (!response || !completion) {
      throw std::invalid_argument("gRPC streaming callback is empty");
    }
    auto operation = operations_.acquire();
    if (!operation) throw std::runtime_error("gRPC client pool is stopped");
    Stub* client = &next();
    boost::asio::co_spawn(
        servicelib::detail::ParallelExecutorRegistry::Get(),
        RunServerStreamingClient<PrepareAsync, Stub, Request, Response>(
            context_, *client, std::move(options.context), std::move(request),
            std::move(response)),
        [completion = std::move(completion), operation = std::move(operation)](
            std::exception_ptr error) mutable noexcept {
          completion(std::move(error));
        });
  }

  template <auto PrepareAsync, typename Request, typename Response>
  std::shared_ptr<AsyncWriter<Request>> asyncClientStreaming(
      datasink::grpc::CallOptions options,
      std::function<void(Response)> response,
      std::function<void(std::exception_ptr)> completion) {
    if (!response || !completion) {
      throw std::invalid_argument("gRPC streaming callback is empty");
    }
    auto operation = operations_.acquire();
    if (!operation) throw std::runtime_error("gRPC client pool is stopped");
    using RPC = agrpc::ClientRPC<PrepareAsync>;
    auto rpc = std::make_shared<RPC>(context_);
    auto queue = std::make_shared<detail::ClientWriteQueue<Request>>();
    Stub* client = &next();
    auto writer = std::make_shared<AsyncWriter<Request>>(
        [queue](Request request) { queue->push(std::move(request)); },
        [queue] { queue->close(); },
        [queue, rpc] {
          queue->close();
          rpc->context().TryCancel();
        });
    boost::asio::co_spawn(
        servicelib::detail::ParallelExecutorRegistry::Get(),
        RunClientStreamingClient<PrepareAsync, Stub, Request, Response>(
            rpc, queue, *client, std::move(options.context),
            std::move(response)),
        [completion = std::move(completion), operation = std::move(operation)](
            std::exception_ptr error) mutable noexcept {
          completion(std::move(error));
        });
    return writer;
  }

  template <auto PrepareAsync, typename Request, typename Response>
  std::shared_ptr<AsyncWriter<Request>> asyncBidirectionalStreaming(
      datasink::grpc::CallOptions options,
      std::function<void(Response)> response,
      std::function<void(std::exception_ptr)> completion) {
    if (!response || !completion) {
      throw std::invalid_argument("gRPC streaming callback is empty");
    }
    auto operation = operations_.acquire();
    if (!operation) throw std::runtime_error("gRPC client pool is stopped");
    using RPC = agrpc::ClientRPC<PrepareAsync>;
    auto rpc = std::make_shared<RPC>(context_);
    auto queue = std::make_shared<detail::ClientWriteQueue<Request>>();
    Stub* client = &next();
    auto writer = std::make_shared<AsyncWriter<Request>>(
        [queue](Request request) { queue->push(std::move(request)); },
        [queue] { queue->close(); },
        [queue, rpc] {
          queue->close();
          rpc->context().TryCancel();
        });
    boost::asio::co_spawn(
        servicelib::detail::ParallelExecutorRegistry::Get(),
        RunBidirectionalStreamingClient<PrepareAsync, Stub, Request, Response>(
            rpc, queue, *client, std::move(options.context),
            std::move(response)),
        [completion = std::move(completion), operation = std::move(operation)](
            std::exception_ptr error) mutable noexcept {
          completion(std::move(error));
        });
    return writer;
  }

 private:
  Stub& next() noexcept {
    const auto index =
        next_.fetch_add(1, std::memory_order_relaxed) % clients_.size();
    return *clients_[index];
  }

  agrpc::GrpcContext& context_;
  std::vector<std::unique_ptr<Stub>> clients_;
  std::atomic<std::size_t> next_{};
  servicelib::detail::AsyncOperations operations_;
};

}  // namespace servicelib::grpc_transport

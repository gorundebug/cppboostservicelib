#pragma once

#include <utility>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <servicelib/datasource/http/router.hpp>
#include <servicelib/runtime/caller.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace servicelib::http {

template <typename T, typename R, typename E>
class SourceEndpoint final
    : public std::enable_shared_from_this<SourceEndpoint<T, R, E>> {
 public:
  using Emit = std::function<void(T)>;
  using Decode = std::function<void(const Request&, MessageContext, Emit)>;
  using EncodeResult = std::function<Response(const R&)>;
  using EncodeError = std::function<Response(const E&)>;
  using ResultId = std::function<std::string(const R&)>;
  using ErrorId = std::function<std::string(const E&)>;

  struct Options final {
    std::chrono::milliseconds resultTimeout{30000};
    Response accepted{202, {}, {}, "application/json; charset=utf-8", true};
  };

  SourceEndpoint(boost::asio::any_io_executor executor, Consumer<T> consumer,
                 Decode decode, EncodeResult encodeResult = {},
                 EncodeError encodeError = {}, ResultId resultId = {},
                 ErrorId errorId = {})
      : SourceEndpoint(std::move(executor), std::move(consumer),
                       std::move(decode), std::move(encodeResult),
                       std::move(encodeError), std::move(resultId),
                       std::move(errorId), Options{}) {}

  SourceEndpoint(boost::asio::any_io_executor executor, Consumer<T> consumer,
                 Decode decode, EncodeResult encodeResult,
                 EncodeError encodeError, ResultId resultId, ErrorId errorId,
                 Options options)
      : executor_(std::move(executor)),
        consumer_(std::move(consumer)),
        decode_(std::move(decode)),
        encodeResult_(std::move(encodeResult)),
        encodeError_(std::move(encodeError)),
        resultId_(std::move(resultId)),
        errorId_(std::move(errorId)),
        options_(std::move(options)) {
    if (!consumer_ || !decode_)
      throw std::invalid_argument("HTTP source consumer and decoder are required");
    if (options_.resultTimeout <= std::chrono::milliseconds::zero())
      throw std::invalid_argument("HTTP source result timeout must be positive");
  }

  void Register(Router& router, std::string method, std::string path) {
    auto self = this->shared_from_this();
    router.Add(std::move(method), std::move(path),
               [self](Request request, MessageContext context) {
                 return self->Handle(std::move(request), std::move(context));
               });
  }

  boost::asio::awaitable<Response> Handle(Request request,
                                          MessageContext context) {
    if (stopped_.load(std::memory_order_acquire))
      co_return TextResponse(503, "HTTP source is stopped\n", request.keepAlive);
    const bool expectsResult = static_cast<bool>(encodeResult_);
    std::shared_ptr<Pending> pending;
    if (expectsResult) {
      pending = std::make_shared<Pending>(executor_);
      pending->id = std::string(context.streamId());
      auto timeout = std::chrono::duration_cast<std::chrono::steady_clock::duration>(
          options_.resultTimeout);
      if (const auto remaining = context.remaining())
        timeout = std::min(timeout, *remaining);
      pending->timer.expires_after(
          std::max(timeout, std::chrono::steady_clock::duration::zero()));
      {
        std::lock_guard lock(mutex_);
        if (!pending_.emplace(pending->id, pending).second)
          co_return TextResponse(409, "duplicate HTTP stream id\n",
                                 request.keepAlive);
      }
    }

    std::size_t emitted{};
    try {
      decode_(request, context, [&](T value) {
        ++emitted;
        consumer_(context, Payload<T>::make(std::move(value)));
      });
    } catch (const std::exception& error) {
      if (pending) Remove(pending);
      co_return TextResponse(400, error.what(), request.keepAlive);
    } catch (...) {
      if (pending) Remove(pending);
      co_return TextResponse(400, "invalid HTTP request\n", request.keepAlive);
    }
    if (emitted == 0) {
      if (pending) Remove(pending);
      co_return TextResponse(400, "HTTP request produced no messages\n",
                             request.keepAlive);
    }
    if (!pending) {
      auto response = options_.accepted;
      response.keepAlive = request.keepAlive;
      co_return response;
    }

    auto cancel = [executor = executor_, weak = std::weak_ptr<Pending>(pending)] {
      boost::asio::post(executor, [weak] {
        if (const auto value = weak.lock()) value->timer.expires_at(
            std::chrono::steady_clock::now());
      });
    };
    using StopCallback = std::stop_callback<std::function<void()>>;
    auto stopCallback = std::make_unique<StopCallback>(
        context.stopToken(), std::function<void()>(cancel));
    std::vector<std::unique_ptr<StopCallback>> externalCallbacks;
    for (const auto& token : context.externalStopTokens())
      externalCallbacks.push_back(std::make_unique<StopCallback>(
          token, std::function<void()>(cancel)));

    boost::system::error_code error;
    co_await pending->timer.async_wait(
        boost::asio::redirect_error(boost::asio::use_awaitable, error));
    static_cast<void>(stopCallback);
    static_cast<void>(externalCallbacks);
    Remove(pending);
    std::lock_guard lock(pending->mutex);
    if (pending->response) {
      pending->response->keepAlive = request.keepAlive;
      co_return std::move(*pending->response);
    }
    const bool explicitlyCancelled = context.stopToken().stop_requested() ||
        std::ranges::any_of(context.externalStopTokens(),
                            [](const std::stop_token& token) {
                              return token.stop_requested();
                            });
    if (explicitlyCancelled)
      co_return TextResponse(499, "request cancelled\n", false);
    co_return TextResponse(504, "pipeline result timed out\n", false);
  }

  bool CompleteResult(const R& result) {
    if (!resultId_) return false;
    return Complete(resultId_(result), encodeResult_ ? encodeResult_(result)
                                                     : Response{});
  }

  bool CompleteResult(std::string streamId, const R& result) {
    if (!encodeResult_) return false;
    return Complete(std::move(streamId), encodeResult_(result));
  }

  bool CompleteError(const E& error) {
    if (!errorId_ || !encodeError_) return false;
    return Complete(errorId_(error), encodeError_(error));
  }

  bool CompleteError(std::string streamId, const E& error) {
    if (!encodeError_) return false;
    return Complete(std::move(streamId), encodeError_(error));
  }

  void Stop() noexcept {
    if (stopped_.exchange(true, std::memory_order_acq_rel)) return;
    std::vector<std::shared_ptr<Pending>> pending;
    {
      std::lock_guard lock(mutex_);
      for (const auto& [unused, value] : pending_) pending.push_back(value);
    }
    for (const auto& value : pending) {
      boost::asio::post(executor_, [value] {
        {
          std::lock_guard lock(value->mutex);
          if (!value->response)
            value->response = TextResponse(503, "HTTP source stopped\n", false);
        }
        value->timer.cancel();
      });
    }
  }

  [[nodiscard]] std::size_t pendingCount() const {
    std::lock_guard lock(mutex_);
    return pending_.size();
  }

 private:
  struct Pending final {
    explicit Pending(boost::asio::any_io_executor executor)
        : timer(std::move(executor)) {}
    std::string id;
    boost::asio::steady_timer timer;
    std::mutex mutex;
    std::optional<Response> response;
  };

  static Response TextResponse(int status, std::string body, bool keepAlive) {
    if (!body.ends_with('\n')) body.push_back('\n');
    return {status, {}, std::move(body), "text/plain; charset=utf-8", keepAlive};
  }

  bool Complete(std::string id, Response response) {
    std::shared_ptr<Pending> pending;
    {
      std::lock_guard lock(mutex_);
      const auto found = pending_.find(id);
      if (found == pending_.end()) return false;
      pending = found->second;
    }
    boost::asio::post(executor_,
                      [pending, response = std::move(response)]() mutable {
      {
        std::lock_guard lock(pending->mutex);
        if (pending->response) return;
        pending->response = std::move(response);
      }
      pending->timer.cancel();
    });
    return true;
  }

  void Remove(const std::shared_ptr<Pending>& pending) {
    std::lock_guard lock(mutex_);
    const auto found = pending_.find(pending->id);
    if (found != pending_.end() && found->second == pending) pending_.erase(found);
  }

  boost::asio::any_io_executor executor_;
  Consumer<T> consumer_;
  Decode decode_;
  EncodeResult encodeResult_;
  EncodeError encodeError_;
  ResultId resultId_;
  ErrorId errorId_;
  Options options_;
  std::atomic<bool> stopped_{};
  mutable std::mutex mutex_;
  std::unordered_map<std::string, std::shared_ptr<Pending>> pending_;
};

}  // namespace servicelib::http

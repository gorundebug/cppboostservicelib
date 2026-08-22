#pragma once

#include <utility>

#include <boost/asio/awaitable.hpp>

#include <servicelib/runtime/detail/http_types.hpp>

#include <functional>
#include <atomic>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>

namespace servicelib::http {

class Router final {
 public:
  using Handler =
      std::function<boost::asio::awaitable<Response>(Request, MessageContext)>;
  using SyncHandler = std::function<Response(Request, MessageContext)>;

  void Add(std::string method, std::string path, Handler handler) {
    AddRoute(std::move(method), std::move(path), std::move(handler), true);
  }

  void AddSync(std::string method, std::string path, SyncHandler handler) {
    if (!handler) throw std::invalid_argument("HTTP handler is required");
    AddRoute(
        std::move(method), std::move(path),
        [handler = std::move(handler)](Request request,
                                       MessageContext context)
            -> boost::asio::awaitable<Response> {
          co_return handler(std::move(request), std::move(context));
        },
        false);
  }

  // Generated services finish registering routes before Server::Start().
  // Publishing the immutable table removes a process-wide mutex from every
  // request while keeping Add() safe and preserving direct, unfrozen Router
  // use in tests and embedders.
  void Freeze() {
    std::lock_guard lock(mutex_);
    frozen_.store(true, std::memory_order_release);
  }

  [[nodiscard]] bool RequiresDisconnectObservation(
      const std::string& method, const std::string& path) const {
    const auto lookup = [&] {
      const auto methodIt = routes_.find(method);
      if (methodIt == routes_.end()) return false;
      const auto routeIt = methodIt->second.find(path);
      return routeIt != methodIt->second.end() &&
             routeIt->second.observeDisconnect;
    };
    if (frozen_.load(std::memory_order_acquire)) return lookup();
    std::lock_guard lock(mutex_);
    return lookup();
  }

  [[nodiscard]] boost::asio::awaitable<Response> Dispatch(
      Request request, MessageContext context) const {
    Route route;
    bool pathExists{};
    const auto lookup = [&] {
      const auto methodIt = routes_.find(request.method);
      if (methodIt != routes_.end()) {
        const auto routeIt = methodIt->second.find(request.path);
        if (routeIt != methodIt->second.end()) route = routeIt->second;
      }
      if (!route.handler) {
        for (const auto& [unusedMethod, methodRoutes] : routes_) {
          if (methodRoutes.contains(request.path)) {
            pathExists = true;
            break;
          }
        }
      }
    };
    if (frozen_.load(std::memory_order_acquire)) {
      lookup();
    } else {
      std::lock_guard lock(mutex_);
      lookup();
    }
    if (!route.handler)
      co_return Response{pathExists ? 405 : 404, {},
                         pathExists ? "method not allowed\n" : "not found\n",
                         "text/plain; charset=utf-8", request.keepAlive};
    co_return co_await route.handler(std::move(request), std::move(context));
  }

 private:
  struct Route final {
    Handler handler;
    bool observeDisconnect{};
  };

  void AddRoute(std::string method, std::string path, Handler handler,
                bool observeDisconnect) {
    if (method.empty() || path.empty() || !handler)
      throw std::invalid_argument("HTTP route method, path and handler are required");
    if (path.front() != '/') path.insert(path.begin(), '/');
    std::lock_guard lock(mutex_);
    if (frozen_.load(std::memory_order_relaxed)) {
      throw std::logic_error("cannot add an HTTP route after router freeze");
    }
    auto& methodRoutes = routes_[method];
    if (!methodRoutes
             .emplace(path, Route{std::move(handler), observeDisconnect})
             .second) {
      throw std::invalid_argument("duplicate HTTP route: " + method + " " +
                                  path);
    }
  }

  mutable std::mutex mutex_;
  std::unordered_map<std::string, std::unordered_map<std::string, Route>>
      routes_;
  std::atomic<bool> frozen_{};
};

}  // namespace servicelib::http

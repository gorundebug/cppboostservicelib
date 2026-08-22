#pragma once

#include <servicelib/datasource/http/router.hpp>
#include <servicelib/runtime/status/status.hpp>
#include <servicelib/runtime/status/web.generated.hpp>

#include <functional>
#include <string>
#include <unordered_set>
#include <utility>

namespace servicelib::http {

// Beast boundary for the same routes and response contracts implemented by
// runtime/telemetry/userver/status.hpp and MetricsHandler in cppservicelib.
inline void RegisterStatusRoutes(
    Router& router, const status::Provider& status,
    std::function<std::string()> exposeMetrics,
    std::string statusPath = "/status",
    std::string metricsPath = "/metrics",
    std::string startupPath = "/health/startup",
    std::string readinessPath = "/health/ready",
    std::string livenessPath = "/health/live") {
  if (!statusPath.empty() && statusPath.front() != '/') {
    statusPath.insert(0, "/");
  }
  if (!metricsPath.empty() && metricsPath.front() != '/') {
    metricsPath.insert(0, "/");
  }

  if (!metricsPath.empty() && exposeMetrics) {
    router.AddSync(
        "GET", std::move(metricsPath),
        [exposeMetrics = std::move(exposeMetrics)](Request request,
                                                    MessageContext) {
          return Response{200,
                          {},
                          exposeMetrics(),
                          "text/plain; version=0.0.4; charset=utf-8",
                          request.keepAlive};
        });
  }
  if (!statusPath.empty()) {
    router.AddSync("GET", statusPath,
                   [](Request request, MessageContext) {
                     return Response{200,
                                     {},
                                     std::string(status::web::kStatusHtml),
                                     "text/html; charset=utf-8",
                                     request.keepAlive};
                   });
    router.AddSync(
        "GET", statusPath + "/data",
        [&status](Request request, MessageContext) {
          return Response{200,
                          {},
                          status.networkDataJson(),
                          "application/json; charset=utf-8",
                          request.keepAlive};
        });
    router.AddSync("GET", statusPath + "/graph",
                   [&status](Request request, MessageContext) {
                     return Response{200,
                                     {},
                                     status.graphYaml(),
                                     "text/yaml; charset=utf-8",
                                     request.keepAlive};
                   });

    const auto immutableAsset = [&router](std::string path, std::string body,
                                          std::string contentType) {
      router.AddSync(
          "GET", std::move(path),
          [body = std::move(body), contentType = std::move(contentType)](
              Request request, MessageContext) {
            Headers headers{{"Cache-Control",
                             "public, max-age=31536000, immutable"}};
            return Response{200, std::move(headers), body, contentType,
                            request.keepAlive};
          });
    };
    immutableAsset(statusPath + "/vis.min.js",
                   std::string(status::web::kVisJavaScript),
                   "application/javascript; charset=utf-8");
    immutableAsset(statusPath + "/vis.min.css",
                   std::string(status::web::kVisCss),
                   "text/css; charset=utf-8");
  }

  std::unordered_set<std::string> registeredHealthPaths;
  for (auto& path : {&startupPath, &readinessPath, &livenessPath}) {
    if (path->empty()) continue;
    if (path->front() != '/') path->insert(0, "/");
    if (!registeredHealthPaths.emplace(*path).second) continue;
    router.AddSync("GET", std::move(*path),
                   [](Request request, MessageContext) {
                     return Response{200, {}, "ok\n",
                                     "text/plain; charset=utf-8",
                                     request.keepAlive};
                   });
  }
}

}  // namespace servicelib::http

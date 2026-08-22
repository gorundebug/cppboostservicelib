#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>

#include <chrono>
#include <future>
#include <string>

#include <gtest/gtest.h>
#include <yaml-cpp/yaml.h>

#include <servicelib/runtime/status/status.hpp>
#include <servicelib/runtime/status/http.hpp>
#include <servicelib/runtime/status/web.generated.hpp>
#include "mockservice/config/config.hpp"

TEST(Status, BuildsLiveTopologyDataAndGraphYaml) {
  const auto config = mockservice::config::MakeConfig();
  const servicelib::config::RuntimeConfig runtime(config);

  const auto data = YAML::Load(servicelib::status::MakeNetworkDataJson(
      runtime, [](servicelib::config::LinkID link) {
        return static_cast<std::int64_t>(link.from * 100 + link.to);
      }));

  const auto input =
      runtime.GetStreamConfigByID(mockservice::config::kInputRequestId);
  ASSERT_TRUE(input.has_value());
  EXPECT_EQ(servicelib::status::StreamIconPath(runtime, *input),
            servicelib::status::kApiIcon);

  ASSERT_TRUE(data["nodes"]);
  ASSERT_TRUE(data["edges"]);
  ASSERT_FALSE(data["nodes"].IsNull());
  ASSERT_GT(data["nodes"].size(), 1);
  const auto image =
      data["nodes"][0]["image"]["unselected"].as<std::string>();
  const auto second_image =
      data["nodes"][1]["image"]["unselected"].as<std::string>();
  EXPECT_TRUE(image.starts_with("data:image/svg+xml;charset=utf-8,"));
  EXPECT_NE(image.find("%3Csvg"), std::string::npos);
  EXPECT_NE(image.find("rx=%2230%22"), std::string::npos);
  EXPECT_NE(second_image.find("rx=%2210%22"), std::string::npos);
  EXPECT_NE(image, second_image);

  const auto yaml = servicelib::status::MakeGraphYaml(runtime);
  EXPECT_NE(yaml.find("services:"), std::string::npos);
  EXPECT_NE(yaml.find("streams:"), std::string::npos);
  EXPECT_NE(yaml.find("pools:"), std::string::npos);
  EXPECT_NE(yaml.find("links:"), std::string::npos);
  EXPECT_NE(yaml.find("callSemantics: PriorityTaskPool"), std::string::npos);
  EXPECT_NE(yaml.find("poolName: \"PriorityTaskPool\""), std::string::npos);
  EXPECT_NE(yaml.find("IncomeService"), std::string::npos);
}

TEST(Status, EmbedsTheSameBrowserAssetsAsOtherRuntimes) {
  EXPECT_GT(servicelib::status::web::kStatusHtml.size(), 1000);
  EXPECT_NE(servicelib::status::web::kStatusHtml.find("new vis.DataSet"),
            std::string_view::npos);
  EXPECT_NE(servicelib::status::web::kStatusHtml.find(
                "window.setTimeout(refreshNetwork, 1000)"),
            std::string_view::npos);
  EXPECT_GT(servicelib::status::web::kVisJavaScript.size(), 100000);
  EXPECT_GT(servicelib::status::web::kVisCss.size(), 10000);
}

namespace {

class StatusProvider final : public servicelib::status::Provider {
 public:
  std::string networkDataJson() const override {
    return R"({"nodes":[{"id":1}],"edges":[]})";
  }
  std::string graphYaml() const override { return "services:\n  - id: 1\n"; }
};

servicelib::http::Response Dispatch(servicelib::http::Router& router,
                                    std::string target) {
  boost::asio::io_context io;
  servicelib::http::Request request;
  request.method = "GET";
  request.path = target;
  request.target = std::move(target);
  auto response = boost::asio::co_spawn(
      io, router.Dispatch(std::move(request), servicelib::MessageContext{}),
      boost::asio::use_future);
  while (response.wait_for(std::chrono::milliseconds{0}) !=
         std::future_status::ready) {
    EXPECT_GT(io.run_one(), 0U);
  }
  return response.get();
}

}  // namespace

TEST(Status, BeastRoutesMatchCanonicalStatusAndMetricsHandlers) {
  servicelib::http::Router router;
  StatusProvider provider;
  servicelib::http::RegisterStatusRoutes(
      router, provider, [] { return std::string{"metric_total 3\n"}; });

  const auto metrics = Dispatch(router, "/metrics");
  EXPECT_EQ(metrics.status, 200);
  EXPECT_EQ(metrics.contentType,
            "text/plain; version=0.0.4; charset=utf-8");
  EXPECT_EQ(metrics.body, "metric_total 3\n");

  const auto page = Dispatch(router, "/status");
  EXPECT_EQ(page.status, 200);
  EXPECT_EQ(page.contentType, "text/html; charset=utf-8");
  EXPECT_EQ(page.body, servicelib::status::web::kStatusHtml);

  const auto data = Dispatch(router, "/status/data");
  EXPECT_EQ(data.status, 200);
  EXPECT_EQ(data.contentType, "application/json; charset=utf-8");
  EXPECT_EQ(data.body, provider.networkDataJson());

  const auto graph = Dispatch(router, "/status/graph");
  EXPECT_EQ(graph.status, 200);
  EXPECT_EQ(graph.contentType, "text/yaml; charset=utf-8");
  EXPECT_EQ(graph.body, provider.graphYaml());

  const auto script = Dispatch(router, "/status/vis.min.js");
  EXPECT_EQ(script.status, 200);
  EXPECT_EQ(script.contentType, "application/javascript; charset=utf-8");
  EXPECT_EQ(script.headers.at("Cache-Control"),
            "public, max-age=31536000, immutable");
  EXPECT_EQ(script.body, servicelib::status::web::kVisJavaScript);
}

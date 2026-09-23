#include "connector_test.grpc.pb.h"

#include <servicelib/runtime/detail/grpc_client.hpp>
#include <servicelib/runtime/detail/grpc_runtime.hpp>
#include <servicelib/runtime/detail/grpc_transport.hpp>

#include <agrpc/client_rpc.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/post.hpp>
#include <grpcpp/server_builder.h>

#include <array>
#include <atomic>
#include <chrono>
#include <csignal>
#include <ctime>
#include <future>
#include <iomanip>
#include <iostream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace asio = boost::asio;
using Clock = std::chrono::steady_clock;
using Request = servicelib::test::EchoRequest;
using Response = servicelib::test::EchoResponse;
using Service = servicelib::test::ConnectorTest;
using Pool = servicelib::grpc_transport::ClientPool<Service::Stub>;
using Runtime = servicelib::async::GrpcRuntime;
using namespace std::chrono_literals;

namespace {
volatile std::sig_atomic_t stopped = 0;
void Stop(int) { stopped = 1; }

struct Lane {
  std::string payload;
  std::uint64_t completed{};
  std::uint64_t errors{};
  std::uint64_t wrongExecutor{};
  std::chrono::nanoseconds latency{};
  std::chrono::nanoseconds maximum{};
  Clock::time_point started;
};

class Measurement {
 public:
  Measurement(Runtime& runtime, Pool& pool,
              std::vector<std::unique_ptr<Service::Stub>>& stubs,
              bool pooled, std::size_t concurrency)
      : runtime_(runtime), pool_(pool), stubs_(stubs), pooled_(pooled),
        lanes_(concurrency), remaining_(concurrency) {
    for (std::size_t i = 0; i < concurrency; ++i) {
      lanes_[i].payload = std::to_string(i) + ":" + std::string(128, 'x');
    }
  }

  bool Run(int seconds, bool report) {
    const auto start = Clock::now();
    until_ = start + std::chrono::seconds(seconds);
    const auto cpuStart = std::clock();
    auto completed = done_.get_future();
    for (auto& lane : lanes_) {
      asio::post(runtime_.executor(), [this, lane = &lane] { Next(*lane); });
    }
    // All requests have a deadline; a missing completion must fail visibly.
    if (completed.wait_for(std::chrono::seconds(seconds) + 15s) !=
        std::future_status::ready) {
      std::cerr << "client completion timed out\n";
      std::abort();
    }
    const double elapsed = std::chrono::duration<double>(Clock::now() - start).count();
    const double cpu = static_cast<double>(std::clock() - cpuStart) / CLOCKS_PER_SEC;
    std::uint64_t count{}, errors{}, wrongExecutor{};
    std::chrono::nanoseconds latency{}, maximum{};
    for (const auto& lane : lanes_) {
      count += lane.completed;
      errors += lane.errors;
      wrongExecutor += lane.wrongExecutor;
      latency += lane.latency;
      maximum = std::max(maximum, lane.maximum);
    }
    if (report) {
      std::cout << std::fixed << std::setprecision(3)
                << "mode=" << (pooled_ ? "pool" : "direct")
                << " concurrency=" << lanes_.size()
                << " count=" << count << " seconds=" << elapsed
                << " rps=" << static_cast<double>(count) / elapsed
                << " errors=" << errors << " wrong_executor=" << wrongExecutor
                << " avg_ms=" << std::chrono::duration<double, std::milli>(latency).count() /
                                       static_cast<double>(count)
                << " max_ms=" << std::chrono::duration<double, std::milli>(maximum).count()
                << " cpu_seconds=" << cpu << " cpu_cores=" << cpu / elapsed
                << std::endl;
    }
    return errors == 0 && wrongExecutor == 0 && count != 0;
  }

 private:
  struct Call {
    grpc::ClientContext context;
    Request request;
    Response response;
  };

  void Next(Lane& lane) {
    lane.started = Clock::now();
    Request request;
    request.set_value(lane.payload);
    if (pooled_) {
      pool_.asyncUnary<&Service::Stub::PrepareAsyncUnary, Request, Response>(
          std::move(request),
          servicelib::datasink::grpc::callOptions(
              servicelib::MessageContext{}.withDeadline(lane.started + 5s)),
          [this, &lane](std::exception_ptr error, std::optional<Response> response) {
            Complete(lane, !error && response && response->value() == lane.payload);
          });
    } else {
      auto call = std::make_shared<Call>();
      call->request = std::move(request);
      call->context.set_deadline(std::chrono::system_clock::now() + 5s);
      auto& stub = *stubs_[nextStub_.fetch_add(1, std::memory_order_relaxed) % stubs_.size()];
      agrpc::ClientRPC<&Service::Stub::PrepareAsyncUnary>::request(
          runtime_.grpcContext(), stub, call->context, call->request, call->response,
          asio::bind_executor(runtime_.executor(),
              [this, &lane, call](grpc::Status status) {
                Complete(lane, status.ok() && call->response.value() == lane.payload);
              }));
    }
  }

  void Complete(Lane& lane, bool valid) {
    const auto now = Clock::now();
    ++lane.completed;
    if (!valid) ++lane.errors;
    if (!runtime_.ioContext().get_executor().running_in_this_thread())
      ++lane.wrongExecutor;
    const auto latency = std::chrono::duration_cast<std::chrono::nanoseconds>(now - lane.started);
    lane.latency += latency;
    lane.maximum = std::max(lane.maximum, latency);
    if (now < until_) {
      Next(lane);
    } else if (remaining_.fetch_sub(1) == 1) {
      done_.set_value();
    }
  }

  Runtime& runtime_;
  Pool& pool_;
  std::vector<std::unique_ptr<Service::Stub>>& stubs_;
  bool pooled_;
  std::vector<Lane> lanes_;
  std::atomic<std::size_t> remaining_;
  std::atomic<std::size_t> nextStub_{};
  std::promise<void> done_;
  Clock::time_point until_;
};

int Server() {
  Service::AsyncService service;
  grpc::ServerBuilder builder;
  builder.AddListeningPort("0.0.0.0:9202", grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  auto queue = builder.AddCompletionQueue();
  auto server = builder.BuildAndStart();
  if (!server) throw std::runtime_error("server failed to start");
  Runtime runtime({.workers = 4}, std::move(queue));
  servicelib::grpc_transport::RegisterUnarySource<&Service::AsyncService::RequestUnary>(
      runtime.grpcContext(), service,
      [](servicelib::MessageContext, const Request& request) -> asio::awaitable<Response> {
        Response response;
        response.set_value(request.value());
        co_return response;
      }, runtime.grpcExecutor());
  runtime.Start();
  std::signal(SIGTERM, Stop);
  std::signal(SIGINT, Stop);
  std::cout << "ready" << std::endl;
  while (!stopped) std::this_thread::sleep_for(100ms);
  server->Shutdown();
  runtime.Stop();
  runtime.Join();
  return 0;
}
}  // namespace

int main(int argc, char** argv) {
  if (argc == 2 && std::string(argv[1]) == "server") return Server();
  if (argc < 2) throw std::runtime_error("expected server or server address");
  const int duration = argc > 2 ? std::stoi(argv[2]) : 20;
  const auto concurrency = argc > 3 ? static_cast<std::size_t>(std::stoul(argv[3])) : 256U;
  if (duration < 1 || concurrency < 1) throw std::runtime_error("invalid measurement size");
  Runtime runtime({.workers = 2});
  Pool pool(runtime.grpcContext(), argv[1], 2,
            [](std::shared_ptr<grpc::Channel> channel) { return Service::NewStub(std::move(channel)); });
  std::vector<std::unique_ptr<Service::Stub>> stubs;
  for (int i = 0; i < 2; ++i) {
    auto channel = grpc::CreateChannel(argv[1], grpc::InsecureChannelCredentials());
    if (!channel->WaitForConnected(std::chrono::system_clock::now() + 10s))
      throw std::runtime_error("server not ready");
    stubs.push_back(Service::NewStub(std::move(channel)));
  }
  runtime.Start();
  bool success = true;
  // Three measurements per implementation with alternating order. Both use
  // the same runtime, payload, deadline and callback executor, without HTTP.
  for (const bool pooled : std::array{false, true, true, false, false, true}) {
    Measurement warmup(runtime, pool, stubs, pooled, concurrency);
    success = warmup.Run(5, false) && success;
    Measurement measurement(runtime, pool, stubs, pooled, concurrency);
    success = measurement.Run(duration, true) && success;
  }
  runtime.Stop();
  runtime.Join();
  return success ? 0 : 1;
}

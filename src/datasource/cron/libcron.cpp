/*
 * Copyright (c) 2026 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 * Licensed under the MIT License. See the
 * [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

#include <servicelib/datasource/cron/libcron.hpp>

#include <libcron/Cron.h>
#include <libcron/CronSchedule.h>

#include <boost/asio/steady_timer.hpp>

#include <array>
#include <atomic>
#include <charconv>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <mutex>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <utility>
#include <vector>

#include <servicelib/runtime/config/dataconnector_types.hpp>
#include <servicelib/runtime/config/endpoint_types.hpp>
#include <servicelib/runtime/datasource.hpp>
#include <servicelib/runtime/detail/asio_dispatch.hpp>

namespace servicelib::datasource::cron {
namespace {

using Scheduler = libcron::Cron<libcron::UTCClock, libcron::Locker>;
using Clock = std::chrono::system_clock;

std::string NewStreamId() {
  static std::atomic<std::uint64_t> sequence{};
  const auto now = static_cast<std::uint64_t>(
      std::chrono::steady_clock::now().time_since_epoch().count());
  const auto value = sequence.fetch_add(1, std::memory_order_relaxed);
  std::array<char, 2 * sizeof(std::uint64_t) * 2 + 1> buffer{};
  auto* current = buffer.data();
  auto* end = buffer.data() + buffer.size();
  current = std::to_chars(current, end, now, 16).ptr;
  *current++ = '-';
  const auto result = std::to_chars(current, end, value, 16);
  return {buffer.data(), result.ptr};
}

config::CronEndpointConfig EndpointConfig(
    const IServiceEnvironment& environment, int endpointId) {
  const auto runtime = environment.getRuntimeConfigSnapshot();
  const auto value = runtime ? runtime->GetEndpointConfigByID(endpointId)
                             : std::nullopt;
  const auto* result = value ? value->As<config::CronEndpointConfig>() : nullptr;
  if (!result) {
    throw std::invalid_argument("cron endpoint config not found");
  }
  return *result;
}

config::CronDataConnectorConfig ConnectorConfig(
    const IServiceEnvironment& environment, int connectorId) {
  const auto runtime = environment.getRuntimeConfigSnapshot();
  const auto value = runtime ? runtime->GetDataConnectorByID(connectorId)
                             : std::nullopt;
  const auto* result =
      value ? value->As<config::CronDataConnectorConfig>() : nullptr;
  if (!result) {
    throw std::invalid_argument("cron data connector config not found");
  }
  if (result->implementation !=
      servicelib::api::DataConnectorImplementation::kCppLibcron) {
    throw std::invalid_argument("cron data connector implementation must be cpp/libcron");
  }
  return *result;
}

}  // namespace

std::string ToLibcronExpression(const std::string& expression) {
  std::istringstream input(expression);
  std::vector<std::string> fields;
  std::string field;
  while (input >> field) fields.push_back(std::move(field));
  if (fields.size() != 5) {
    throw std::invalid_argument(
        "portable cron expression must contain exactly five fields");
  }
  const bool dayOfMonthSpecified = fields[2] != "*";
  const bool dayOfWeekSpecified = fields[4] != "*";
  if (dayOfMonthSpecified && dayOfWeekSpecified) {
    throw std::invalid_argument(
        "portable cron expression cannot constrain both day-of-month and day-of-week");
  }
  if (dayOfMonthSpecified) {
    fields[4] = "?";
  } else if (dayOfWeekSpecified) {
    fields[2] = "?";
  } else {
    fields[4] = "?";
  }
  return "0 " + fields[0] + " " + fields[1] + " " + fields[2] + " " +
         fields[3] + " " + fields[4];
}

struct Endpoint::Impl final {
  Impl(IServiceEnvironment& environmentValue, int endpointIdValue,
       Output outputValue)
      : environment(environmentValue),
        endpointId(endpointIdValue),
        endpointName(EndpointConfig(environment, endpointId).name),
        output(std::move(outputValue)),
        metrics(environment.getMetrics(), environment.getLogger(),
                ConnectorConfig(environment,
                                EndpointConfig(environment, endpointId)
                                    .idDataConnector)
                    .name,
                endpointName, "cron") {}

  std::optional<std::string> configure() {
    const auto cfg = EndpointConfig(environment, endpointId);
    if (!cfg.enabled) return std::nullopt;
    if (cfg.timezone != "UTC") {
      throw std::invalid_argument("scheduled endpoint timezone must be UTC");
    }
    overlapPolicy = cfg.overlapPolicy;
    missedRunPolicy = cfg.missedRunPolicy;
    const auto expression = ToLibcronExpression(cfg.schedule);
    auto cronData = libcron::CronData::create(expression);
    if (!cronData.is_valid()) {
      throw std::invalid_argument("invalid cron schedule for endpoint " +
                                  endpointName);
    }
    evaluator.emplace(cronData);
    lastScheduled.reset();
    {
      std::lock_guard lock(mutex);
      stopping = false;
    }
    return expression;
  }

  void fire(const libcron::TaskInformation& information) {
    const auto firedAt = Clock::now();
    const auto scheduledAt = firedAt - information.get_delay();
    std::size_t due = 1;
    if (lastScheduled && evaluator) {
      auto cursor = *lastScheduled + std::chrono::seconds{1};
      while (cursor <= scheduledAt) {
        const auto [valid, next] = evaluator->calculate_from(cursor);
        if (!valid || next >= scheduledAt) break;
        ++due;
        cursor = next + std::chrono::seconds{1};
      }
    }
    lastScheduled = scheduledAt;
    if (due > 1 &&
        missedRunPolicy == api::ScheduleMissedRunPolicy::kSkip) {
      return;
    }

    {
      std::lock_guard lock(mutex);
      if (stopping) return;
      if (overlapPolicy == api::ScheduleOverlapPolicy::kSkip && active != 0) {
        return;
      }
      ++active;
    }
    try {
      servicelib::detail::ParallelExecutorRegistry::Post([this, scheduledAt] {
        auto context = ApplyDataSourceEndpointTracing(
            MessageContext{}.withStreamId(NewStreamId()), environment,
            endpointId);
        const auto started = metrics.requestStart();
        std::exception_ptr error;
        try {
          output(std::move(context), Payload<ScheduleTrigger>::make(
              MakeScheduleTrigger(endpointId, endpointName, scheduledAt,
                                  Clock::now(), ScheduleBackend::kLocal)));
        } catch (...) {
          error = std::current_exception();
        }
        metrics.requestEnd(started, error);
        {
          std::lock_guard lock(mutex);
          --active;
        }
        drained.notify_all();
      });
    } catch (...) {
      {
        std::lock_guard lock(mutex);
        --active;
      }
      drained.notify_all();
      throw;
    }
  }

  void stop(Context context) noexcept {
    std::unique_lock lock(mutex);
    stopping = true;
    if (context.deadline()) {
      static_cast<void>(drained.wait_until(
          lock, *context.deadline(), [this] { return active == 0; }));
    } else {
      drained.wait(lock, [this] { return active == 0; });
    }
  }

  IServiceEnvironment& environment;
  int endpointId;
  std::string endpointName;
  Output output;
  DataSourceEndpointMetrics metrics;
  api::ScheduleOverlapPolicy overlapPolicy{api::ScheduleOverlapPolicy::kSkip};
  api::ScheduleMissedRunPolicy missedRunPolicy{
      api::ScheduleMissedRunPolicy::kSkip};
  std::optional<libcron::CronSchedule> evaluator;
  std::optional<Clock::time_point> lastScheduled;
  std::mutex mutex;
  std::condition_variable drained;
  std::size_t active{};
  bool stopping{true};
};

Endpoint::Endpoint(IServiceEnvironment& environment, int endpointId,
                   Output output)
    : impl_(std::make_unique<Impl>(environment, endpointId,
                                  std::move(output))) {}

Endpoint::~Endpoint() = default;
int Endpoint::id() const noexcept { return impl_->endpointId; }
const std::string& Endpoint::name() const noexcept { return impl_->endpointName; }

struct LibcronDataSource::Impl final {
  Impl(IServiceEnvironment& environmentValue, int connectorIdValue)
      : environment(environmentValue),
        connectorId(connectorIdValue),
        connectorName(ConnectorConfig(environment, connectorId).name) {}

  void scheduleTick() {
    timer->expires_after(std::chrono::milliseconds{500});
    timer->async_wait([this](const boost::system::error_code& error) {
      if (error || !started.load(std::memory_order_acquire)) return;
      try {
        scheduler.tick();
      } catch (const std::exception& exception) {
        try {
          environment.getLogger().error(
              "cron scheduler tick failed", {log::Field::Err(exception)});
        } catch (...) {
        }
      } catch (...) {
        try {
          environment.getLogger().error(
              "cron scheduler tick failed with an unknown error");
        } catch (...) {
        }
      }
      if (started.load(std::memory_order_acquire)) scheduleTick();
    });
  }

  IServiceEnvironment& environment;
  int connectorId;
  std::string connectorName;
  std::vector<std::shared_ptr<Endpoint>> endpoints;
  Scheduler scheduler;
  std::unique_ptr<boost::asio::steady_timer> timer;
  std::atomic<bool> started{false};
};

std::shared_ptr<LibcronDataSource> LibcronDataSource::make(
    IServiceEnvironment& environment, int connectorId) {
  return std::shared_ptr<LibcronDataSource>(
      new LibcronDataSource(environment, connectorId));
}

LibcronDataSource::LibcronDataSource(IServiceEnvironment& environment,
                                     int connectorId)
    : impl_(std::make_unique<Impl>(environment, connectorId)) {}

LibcronDataSource::~LibcronDataSource() = default;

void LibcronDataSource::addEndpoint(std::shared_ptr<Endpoint> endpoint) {
  if (!endpoint) throw std::invalid_argument("cron endpoint is null");
  if (impl_->started.load(std::memory_order_acquire)) {
    throw std::logic_error("cron endpoints must be added before start");
  }
  for (const auto& existing : impl_->endpoints) {
    if (existing->id() == endpoint->id()) {
      throw std::logic_error("duplicate cron endpoint: " + endpoint->name());
    }
  }
  const auto cfg = EndpointConfig(impl_->environment, endpoint->id());
  if (cfg.idDataConnector != impl_->connectorId) {
    throw std::invalid_argument("cron endpoint references another connector");
  }
  impl_->endpoints.push_back(std::move(endpoint));
}

void LibcronDataSource::start(Context context) {
  static_cast<void>(context);
  bool expected = false;
  if (!impl_->started.compare_exchange_strong(expected, true,
                                               std::memory_order_acq_rel)) {
    throw std::logic_error("cron data source is already started");
  }
  try {
    for (const auto& endpoint : impl_->endpoints) {
      const auto expression = endpoint->impl_->configure();
      if (!expression) continue;
      if (!impl_->scheduler.add_schedule(
              endpoint->name(), *expression,
              [endpoint](const libcron::TaskInformation& information) {
                endpoint->impl_->fire(information);
              })) {
        throw std::invalid_argument("invalid cron schedule for endpoint " +
                                    endpoint->name());
      }
    }
    impl_->timer = std::make_unique<boost::asio::steady_timer>(
        servicelib::detail::ParallelExecutorRegistry::Get());
    impl_->scheduleTick();
  } catch (...) {
    impl_->started.store(false, std::memory_order_release);
    impl_->scheduler.clear_schedules();
    for (const auto& endpoint : impl_->endpoints) endpoint->impl_->stop({});
    throw;
  }
}

void LibcronDataSource::stop(Context context) {
  if (!impl_->started.exchange(false, std::memory_order_acq_rel)) return;
  if (impl_->timer) {
    impl_->timer->cancel();
  }
  impl_->scheduler.clear_schedules();
  for (const auto& endpoint : impl_->endpoints) endpoint->impl_->stop(context);
  impl_->timer.reset();
}

int LibcronDataSource::id() const noexcept { return impl_->connectorId; }
const std::string& LibcronDataSource::getName() const noexcept {
  return impl_->connectorName;
}

}  // namespace servicelib::datasource::cron

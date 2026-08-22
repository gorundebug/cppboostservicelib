#include <servicelib/runtime/environment/metrics/prometheus.hpp>

#include <cassert>
#include <string>

int main() {
  servicelib::metrics::PrometheusMetrics metrics;
  auto scope = metrics.scope("task_pool", {{"service", "orders"}});
  auto tasks = scope->counter("tasks_total", "Accepted tasks",
                              {{"name", "default"}});
  auto active = scope->gauge("active", "Active tasks");
  auto duration = scope->histogram("duration_seconds", "Task duration", {},
                                   {0.1, 1.0});
  double lag = 0.25;
  auto observable = scope->observableFloat64Gauge(
      "event_loop_lag_seconds", "Event loop lag", [&] { return lag; });
  tasks->add(3);
  active->set(2);
  duration->observe(0.05);
  duration->observe(0.5);
  const auto output = metrics.Expose();
  assert(output.find("# TYPE task_pool_tasks_total counter") !=
         std::string::npos);
  assert(output.find(
             "task_pool_tasks_total{name=\"default\",service=\"orders\"} 3") !=
         std::string::npos);
  assert(output.find("task_pool_active{service=\"orders\"} 2") !=
         std::string::npos);
  assert(output.find(
             "task_pool_duration_seconds_bucket{le=\"0.10000000000000001\",service=\"orders\"} 1") !=
         std::string::npos);
  assert(output.find("task_pool_duration_seconds_count{service=\"orders\"} 2") !=
         std::string::npos);
  assert(output.find(
             "task_pool_event_loop_lag_seconds{service=\"orders\"} 0.25") !=
         std::string::npos);
  static_cast<void>(observable);

  bool negativeCounter = false;
  try {
    tasks->add(-1);
  } catch (const std::invalid_argument&) {
    negativeCounter = true;
  }
  assert(negativeCounter);
}

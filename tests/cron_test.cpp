/*
 * Copyright (c) 2026 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 * Licensed under the MIT License. See the
 * [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

#include <stdexcept>

#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <array>

#include "test_async.hpp"

#include <servicelib/datasource/cron/libcron.hpp>

TEST(CronDataSource, AdaptsPortableExpressionsToLibcron) {
  using servicelib::datasource::cron::ToLibcronExpression;
  EXPECT_EQ(ToLibcronExpression("*/5 * * * *"), "0 */5 * * * ?");
  EXPECT_EQ(ToLibcronExpression("30 8 1 * *"), "0 30 8 1 * ?");
  EXPECT_EQ(ToLibcronExpression("15 9 * * MON-FRI"),
            "0 15 9 ? * MON-FRI");
}

TEST(CronDataSource, RejectsNonPortableExpressions) {
  using servicelib::datasource::cron::ToLibcronExpression;
  EXPECT_THROW(ToLibcronExpression("0 0 1 * MON"), std::invalid_argument);
  EXPECT_THROW(ToLibcronExpression("0 0 * *"), std::invalid_argument);
  EXPECT_THROW(ToLibcronExpression("0 0 0 * * ?"), std::invalid_argument);
}

TEST(CronDataSource, WaitsForCorrelatedPipelineResult) {
  servicelib::datasource::cron::detail::ResultWaiter waiter;
  auto pending = waiter.begin("request-1");
  std::atomic<bool> completed{false};
  std::thread waiting([&] {
    waiter.wait("request-1", pending);
    completed.store(true, std::memory_order_release);
  });
  std::this_thread::yield();
  EXPECT_FALSE(completed.load(std::memory_order_acquire));
  EXPECT_EQ(waiter.complete("request-1"),
            servicelib::datasource::cron::detail::ResultWaiter::Completion::
                kCompleted);
  waiting.join();
  EXPECT_TRUE(completed.load(std::memory_order_acquire));
}

TEST(CronDataSource, CorrelatedWaitDoesNotOccupyExecutorWorkers) {
  test_async::AsioRuntime runtime;
  servicelib::datasource::cron::detail::ResultWaiter waiter;
  std::array<std::string, 4> ids{"cron-0", "cron-1", "cron-2", "cron-3"};
  std::atomic<unsigned> entered{0};
  std::atomic<unsigned> finished{0};
  test_async::Event allEntered;
  test_async::Event allFinished;
  test_async::Event delivered;
  for (const auto& id : ids) {
    auto pending = waiter.begin(id);
    servicelib::detail::ParallelExecutorRegistry::Post([&, id, pending] {
      if (entered.fetch_add(1) == ids.size() - 1) allEntered.Send();
      waiter.wait(id, pending);
      if (finished.fetch_add(1) == ids.size() - 1) allFinished.Send();
    });
  }
  EXPECT_TRUE(allEntered.WaitForEvent());
  servicelib::detail::ParallelExecutorRegistry::Post([&] {
    for (const auto& id : ids) static_cast<void>(waiter.complete(id));
    delivered.Send();
  });
  EXPECT_TRUE(delivered.WaitForEventFor(std::chrono::milliseconds{200}));
  // Rescue a blocking implementation so a failed assertion does not hang CTest.
  for (const auto& id : ids) static_cast<void>(waiter.complete(id));
  EXPECT_TRUE(allFinished.WaitForEvent());
  EXPECT_TRUE(delivered.WaitForEvent());
}

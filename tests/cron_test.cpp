/*
 * Copyright (c) 2026 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 * Licensed under the MIT License. See the
 * [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

#include <stdexcept>

#include <gtest/gtest.h>

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

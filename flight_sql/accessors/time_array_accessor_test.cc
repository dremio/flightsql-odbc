/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include "arrow/testing/builder.h"
#include "time_array_accessor.h"
#include "utils.h"
#include "gtest/gtest.h"
#include "odbcabstraction/calendar_utils.h"

namespace driver {
namespace flight_sql {

using namespace arrow;
using namespace odbcabstraction;

TEST(TEST_TIME32, TIME_WITH_SECONDS) {
  auto value_field = field("f0", time32(TimeUnit::SECOND));

  std::vector<int32_t> t32_values = {14896, 14897, 14892, 85400, 14893, 14895};

  std::shared_ptr<Array> time32_array;
  ArrayFromVector<Time32Type, int32_t>(value_field->type(),
                                       t32_values, &time32_array);

  TimeArrayFlightSqlAccessor<CDataType_TIME, Time32Array, TimeUnit::SECOND> accessor(time32_array.get());

  std::vector<TIME_STRUCT> buffer(t32_values.size());
  std::vector<ssize_t> strlen_buffer(t32_values.size());

  ColumnBinding binding(CDataType_TIME, 0, 0, buffer.data(), 0, strlen_buffer.data());

  int64_t value_offset = 0;
  odbcabstraction::Diagnostics diagnostics("Foo", "Foo", OdbcVersion::V_3);
  ASSERT_EQ(t32_values.size(),
          accessor.GetColumnarData(&binding, 0, t32_values.size(), value_offset, false, diagnostics, nullptr));

  for (size_t i = 0; i < t32_values.size(); ++i) {
    ASSERT_EQ(sizeof(TIME_STRUCT), strlen_buffer[i]);

    tm time{};

    GetTimeForSecondsSinceEpoch(time, t32_values[i]);
    ASSERT_EQ(buffer[i].hour, time.tm_hour);
    ASSERT_EQ(buffer[i].minute, time.tm_min);
    ASSERT_EQ(buffer[i].second, time.tm_sec);
  }
}

TEST(TEST_TIME32, TIME_WITH_MILLI) {
  auto value_field = field("f0", time32(TimeUnit::MILLI));
  std::vector<int32_t> t32_values = {14896000, 14897000, 14892000,
                                     85400000, 14893000, 14895000};

  std::shared_ptr<Array> time32_array;
  ArrayFromVector<Time32Type, int32_t>(value_field->type(),
                                       t32_values, &time32_array);

  TimeArrayFlightSqlAccessor<CDataType_TIME, Time32Array, TimeUnit::MILLI> accessor(time32_array.get());

  std::vector<TIME_STRUCT> buffer(t32_values.size());
  std::vector<ssize_t> strlen_buffer(t32_values.size());

  ColumnBinding binding(CDataType_TIME, 0, 0, buffer.data(), 0, strlen_buffer.data());

  int64_t value_offset = 0;
  odbcabstraction::Diagnostics diagnostics("Foo", "Foo", OdbcVersion::V_3);
  ASSERT_EQ(t32_values.size(),
          accessor.GetColumnarData(&binding, 0, t32_values.size(), value_offset, false, diagnostics, nullptr));

  for (size_t i = 0; i < t32_values.size(); ++i) {
    ASSERT_EQ(sizeof(TIME_STRUCT), strlen_buffer[i]);

    tm time{};

    auto convertedValue = t32_values[i] / MILLI_TO_SECONDS_DIVISOR;
    GetTimeForSecondsSinceEpoch(time, convertedValue);

    ASSERT_EQ(buffer[i].hour, time.tm_hour);
    ASSERT_EQ(buffer[i].minute, time.tm_min);
    ASSERT_EQ(buffer[i].second, time.tm_sec);
  }
}

TEST(TEST_TIME64, TIME_WITH_MICRO) {
  auto value_field = field("f0", time64(TimeUnit::MICRO));

  std::vector<int64_t> t64_values = {14896000, 14897000, 14892000,
                                     85400000, 14893000, 14895000};

  std::shared_ptr<Array> time64_array;
  ArrayFromVector<Time64Type, int64_t>(value_field->type(),
                                       t64_values, &time64_array);

  TimeArrayFlightSqlAccessor<CDataType_TIME, Time64Array, TimeUnit::MICRO> accessor(time64_array.get());

  std::vector<TIME_STRUCT> buffer(t64_values.size());
  std::vector<ssize_t> strlen_buffer(t64_values.size());

  ColumnBinding binding(CDataType_TIME, 0, 0, buffer.data(), 0, strlen_buffer.data());

  int64_t value_offset = 0;
  odbcabstraction::Diagnostics diagnostics("Foo", "Foo", OdbcVersion::V_3);
  ASSERT_EQ(t64_values.size(),
          accessor.GetColumnarData(&binding, 0, t64_values.size(), value_offset, false, diagnostics, nullptr));

  for (size_t i = 0; i < t64_values.size(); ++i) {
    ASSERT_EQ(sizeof(TIME_STRUCT), strlen_buffer[i]);

    tm time{};

    const auto convertedValue = t64_values[i] / MICRO_TO_SECONDS_DIVISOR;
    GetTimeForSecondsSinceEpoch(time, convertedValue);

    ASSERT_EQ(buffer[i].hour, time.tm_hour);
    ASSERT_EQ(buffer[i].minute, time.tm_min);
    ASSERT_EQ(buffer[i].second, time.tm_sec);
  }
}

TEST(TEST_TIME64, TIME_WITH_NANO) {
  auto value_field = field("f0", time64(TimeUnit::NANO));
  std::vector<int64_t> t64_values = {14896000000, 14897000000, 14892000000,
                                     85400000000, 14893000000, 14895000000};

  std::shared_ptr<Array> time64_array;
  ArrayFromVector<Time64Type, int64_t>(value_field->type(),
                                       t64_values, &time64_array);

  TimeArrayFlightSqlAccessor<CDataType_TIME, Time64Array, TimeUnit::NANO> accessor(
      time64_array.get());

  std::vector<TIME_STRUCT> buffer(t64_values.size());
  std::vector<ssize_t> strlen_buffer(t64_values.size());

  ColumnBinding binding(CDataType_TIME, 0, 0, buffer.data(), 0, strlen_buffer.data());

  int64_t value_offset = 0;
  odbcabstraction::Diagnostics diagnostics("Foo", "Foo", OdbcVersion::V_3);
  ASSERT_EQ(t64_values.size(),
          accessor.GetColumnarData(&binding, 0, t64_values.size(), value_offset, false, diagnostics, nullptr));

  for (size_t i = 0; i < t64_values.size(); ++i) {
    ASSERT_EQ(sizeof(TIME_STRUCT), strlen_buffer[i]);

    tm time{};

    const auto convertedValue = t64_values[i] / NANO_TO_SECONDS_DIVISOR;
    GetTimeForSecondsSinceEpoch(time, convertedValue);

    ASSERT_EQ(buffer[i].hour, time.tm_hour);
    ASSERT_EQ(buffer[i].minute, time.tm_min);
    ASSERT_EQ(buffer[i].second, time.tm_sec);
  }
}
} // namespace flight_sql
} // namespace driver

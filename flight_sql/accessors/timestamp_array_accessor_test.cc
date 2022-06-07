/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include "arrow/testing/builder.h"
#include "timestamp_array_accessor.h"
#include "utils.h"
#include "gtest/gtest.h"
#include "odbcabstraction/calendar_utils.h"

namespace driver {
namespace flight_sql {

using namespace arrow;
using namespace odbcabstraction;

TEST(TEST_TIMESTAMP, TIMESTAMP_WITH_MILI) {
  std::vector<int64_t> values = {86400370,  172800000, 259200000, 1649793238110LL,
                                 345600000, 432000000, 518400000};

  std::shared_ptr<Array> timestamp_array;

  auto timestamp_field = field("timestamp_field", timestamp(TimeUnit::MILLI));
  ArrayFromVector<TimestampType, int64_t>(timestamp_field->type(),
                                          values, &timestamp_array);

  TimestampArrayFlightSqlAccessor<CDataType_TIMESTAMP> accessor(timestamp_array.get());

  std::vector<TIMESTAMP_STRUCT> buffer(values.size());
  std::vector<ssize_t> strlen_buffer(values.size());

  int64_t value_offset = 0;
  ColumnBinding binding(CDataType_TIMESTAMP, 0, 0, buffer.data(), 0, strlen_buffer.data());
  odbcabstraction::Diagnostics diagnostics("Foo", "Foo", OdbcVersion::V_3);
  ASSERT_EQ(values.size(),
          accessor.GetColumnarData(&binding, 0, values.size(), value_offset, false, diagnostics));

  for (size_t i = 0; i < values.size(); ++i) {
    ASSERT_EQ(sizeof(TIMESTAMP_STRUCT), strlen_buffer[i]);

    tm date{};

    auto converted_time = values[i] / MILLI_TO_SECONDS_DIVISOR;
    GetTimeForMillisSinceEpoch(date, converted_time);

    ASSERT_EQ(buffer[i].year, 1900 + (date.tm_year));
    ASSERT_EQ(buffer[i].month, date.tm_mon + 1);
    ASSERT_EQ(buffer[i].day, date.tm_mday);
    ASSERT_EQ(buffer[i].hour, date.tm_hour);
    ASSERT_EQ(buffer[i].minute, date.tm_min);
    ASSERT_EQ(buffer[i].second, date.tm_sec);
    ASSERT_EQ(buffer[i].fraction, values[i] % MILLI_TO_SECONDS_DIVISOR);
  }
}

TEST(TEST_TIMESTAMP, TIMESTAMP_WITH_SECONDS) {
  std::vector<int64_t> values = {86400,  172800, 259200, 1649793238,
                                 345600, 432000, 518400};

  std::shared_ptr<Array> timestamp_array;

  auto timestamp_field = field("timestamp_field", timestamp(TimeUnit::SECOND));
  ArrayFromVector<TimestampType, int64_t>(timestamp_field->type(),
                                          values, &timestamp_array);

  TimestampArrayFlightSqlAccessor<CDataType_TIMESTAMP> accessor(timestamp_array.get());

  std::vector<TIMESTAMP_STRUCT> buffer(values.size());
  std::vector<ssize_t> strlen_buffer(values.size());

  int64_t value_offset = 0;
  ColumnBinding binding(CDataType_TIMESTAMP, 0, 0, buffer.data(), 0, strlen_buffer.data());
  odbcabstraction::Diagnostics diagnostics("Foo", "Foo", OdbcVersion::V_3);

  ASSERT_EQ(values.size(),
          accessor.GetColumnarData(&binding, 0, values.size(), value_offset, false, diagnostics));

  for (size_t i = 0; i < values.size(); ++i) {
    ASSERT_EQ(sizeof(TIMESTAMP_STRUCT), strlen_buffer[i]);
    tm date{};

    auto converted_time = values[i];
    GetTimeForMillisSinceEpoch(date, converted_time);

    ASSERT_EQ(buffer[i].year, 1900 + (date.tm_year));
    ASSERT_EQ(buffer[i].month, date.tm_mon + 1);
    ASSERT_EQ(buffer[i].day, date.tm_mday);
    ASSERT_EQ(buffer[i].hour, date.tm_hour);
    ASSERT_EQ(buffer[i].minute, date.tm_min);
    ASSERT_EQ(buffer[i].second, date.tm_sec);
    ASSERT_EQ(buffer[i].fraction, values[i] % 1);
  }
}

TEST(TEST_TIMESTAMP, TIMESTAMP_WITH_MICRO) {
  std::vector<int64_t> values = {86400000000, 1649793238000000};

  std::shared_ptr<Array> timestamp_array;

  auto timestamp_field = field("timestamp_field", timestamp(TimeUnit::MICRO));
  ArrayFromVector<TimestampType, int64_t>(timestamp_field->type(),
                                          values, &timestamp_array);

  TimestampArrayFlightSqlAccessor<CDataType_TIMESTAMP> accessor(timestamp_array.get());

  std::vector<TIMESTAMP_STRUCT> buffer(values.size());
  std::vector<ssize_t> strlen_buffer(values.size());

  int64_t value_offset = 0;
  ColumnBinding binding(CDataType_TIMESTAMP, 0, 0, buffer.data(), 0, strlen_buffer.data());
  odbcabstraction::Diagnostics diagnostics("Foo", "Foo", OdbcVersion::V_3);

    ASSERT_EQ(values.size(),
            accessor.GetColumnarData(&binding, 0, values.size(), value_offset, false, diagnostics));

  for (size_t i = 0; i < values.size(); ++i) {
    ASSERT_EQ(sizeof(TIMESTAMP_STRUCT), strlen_buffer[i]);

    tm date{};

    auto converted_time = values[i] / MICRO_TO_SECONDS_DIVISOR;
    GetTimeForMillisSinceEpoch(date, converted_time);

    ASSERT_EQ(buffer[i].year, 1900 + (date.tm_year));
    ASSERT_EQ(buffer[i].month, date.tm_mon + 1);
    ASSERT_EQ(buffer[i].day, date.tm_mday);
    ASSERT_EQ(buffer[i].hour, date.tm_hour);
    ASSERT_EQ(buffer[i].minute, date.tm_min);
    ASSERT_EQ(buffer[i].second, date.tm_sec);
    ASSERT_EQ(buffer[i].fraction, values[i] % MICRO_TO_SECONDS_DIVISOR);
  }
}

TEST(TEST_TIMESTAMP, TIMESTAMP_WITH_NANO) {
  std::vector<int64_t> values = {86400000010000, 1649793238000000000};

  std::shared_ptr<Array> timestamp_array;

  auto timestamp_field = field("timestamp_field", timestamp(TimeUnit::NANO));
  ArrayFromVector<TimestampType, int64_t>(timestamp_field->type(),
                                          values, &timestamp_array);

  TimestampArrayFlightSqlAccessor<CDataType_TIMESTAMP> accessor(timestamp_array.get());

  std::vector<TIMESTAMP_STRUCT> buffer(values.size());
  std::vector<ssize_t> strlen_buffer(values.size());

  int64_t value_offset = 0;
  ColumnBinding binding(CDataType_TIMESTAMP, 0, 0, buffer.data(), 0, strlen_buffer.data());

  odbcabstraction::Diagnostics diagnostics("Foo", "Foo", OdbcVersion::V_3);
  ASSERT_EQ(values.size(),
          accessor.GetColumnarData(&binding, 0, values.size(), value_offset, false, diagnostics));

  for (size_t i = 0; i < values.size(); ++i) {
    ASSERT_EQ(sizeof(TIMESTAMP_STRUCT), strlen_buffer[i]);
    tm date{};

    auto converted_time = values[i] / NANO_TO_SECONDS_DIVISOR;
    GetTimeForMillisSinceEpoch(date, converted_time);

    ASSERT_EQ(buffer[i].year, 1900 + (date.tm_year));
    ASSERT_EQ(buffer[i].month, date.tm_mon + 1);
    ASSERT_EQ(buffer[i].day, date.tm_mday);
    ASSERT_EQ(buffer[i].hour, date.tm_hour);
    ASSERT_EQ(buffer[i].minute, date.tm_min);
    ASSERT_EQ(buffer[i].second, date.tm_sec);
    ASSERT_EQ(buffer[i].fraction, values[i] % NANO_TO_SECONDS_DIVISOR);
  }
}
} // namespace flight_sql
} // namespace driver

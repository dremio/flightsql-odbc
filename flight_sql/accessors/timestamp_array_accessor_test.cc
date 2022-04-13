// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "arrow/testing/builder.h"
#include "gtest/gtest.h"
#include "timestamp_array_accessor.h"
#include "utils.h"

namespace driver {
namespace flight_sql {

using namespace arrow;
using namespace odbcabstraction;

TEST(TEST_TIMESTAMP, TIMESTAMP_WITH_MILI) {
  std::vector<int64_t> values = {86400370,  172800000, 259200000, 1649793238110,
                                        345600000, 432000000, 518400000};

  std::vector<bool> is_valid = {true, true, true, true, true, true, true};

  std::shared_ptr<Array> timestamp_array;

  auto timestamp_field = field("timestamp_field", timestamp(TimeUnit::MILLI));
  ArrayFromVector<TimestampType, int64_t>(timestamp_field->type(), is_valid, values, &timestamp_array);

  TimestampArrayAccessor<CDataType_TIMESTAMP> accessor(timestamp_array.get());

  TIMESTAMP_STRUCT buffer[values.size()];
  ssize_t strlen_buffer[values.size()];

  ColumnBinding binding(CDataType_TIMESTAMP, 0, 0, buffer, 0, strlen_buffer);

  ASSERT_EQ(values.size(),
            accessor.GetColumnarData(&binding, 0, values.size(), 0));

  for (int i = 0; i < values.size(); ++i) {
    ASSERT_EQ(sizeof(TIMESTAMP_STRUCT), strlen_buffer[i]);

    static thread_local tm date;

    long converted_time = values[i] / MILLI_TO_SECONDS_DIVISOR;
    gmtime_r(&converted_time, &date);

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
    std::vector<bool> is_valid = {true, true, true, true, true, true, true};

    std::shared_ptr<Array> timestamp_array;

    auto timestamp_field = field("timestamp_field", timestamp(TimeUnit::SECOND));
    ArrayFromVector<TimestampType, int64_t>(timestamp_field->type(), is_valid, values, &timestamp_array);

    TimestampArrayAccessor<CDataType_TIMESTAMP> accessor(timestamp_array.get());

    TIMESTAMP_STRUCT buffer[values.size()];
    ssize_t strlen_buffer[values.size()];

    ColumnBinding binding(CDataType_TIMESTAMP, 0, 0, buffer, 0, strlen_buffer);

    ASSERT_EQ(values.size(),
              accessor.GetColumnarData(&binding, 0, values.size(), 0));

    for (int i = 0; i < values.size(); ++i) {
      ASSERT_EQ(sizeof(TIMESTAMP_STRUCT), strlen_buffer[i]);
      static thread_local tm date;

      long converted_time = values[i];
      gmtime_r(&converted_time, &date);

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
  std::vector<bool> is_valid = {true, true};

  std::shared_ptr<Array> timestamp_array;

  auto timestamp_field = field("timestamp_field", timestamp(TimeUnit::MICRO));
  ArrayFromVector<TimestampType, int64_t>(timestamp_field->type(), is_valid, values, &timestamp_array);

  TimestampArrayAccessor<CDataType_TIMESTAMP> accessor(timestamp_array.get());

  TIMESTAMP_STRUCT buffer[values.size()];
  ssize_t strlen_buffer[values.size()];

  ColumnBinding binding(CDataType_TIMESTAMP, 0, 0, buffer, 0, strlen_buffer);

  ASSERT_EQ(values.size(),
            accessor.GetColumnarData(&binding, 0, values.size(), 0));

  for (int i = 0; i < values.size(); ++i) {
    ASSERT_EQ(sizeof(TIMESTAMP_STRUCT), strlen_buffer[i]);

    static thread_local tm date;

    long converted_time = values[i] / MICRO_TO_SECONDS_DIVISOR;
    gmtime_r(&converted_time, &date);

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
  std::vector<bool> is_valid = {true, true};

  std::shared_ptr<Array> timestamp_array;

  auto timestamp_field = field("timestamp_field", timestamp(TimeUnit::NANO));
  ArrayFromVector<TimestampType, int64_t>(timestamp_field->type(), is_valid, values, &timestamp_array);

  TimestampArrayAccessor<CDataType_TIMESTAMP> accessor(timestamp_array.get());

  TIMESTAMP_STRUCT buffer[values.size()];
  ssize_t strlen_buffer[values.size()];

  ColumnBinding binding(CDataType_TIMESTAMP, 0, 0, buffer, 0, strlen_buffer);

  ASSERT_EQ(values.size(),
            accessor.GetColumnarData(&binding, 0, values.size(), 0));

  for (int i = 0; i < values.size(); ++i) {
    ASSERT_EQ(sizeof(TIMESTAMP_STRUCT), strlen_buffer[i]);
    static thread_local tm date;

    long converted_time = values[i] / NANO_TO_SECONDS_DIVISOR;
    gmtime_r(&converted_time, &date);

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

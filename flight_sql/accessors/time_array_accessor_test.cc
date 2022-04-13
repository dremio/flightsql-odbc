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
#include "time_array_accessor.h"
#include "utils.h"

namespace driver {
namespace flight_sql {

using namespace arrow;
using namespace odbcabstraction;

TEST(TEST_TIME32, TIME_WITH_SECONDS) {
  std::vector<bool> is_valid = {true, true, true, true, true, true};
  auto value_field = field("f0", time32(TimeUnit::SECOND));

  std::vector<int32_t> t32_values = {14896, 14897, 14892, 85400, 14893, 14895};

  std::shared_ptr<Array> time32_array;
  ArrayFromVector<Time32Type, int32_t>(value_field->type(), is_valid, t32_values, &time32_array);

  TimeArrayFlightSqlAccessor<CDataType_TIME, Time32Array> accessor(
    dynamic_cast<NumericArray<Time32Type> *>(time32_array.get()));

  TIME_STRUCT buffer[t32_values.size()];
  ssize_t strlen_buffer[t32_values.size()];

  ColumnBinding binding(CDataType_TIME, 0, 0, buffer, 0, strlen_buffer);

  ASSERT_EQ(t32_values.size(),
            accessor.GetColumnarData(&binding, 0, t32_values.size(), 0));

  for (int i = 0; i < t32_values.size(); ++i) {
    ASSERT_EQ(sizeof(TIME_STRUCT), strlen_buffer[i]);

    static thread_local tm time;

    long convertedValue = t32_values[i];
    gmtime_r(
      &convertedValue,
      &time);

    ASSERT_EQ(buffer[i].hour, time.tm_hour);
    ASSERT_EQ(buffer[i].minute, time.tm_min);
    ASSERT_EQ(buffer[i].second, time.tm_sec);
  }
}

TEST(TEST_TIME32, TIME_WITH_MILLI) {
  std::vector<bool> is_valid1 = {true, true, true, true, true, true};
  auto value_field = field("f0", time32(TimeUnit::MILLI));

  std::vector<int32_t> t32_values = {14896000, 14897000, 14892000, 85400000, 14893000, 14895000};

  std::shared_ptr<Array> time32_array;
  ArrayFromVector<Time32Type, int32_t>(value_field->type(), is_valid1, t32_values, &time32_array);

  TimeArrayFlightSqlAccessor<CDataType_TIME, Time32Array> accessor(
    dynamic_cast<NumericArray<Time32Type> *>(time32_array.get()));

  TIME_STRUCT buffer[t32_values.size()];
  ssize_t strlen_buffer[t32_values.size()];

  ColumnBinding binding(CDataType_TIME, 0, 0, buffer, 0, strlen_buffer);

  ASSERT_EQ(t32_values.size(),
            accessor.GetColumnarData(&binding, 0, t32_values.size(), 0));

  for (int i = 0; i < t32_values.size(); ++i) {
    ASSERT_EQ(sizeof(TIME_STRUCT), strlen_buffer[i]);

    static thread_local tm time;

    long convertedValue = t32_values[i] / MILLI_TO_SECONDS_DIVISOR;
    gmtime_r(
      &convertedValue,
      &time);

    ASSERT_EQ(buffer[i].hour, time.tm_hour);
    ASSERT_EQ(buffer[i].minute, time.tm_min);
    ASSERT_EQ(buffer[i].second, time.tm_sec);
  }
}

TEST(TEST_TIME64, TIME_WITH_MICRO) {
  std::vector<bool> is_valid1 = {true, true, true, true, true, true};
  auto value_field = field("f0", time64(TimeUnit::MICRO));

  std::vector<int64_t> t64_values = {14896000, 14897000, 14892000, 85400000, 14893000, 14895000};

  std::shared_ptr<Array> time64_array;
  ArrayFromVector<Time64Type, int64_t>(value_field->type(), is_valid1, t64_values, &time64_array);

  TimeArrayFlightSqlAccessor<CDataType_TIME, Time64Array> accessor(
    dynamic_cast<NumericArray<Time64Type> *>(time64_array.get()));

  TIME_STRUCT buffer[t64_values.size()];
  ssize_t strlen_buffer[t64_values.size()];

  ColumnBinding binding(CDataType_TIME, 0, 0, buffer, 0, strlen_buffer);

  ASSERT_EQ(t64_values.size(),
            accessor.GetColumnarData(&binding, 0, t64_values.size(), 0));

  for (int i = 0; i < t64_values.size(); ++i) {
    ASSERT_EQ(sizeof(TIME_STRUCT), strlen_buffer[i]);

    static thread_local tm time;

    const auto convertedValue = t64_values[i] / MICRO_TO_SECONDS_DIVISOR;
    gmtime_r(
      &convertedValue,
      &time);

    ASSERT_EQ(buffer[i].hour, time.tm_hour);
    ASSERT_EQ(buffer[i].minute, time.tm_min);
    ASSERT_EQ(buffer[i].second, time.tm_sec);
  }
}

TEST(TEST_TIME64, TIME_WITH_NANO) {
  std::vector<bool> is_valid1 = {true, true, true, true, true, true};
  auto value_field = field("f0", time64(TimeUnit::NANO));

  std::vector<int64_t> t64_values = {14896000000, 14897000000, 14892000000, 85400000000, 14893000000, 14895000000};

  std::shared_ptr<Array> time64_array;
  ArrayFromVector<Time64Type, int64_t>(value_field->type(), is_valid1, t64_values, &time64_array);

  TimeArrayFlightSqlAccessor<CDataType_TIME, Time64Array> accessor(
    dynamic_cast<NumericArray<Time64Type> *>(time64_array.get()));

  TIME_STRUCT buffer[t64_values.size()];
  ssize_t strlen_buffer[t64_values.size()];

  ColumnBinding binding(CDataType_TIME, 0, 0, buffer, 0, strlen_buffer);

  ASSERT_EQ(t64_values.size(),
            accessor.GetColumnarData(&binding, 0, t64_values.size(), 0));

  for (int i = 0; i < t64_values.size(); ++i) {
    ASSERT_EQ(sizeof(TIME_STRUCT), strlen_buffer[i]);

    static thread_local tm time;

    const auto convertedValue = t64_values[i] / NANO_TO_SECONDS_DIVISOR;
    gmtime_r(
      &convertedValue,
      &time);

    ASSERT_EQ(buffer[i].hour, time.tm_hour); 
    ASSERT_EQ(buffer[i].minute, time.tm_min); 
    ASSERT_EQ(buffer[i].second, time.tm_sec);
  }
}
} // namespace flight_sql
} // namespace driver

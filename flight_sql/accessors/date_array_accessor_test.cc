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
#include "boolean_array_accessor.h"
#include "date_array_accessor.h"
#include "gtest/gtest.h"

namespace driver {
namespace flight_sql {

using namespace arrow;
using namespace odbcabstraction;

TEST(DateArrayAccessor, Test_Date32Array_CDataType_DATE) {
  std::vector<int32_t> values = {7589, 12320, 18980, 19095};
  std::vector<bool> is_valid = {true, true, true, true};

  std::shared_ptr<Array> array;
  ArrayFromVector<Date32Type, int32_t>(is_valid, values, &array);

  DateArrayFlightSqlAccessor<CDataType_DATE, Date32Array> accessor(
      dynamic_cast<NumericArray<Date32Type> *>(array.get()));

  tagDATE_STRUCT buffer[values.size()];
  ssize_t strlen_buffer[values.size()];

  ColumnBinding binding(CDataType_DATE, 0, 0, buffer, 0, strlen_buffer);

  ASSERT_EQ(values.size(),
            accessor.GetColumnarData(&binding, 0, values.size(), 0));

  for (int i = 0; i < values.size(); ++i) {
    ASSERT_EQ(sizeof(DATE_STRUCT), strlen_buffer[i]);
    static thread_local tm date;

    long converted_time = values[i] * 86400;
    gmtime_r(&converted_time, &date);
    ASSERT_EQ((date.tm_year + 1900), buffer[i].year);
    ASSERT_EQ(date.tm_mon + 1, buffer[i].month);
    ASSERT_EQ(date.tm_mday, buffer[i].day);
  }
}

TEST(DateArrayAccessor, Test_Date64Array_CDataType_DATE) {
  std::vector<int64_t> values = {86400000,  172800000, 259200000, 1649793238110,
                                 345600000, 432000000, 518400000};
  std::vector<bool> is_valid = {true, true, true, true, true, true, true};

  std::shared_ptr<Array> array;
  ArrayFromVector<Date64Type, int64_t>(is_valid, values, &array);

  DateArrayFlightSqlAccessor<CDataType_DATE, Date64Array> accessor(
      dynamic_cast<NumericArray<Date64Type> *>(array.get()));

  tagDATE_STRUCT buffer[values.size()];
  ssize_t strlen_buffer[values.size()];

  ColumnBinding binding(CDataType_DATE, 0, 0, buffer, 0, strlen_buffer);

  ASSERT_EQ(values.size(),
            accessor.GetColumnarData(&binding, 0, values.size(), 0));

  for (int i = 0; i < values.size(); ++i) {
    ASSERT_EQ(sizeof(DATE_STRUCT), strlen_buffer[i]);
    static thread_local tm date;

    long converted_time = values[i] / 1000;
    gmtime_r(&converted_time, &date);
    ASSERT_EQ((date.tm_year + 1900), buffer[i].year);
    ASSERT_EQ(date.tm_mon + 1, buffer[i].month);
    ASSERT_EQ(date.tm_mday, buffer[i].day);
  }
}

} // namespace flight_sql
} // namespace driver

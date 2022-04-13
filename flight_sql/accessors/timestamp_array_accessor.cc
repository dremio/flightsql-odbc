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

#include "timestamp_array_accessor.h"

namespace driver {
namespace flight_sql {
namespace {
long convertTimeStampBasedOnUnit(TimeUnit::type unit) {
  long converted_result;
  switch (unit) {
  case TimeUnit::SECOND:
    converted_result = 1;
    break;
  case TimeUnit::MILLI:
    converted_result = MILLI_TO_SECONDS_DIVISOR;
    break;
  case TimeUnit::MICRO:
    converted_result = MICRO_TO_SECONDS_DIVISOR;
    break;
  case TimeUnit::NANO:
    converted_result = NANO_TO_SECONDS_DIVISOR;
    break;
  }
  return converted_result;
}
} // namespace

using namespace arrow;
using namespace odbcabstraction;

template <CDataType TARGET_TYPE>
TimestampArrayAccessor<TARGET_TYPE>::TimestampArrayAccessor(Array *array)
    : FlightSqlAccessor<TimestampArray, TARGET_TYPE,
                        TimestampArrayAccessor<TARGET_TYPE>>(array) {}

template <CDataType TARGET_TYPE>
void TimestampArrayAccessor<TARGET_TYPE>::MoveSingleCell_impl(
    ColumnBinding *binding, TimestampArray *array, int64_t i,
    int64_t value_offset) {
  typedef unsigned char c_type;
  auto *buffer = static_cast<TIMESTAMP_STRUCT *>(binding->buffer);

  auto time_type =
      arrow::internal::checked_pointer_cast<TimestampType>(array->type());

  long value = array->Value(i);
  const auto divisor = convertTimeStampBasedOnUnit(time_type->unit());
  const auto result_to_convert = value / divisor;
  static thread_local tm timestamp;

  gmtime_r(&result_to_convert, &timestamp);

  buffer[i].year = 1900 + (timestamp.tm_year);
  buffer[i].month = timestamp.tm_mon + 1;
  buffer[i].day = timestamp.tm_mday;
  buffer[i].hour = timestamp.tm_hour;
  buffer[i].minute = timestamp.tm_min;
  buffer[i].second = timestamp.tm_sec;
  buffer[i].fraction = value % divisor;

  if (binding->strlen_buffer) {
    binding->strlen_buffer[i] = static_cast<ssize_t>(sizeof(TIMESTAMP_STRUCT));
  }
}

template class TimestampArrayAccessor<odbcabstraction::CDataType_TIMESTAMP>;

} // namespace flight_sql
} // namespace driver

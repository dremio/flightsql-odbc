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

#include "time_array_accessor.h"
#include "odbcabstraction/calendar_utils.h"

namespace driver {
namespace flight_sql {
namespace {
template <typename T>
int64_t ConvertTimeValue(typename T::value_type value, TimeUnit::type unit) {
  return value;
}

template <>
int64_t ConvertTimeValue<Time32Array>(int32_t value, TimeUnit::type unit) {
  return unit == TimeUnit::SECOND ? value : value / MILLI_TO_SECONDS_DIVISOR;
}

template <>
int64_t ConvertTimeValue<Time64Array>(int64_t value, TimeUnit::type unit) {
  return unit == TimeUnit::MICRO ? value / MICRO_TO_SECONDS_DIVISOR
                                 : value / NANO_TO_SECONDS_DIVISOR;
}
} // namespace

template <CDataType TARGET_TYPE, typename ARROW_ARRAY>
TimeArrayFlightSqlAccessor<
    TARGET_TYPE, ARROW_ARRAY>::TimeArrayFlightSqlAccessor(Array *array)
    : FlightSqlAccessor<ARROW_ARRAY, TARGET_TYPE,
                        TimeArrayFlightSqlAccessor<TARGET_TYPE, ARROW_ARRAY>>(
          array) {}

template <CDataType TARGET_TYPE, typename ARROW_ARRAY>
void TimeArrayFlightSqlAccessor<TARGET_TYPE, ARROW_ARRAY>::MoveSingleCell_impl(
  ColumnBinding *binding, ARROW_ARRAY *array, int64_t cell_counter,
  int64_t value_offset, odbcabstraction::Diagnostics &diagnostic) {
  typedef unsigned char c_type;
  auto *buffer = static_cast<TIME_STRUCT *>(binding->buffer);

  auto time_type =
      arrow::internal::checked_pointer_cast<TimeType>(array->type());
  auto time_unit = time_type->unit();

  tm time{};

  auto converted_value =
      ConvertTimeValue<ARROW_ARRAY>(array->Value(cell_counter), time_unit);

  GetTimeForMillisSinceEpoch(time, converted_value);

  buffer[cell_counter].hour = time.tm_hour;
  buffer[cell_counter].minute = time.tm_min;
  buffer[cell_counter].second = time.tm_sec;

  if (binding->strlen_buffer) {
    binding->strlen_buffer[cell_counter] = static_cast<ssize_t>(sizeof(TIME_STRUCT));
  }
}

template class TimeArrayFlightSqlAccessor<odbcabstraction::CDataType_TIME,
                                          Time32Array>;
template class TimeArrayFlightSqlAccessor<odbcabstraction::CDataType_TIME,
                                          Time64Array>;

} // namespace flight_sql
} // namespace driver

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
#include "odbcabstraction/calendar_utils.h"

using namespace arrow;

namespace {
int64_t convertTimeStampBasedOnUnit(TimeUnit::type unit) {
  int64_t converted_result;
  switch (unit) {
    case TimeUnit::SECOND:
      converted_result = 1;
      break;
    case TimeUnit::MILLI:
      converted_result = driver::flight_sql::MILLI_TO_SECONDS_DIVISOR;
      break;
    case TimeUnit::MICRO:
      converted_result = driver::flight_sql::MICRO_TO_SECONDS_DIVISOR;
      break;
    case TimeUnit::NANO:
      converted_result = driver::flight_sql::NANO_TO_SECONDS_DIVISOR;
      break;
  }
  return converted_result;
}
} // namespace

namespace driver {
namespace flight_sql {

using namespace odbcabstraction;

template <CDataType TARGET_TYPE>
TimestampArrayFlightSqlAccessor<TARGET_TYPE>::TimestampArrayFlightSqlAccessor(Array *array)
    : FlightSqlAccessor<TimestampArray, TARGET_TYPE,
                        TimestampArrayFlightSqlAccessor<TARGET_TYPE>>(array),
                        timestamp_type_(
                          arrow::internal::checked_pointer_cast<TimestampType>(
                            array->type())) {}

template <CDataType TARGET_TYPE>
void
TimestampArrayFlightSqlAccessor<TARGET_TYPE>::MoveSingleCell_impl(ColumnBinding *binding,
                                                                  TimestampArray *array,
                                                                  int64_t cell_counter,
                                                                  int64_t &value_offset,
                                                                  bool update_value_offset,
                                                                  odbcabstraction::Diagnostics &diagnostics) {
  typedef unsigned char c_type;
  auto *buffer = static_cast<TIMESTAMP_STRUCT *>(binding->buffer);

  int64_t value = array->Value(cell_counter);
  const auto divisor = convertTimeStampBasedOnUnit(timestamp_type_->unit());
  const auto converted_result = value / divisor;
  tm timestamp{};

  GetTimeForMillisSinceEpoch(timestamp, converted_result);

  buffer[cell_counter].year = 1900 + (timestamp.tm_year);
  buffer[cell_counter].month = timestamp.tm_mon + 1;
  buffer[cell_counter].day = timestamp.tm_mday;
  buffer[cell_counter].hour = timestamp.tm_hour;
  buffer[cell_counter].minute = timestamp.tm_min;
  buffer[cell_counter].second = timestamp.tm_sec;
  buffer[cell_counter].fraction = value % divisor;

  if (binding->strlen_buffer) {
    binding->strlen_buffer[cell_counter] = static_cast<ssize_t>(sizeof(TIMESTAMP_STRUCT));
  }
}

template class TimestampArrayFlightSqlAccessor<odbcabstraction::CDataType_TIMESTAMP>;

} // namespace flight_sql
} // namespace driver

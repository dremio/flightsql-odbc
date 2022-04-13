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

#include "date_array_accessor.h"
#include "time.h"
#include "arrow/compute/api.h"
#include "calendar_utils.h"

using namespace arrow;


namespace {
  template <typename T> long convertDate(typename T::value_type value) {
    return value;
  }

/// Converts the value from the array, which is in milliseconds, to seconds.
/// \param value    the value extracted from the array in milliseconds.
/// \return         the converted value in seconds.
  template <> long convertDate<Date64Array>(int64_t value) {
    return value / driver::flight_sql::MILLI_TO_SECONDS_DIVISOR;
  }

/// Converts the value from the array, which is in days, to seconds.
/// \param value    the value extracted from the array in days.
/// \return         the converted value in seconds.
  template <> long convertDate<Date32Array>(int32_t value) {
    return value * driver::flight_sql::DAYS_TO_SECONDS_MULTIPLIER;
  }
} // namespace

namespace driver {
namespace flight_sql {

using namespace odbcabstraction;

template <CDataType TARGET_TYPE, typename ARROW_ARRAY>
DateArrayFlightSqlAccessor<
    TARGET_TYPE, ARROW_ARRAY>::DateArrayFlightSqlAccessor(Array *array)
    : FlightSqlAccessor<ARROW_ARRAY, TARGET_TYPE,
                        DateArrayFlightSqlAccessor<TARGET_TYPE, ARROW_ARRAY>>(
          array) {}

template <CDataType TARGET_TYPE, typename ARROW_ARRAY>
void DateArrayFlightSqlAccessor<TARGET_TYPE, ARROW_ARRAY>::MoveSingleCell_impl(
  ColumnBinding *binding, ARROW_ARRAY *array, int64_t cell_counter,
  int64_t value_offset, odbcabstraction::Diagnostics &diagnostics) {
  typedef unsigned char c_type;
  auto *buffer = static_cast<DATE_STRUCT *>(binding->buffer);
  long value = convertDate<ARROW_ARRAY>(array->Value(cell_counter));
  tm date{};

  GetTimeForAccessor(date, value);

  buffer[cell_counter].year = 1900 + (date.tm_year);
  buffer[cell_counter].month = date.tm_mon + 1;
  buffer[cell_counter].day = date.tm_mday;

  if (binding->strlen_buffer) {
    binding->strlen_buffer[cell_counter] = static_cast<ssize_t>(sizeof(DATE_STRUCT));
  }
}

template class DateArrayFlightSqlAccessor<odbcabstraction::CDataType_DATE,
                                          Date32Array>;
template class DateArrayFlightSqlAccessor<odbcabstraction::CDataType_DATE,
                                          Date64Array>;

} // namespace flight_sql
} // namespace driver

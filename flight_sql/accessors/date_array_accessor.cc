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

namespace driver {
namespace flight_sql {
namespace {
  template <typename T>
  long convertDate(long value) {
    return value;
  }

  template <>
  long convertDate<Date64Array>(long value) {
    return value/1000;
  }

  template <>
  long convertDate<Date32Array>(long value) {
    return value * 86400;
  }
}

using namespace arrow;
using namespace odbcabstraction;

template<CDataType TARGET_TYPE, typename ARROW_ARRAY>
DateArrayFlightSqlAccessor<TARGET_TYPE, ARROW_ARRAY>::DateArrayFlightSqlAccessor(
  ARROW_ARRAY *array) : FlightSqlAccessor<ARROW_ARRAY, TARGET_TYPE,
  DateArrayFlightSqlAccessor<TARGET_TYPE, ARROW_ARRAY>>(array) {}

template<CDataType TARGET_TYPE, typename ARROW_ARRAY>
void DateArrayFlightSqlAccessor<TARGET_TYPE, ARROW_ARRAY>::MoveSingleCell_impl(ColumnBinding *binding,
                                                                  ARROW_ARRAY *array, int64_t i,
                                                                  int64_t value_offset) {
  typedef unsigned char c_type;
  auto *buffer = static_cast<DATE_STRUCT *>(binding->buffer);
  long value = convertDate<ARROW_ARRAY>(array->Value(i));
  static thread_local tm date;

  gmtime_r(&value, &date);

  buffer[i].year = 1900 + (date.tm_year);
  buffer[i].month = date.tm_mon + 1;
  buffer[i].day = date.tm_mday;

  if (binding->strlen_buffer) {
    binding->strlen_buffer[i] = static_cast<ssize_t>(sizeof(unsigned char));
  }
}

template class DateArrayFlightSqlAccessor<odbcabstraction::CDataType_DATE, Date32Array>;
template class DateArrayFlightSqlAccessor<odbcabstraction::CDataType_DATE, Date64Array>;

} // namespace flight_sql
} // namespace driver

/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

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
RowStatus TimeArrayFlightSqlAccessor<TARGET_TYPE, ARROW_ARRAY>::MoveSingleCell_impl(
  ColumnBinding *binding, ARROW_ARRAY *array, int64_t cell_counter,
  int64_t &value_offset, bool update_value_offset, odbcabstraction::Diagnostics &diagnostic) {
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
    binding->strlen_buffer[cell_counter] = static_cast<ssize_t>(GetCellLength_impl(binding));
  }
  return odbcabstraction::RowStatus_SUCCESS;
}

template <CDataType TARGET_TYPE, typename ARROW_ARRAY>
size_t TimeArrayFlightSqlAccessor<TARGET_TYPE, ARROW_ARRAY>::GetCellLength_impl(ColumnBinding *binding) const {
  return sizeof(TIME_STRUCT);
}

template class TimeArrayFlightSqlAccessor<odbcabstraction::CDataType_TIME,
                                          Time32Array>;
template class TimeArrayFlightSqlAccessor<odbcabstraction::CDataType_TIME,
                                          Time64Array>;

} // namespace flight_sql
} // namespace driver

/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include "time_array_accessor.h"
#include "odbcabstraction/calendar_utils.h"

namespace driver {
namespace flight_sql {

Accessor* CreateTimeAccessor(arrow::Array *array, arrow::Type::type type) {
  auto time_type =
      arrow::internal::checked_pointer_cast<TimeType>(array->type());
  auto time_unit = time_type->unit();

  if (type == arrow::Type::TIME32) {
    switch (time_unit) {
    case TimeUnit::SECOND:
      return new TimeArrayFlightSqlAccessor<CDataType_TIME, Time32Array,
                                            TimeUnit::SECOND>(array);
    case TimeUnit::MILLI:
      return new TimeArrayFlightSqlAccessor<CDataType_TIME, Time32Array,
                                            TimeUnit::MILLI>(array);
    case TimeUnit::MICRO:
      return new TimeArrayFlightSqlAccessor<CDataType_TIME, Time32Array,
                                            TimeUnit::MICRO>(array);
    case TimeUnit::NANO:
      return new TimeArrayFlightSqlAccessor<CDataType_TIME, Time32Array,
                                            TimeUnit::NANO>(array);
    }
  } else if (type == arrow::Type::TIME64) {
    switch (time_unit) {
    case TimeUnit::SECOND:
      return new TimeArrayFlightSqlAccessor<CDataType_TIME, Time64Array,
                                            TimeUnit::SECOND>(array);
    case TimeUnit::MILLI:
      return new TimeArrayFlightSqlAccessor<CDataType_TIME, Time64Array,
                                            TimeUnit::MILLI>(array);
    case TimeUnit::MICRO:
      return new TimeArrayFlightSqlAccessor<CDataType_TIME, Time64Array,
                                            TimeUnit::MICRO>(array);
    case TimeUnit::NANO:
      return new TimeArrayFlightSqlAccessor<CDataType_TIME, Time64Array,
                                            TimeUnit::NANO>(array);
    }
  }
  assert(false);
  throw DriverException("Unsupported input supplied to CreateTimeAccessor");
}

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

template <CDataType TARGET_TYPE, typename ARROW_ARRAY, TimeUnit::type UNIT>
TimeArrayFlightSqlAccessor<
    TARGET_TYPE, ARROW_ARRAY, UNIT>::TimeArrayFlightSqlAccessor(Array *array)
    : FlightSqlAccessor<ARROW_ARRAY, TARGET_TYPE,
                        TimeArrayFlightSqlAccessor<TARGET_TYPE, ARROW_ARRAY, UNIT>>(
          array) {}

template <CDataType TARGET_TYPE, typename ARROW_ARRAY, TimeUnit::type UNIT>
RowStatus TimeArrayFlightSqlAccessor<TARGET_TYPE, ARROW_ARRAY, UNIT>::MoveSingleCell_impl(
  ColumnBinding *binding, int64_t cell_counter, int64_t &value_offset,
    bool update_value_offset, odbcabstraction::Diagnostics &diagnostic) {
  typedef unsigned char c_type;
  auto *buffer = static_cast<TIME_STRUCT *>(binding->buffer);

  tm time{};

  auto converted_value_seconds =
      ConvertTimeValue<ARROW_ARRAY>(this->GetArray()->Value(cell_counter), UNIT);

  GetTimeForSecondsSinceEpoch(time, converted_value_seconds);

  buffer[cell_counter].hour = time.tm_hour;
  buffer[cell_counter].minute = time.tm_min;
  buffer[cell_counter].second = time.tm_sec;

  if (binding->strlen_buffer) {
    binding->strlen_buffer[cell_counter] = static_cast<ssize_t>(GetCellLength_impl(binding));
  }
  return odbcabstraction::RowStatus_SUCCESS;
}

template <CDataType TARGET_TYPE, typename ARROW_ARRAY, TimeUnit::type UNIT>
size_t TimeArrayFlightSqlAccessor<TARGET_TYPE, ARROW_ARRAY, UNIT>::GetCellLength_impl(ColumnBinding *binding) const {
  return sizeof(TIME_STRUCT);
}

template class TimeArrayFlightSqlAccessor<odbcabstraction::CDataType_TIME,
                                          Time32Array, TimeUnit::SECOND>;
template class TimeArrayFlightSqlAccessor<odbcabstraction::CDataType_TIME,
                                          Time32Array, TimeUnit::MILLI>;
template class TimeArrayFlightSqlAccessor<odbcabstraction::CDataType_TIME,
                                          Time32Array, TimeUnit::MICRO>;
template class TimeArrayFlightSqlAccessor<odbcabstraction::CDataType_TIME,
                                          Time32Array, TimeUnit::NANO>;
template class TimeArrayFlightSqlAccessor<odbcabstraction::CDataType_TIME,
                                          Time64Array, TimeUnit::SECOND>;
template class TimeArrayFlightSqlAccessor<odbcabstraction::CDataType_TIME,
                                          Time64Array, TimeUnit::MILLI>;
template class TimeArrayFlightSqlAccessor<odbcabstraction::CDataType_TIME,
                                          Time64Array, TimeUnit::MICRO>;
template class TimeArrayFlightSqlAccessor<odbcabstraction::CDataType_TIME,
                                          Time64Array, TimeUnit::NANO>;

} // namespace flight_sql
} // namespace driver

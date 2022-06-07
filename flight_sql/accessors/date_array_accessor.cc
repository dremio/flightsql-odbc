/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include "date_array_accessor.h"
#include "time.h"
#include "arrow/compute/api.h"
#include "odbcabstraction/calendar_utils.h"

using namespace arrow;


namespace {
  template <typename T> int64_t convertDate(typename T::value_type value) {
    return value;
  }

/// Converts the value from the array, which is in milliseconds, to seconds.
/// \param value    the value extracted from the array in milliseconds.
/// \return         the converted value in seconds.
  template <> int64_t convertDate<Date64Array>(int64_t value) {
    return value / driver::flight_sql::MILLI_TO_SECONDS_DIVISOR;
  }

/// Converts the value from the array, which is in days, to seconds.
/// \param value    the value extracted from the array in days.
/// \return         the converted value in seconds.
  template <> int64_t convertDate<Date32Array>(int32_t value) {
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
  int64_t &value_offset, bool update_value_offset, odbcabstraction::Diagnostics &diagnostics) {
  typedef unsigned char c_type;
  auto *buffer = static_cast<DATE_STRUCT *>(binding->buffer);
  auto value = convertDate<ARROW_ARRAY>(array->Value(cell_counter));
  tm date{};

  GetTimeForMillisSinceEpoch(date, value);

  buffer[cell_counter].year = 1900 + (date.tm_year);
  buffer[cell_counter].month = date.tm_mon + 1;
  buffer[cell_counter].day = date.tm_mday;

  if (binding->strlen_buffer) {
    binding->strlen_buffer[cell_counter] = static_cast<ssize_t>(GetCellLength_impl(binding));
  }
}

template <CDataType TARGET_TYPE, typename ARROW_ARRAY>
size_t DateArrayFlightSqlAccessor<TARGET_TYPE, ARROW_ARRAY>::GetCellLength_impl(ColumnBinding *binding) const {
  return sizeof(DATE_STRUCT);
}

template class DateArrayFlightSqlAccessor<odbcabstraction::CDataType_DATE,
                                          Date32Array>;
template class DateArrayFlightSqlAccessor<odbcabstraction::CDataType_DATE,
                                          Date64Array>;

} // namespace flight_sql
} // namespace driver

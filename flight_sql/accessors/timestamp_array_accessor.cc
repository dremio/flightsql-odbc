/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include "timestamp_array_accessor.h"
#include "odbcabstraction/calendar_utils.h"

#include <cmath>
#include <limits>

using namespace arrow;

namespace {
inline int64_t GetConversionToSecondsDivisor(TimeUnit::type unit) {
  int64_t divisor = 1;
  switch (unit) {
    case TimeUnit::SECOND:
      divisor = 1;
      break;
    case TimeUnit::MILLI:
      divisor = driver::flight_sql::MILLI_TO_SECONDS_DIVISOR;
      break;
    case TimeUnit::MICRO:
      divisor = driver::flight_sql::MICRO_TO_SECONDS_DIVISOR;
      break;
    case TimeUnit::NANO:
      divisor = driver::flight_sql::NANO_TO_SECONDS_DIVISOR;
      break;
    default:
      assert(false);
      throw driver::odbcabstraction::DriverException("Unrecognized time unit value: " + std::to_string(unit));
  }
  return divisor;
}

uint32_t CalculateFraction(TimeUnit::type unit, int64_t units_since_epoch) {
  // Convert the given remainder and time unit to nanoseconds
  // since the fraction field on TIMESTAMP_STRUCT is in nanoseconds.
  if (unit == TimeUnit::SECOND)
    return 0;
  
  const int64_t divisor = GetConversionToSecondsDivisor(unit);
  const int64_t nano_divisor = GetConversionToSecondsDivisor(TimeUnit::NANO);

  if (units_since_epoch < 0)
    if (units_since_epoch <= (std::numeric_limits<decltype(units_since_epoch)>::min() + divisor))
      // Prevent trying to derive and add a value larger than INT64_MAX (i.e. the time value at the start of
      // the second which is used to shift the value positive before the modulo operation)) in next statement.
      units_since_epoch += divisor;
    // See below regarding floor division; here we want ceiling division.
    // FIXME this goes poorly (trying to use a value > INT64_MAX when units_since_epoch is
    // less than the smallest multiple of divisor greater than INT64_MIN.
    units_since_epoch += divisor * std::abs((units_since_epoch - (divisor - 1)) / divisor);
  return (units_since_epoch % divisor) * (nano_divisor / divisor);
}
} // namespace

namespace driver {
namespace flight_sql {

using namespace odbcabstraction;

template <CDataType TARGET_TYPE, TimeUnit::type UNIT>
TimestampArrayFlightSqlAccessor<TARGET_TYPE, UNIT>::TimestampArrayFlightSqlAccessor(Array *array)
    : FlightSqlAccessor<TimestampArray, TARGET_TYPE,
                        TimestampArrayFlightSqlAccessor<TARGET_TYPE, UNIT>>(array) {}

template <CDataType TARGET_TYPE, TimeUnit::type UNIT>
RowStatus
TimestampArrayFlightSqlAccessor<TARGET_TYPE, UNIT>::MoveSingleCell_impl(
    ColumnBinding *binding, int64_t arrow_row, int64_t cell_counter,
    int64_t &value_offset, bool update_value_offset,
    odbcabstraction::Diagnostics &diagnostics) {
  // Times less than the minimum integer number of seconds that can be represented
  // for each time unit will not convert correctly.  This is mostly interesting for
  // nanoseconds as timestamps in other units are outside of the accepted range of
  // Gregorian dates.
  auto *buffer = static_cast<TIMESTAMP_STRUCT *>(binding->buffer);

  int64_t value = this->GetArray()->Value(arrow_row);
  const auto divisor = GetConversionToSecondsDivisor(UNIT);
  const auto converted_result_seconds =
    // We want floor division here; C++ will round towards zero
    (value < 0)
    // Floor division: Shift all "fractional" (not a multiple of divisor) values so they round towards
    // zero (and to the same value) along with the "floor" less than them, then add 1 to get back to
    // the floor.  Althernative we could shift negatively by (divisor - 1) but this breaks near
    // INT64_MIN causing underflow..
    ? ((value + 1) / divisor) - 1
    // Towards zero is already floor
    : value / divisor;
  tm timestamp = {0};

  GetTimeForSecondsSinceEpoch(timestamp, converted_result_seconds);

  buffer[cell_counter].year = 1900 + (timestamp.tm_year);
  buffer[cell_counter].month = timestamp.tm_mon + 1;
  buffer[cell_counter].day = timestamp.tm_mday;
  buffer[cell_counter].hour = timestamp.tm_hour;
  buffer[cell_counter].minute = timestamp.tm_min;
  buffer[cell_counter].second = timestamp.tm_sec;
  buffer[cell_counter].fraction = CalculateFraction(UNIT, value);

  if (binding->strlen_buffer) {
    binding->strlen_buffer[cell_counter] = static_cast<ssize_t>(GetCellLength_impl(binding));
  }

  return odbcabstraction::RowStatus_SUCCESS;
}

template <CDataType TARGET_TYPE, TimeUnit::type UNIT>
size_t TimestampArrayFlightSqlAccessor<TARGET_TYPE, UNIT>::GetCellLength_impl(ColumnBinding *binding) const {
  return sizeof(TIMESTAMP_STRUCT);
}

template class TimestampArrayFlightSqlAccessor<odbcabstraction::CDataType_TIMESTAMP, TimeUnit::SECOND>;
template class TimestampArrayFlightSqlAccessor<odbcabstraction::CDataType_TIMESTAMP, TimeUnit::MILLI>;
template class TimestampArrayFlightSqlAccessor<odbcabstraction::CDataType_TIMESTAMP, TimeUnit::MICRO>;
template class TimestampArrayFlightSqlAccessor<odbcabstraction::CDataType_TIMESTAMP, TimeUnit::NANO>;

} // namespace flight_sql
} // namespace driver

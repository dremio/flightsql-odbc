/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include "boolean_array_accessor.h"

namespace driver {
namespace flight_sql {

using namespace arrow;
using namespace odbcabstraction;

template <CDataType TARGET_TYPE>
BooleanArrayFlightSqlAccessor<TARGET_TYPE>::BooleanArrayFlightSqlAccessor(
    Array *array)
    : FlightSqlAccessor<BooleanArray, TARGET_TYPE,
                        BooleanArrayFlightSqlAccessor<TARGET_TYPE>>(array) {}

template <CDataType TARGET_TYPE>
void BooleanArrayFlightSqlAccessor<TARGET_TYPE>::MoveSingleCell_impl(
    ColumnBinding *binding, BooleanArray *array, int64_t i,
    int64_t &value_offset, bool update_value_offset, odbcabstraction::Diagnostics &diagnostics) {
  typedef unsigned char c_type;
  bool value = array->Value(i);

  auto *buffer = static_cast<c_type *>(binding->buffer);
  buffer[i] = value ? 1 : 0;

  if (binding->strlen_buffer) {
    binding->strlen_buffer[i] = static_cast<ssize_t>(GetCellLength_impl(binding));
  }
}

template <CDataType TARGET_TYPE>
size_t BooleanArrayFlightSqlAccessor<TARGET_TYPE>::GetCellLength_impl(ColumnBinding *binding) const {
  return sizeof(unsigned char);
}

template class BooleanArrayFlightSqlAccessor<odbcabstraction::CDataType_BIT>;

} // namespace flight_sql
} // namespace driver

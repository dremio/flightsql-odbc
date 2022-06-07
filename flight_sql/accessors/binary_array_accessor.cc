/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include "binary_array_accessor.h"

#include <arrow/array.h>
#include <algorithm>
#include <cstdint>

namespace driver {
namespace flight_sql {

using namespace arrow;
using namespace odbcabstraction;

namespace {

inline RowStatus MoveSingleCellToBinaryBuffer(ColumnBinding *binding,
                                         BinaryArray *array, int64_t i,
                                         int64_t &value_offset, bool update_value_offset, odbcabstraction::Diagnostics &diagnostics) {
  RowStatus result = odbcabstraction::RowStatus_SUCCESS;

  const char *value = array->Value(i).data();
  size_t size_in_bytes = array->value_length(i);

  size_t remaining_length = static_cast<size_t>(size_in_bytes - value_offset);
  size_t value_length =
      std::min(remaining_length,
               binding->buffer_length);

  auto *byte_buffer = static_cast<unsigned char *>(binding->buffer) +
                      i * binding->buffer_length;
  memcpy(byte_buffer, ((char *)value) + value_offset, value_length);

  if (remaining_length > binding->buffer_length) {
    result = odbcabstraction::RowStatus_SUCCESS_WITH_INFO;
    diagnostics.AddTruncationWarning();
    if (update_value_offset) {
      value_offset += value_length;
    }
  } else if (update_value_offset) {
    value_offset = -1;
  }

  if (binding->strlen_buffer) {
    binding->strlen_buffer[i] = static_cast<ssize_t>(remaining_length);
  }

  return result;
}

} // namespace

template <CDataType TARGET_TYPE>
BinaryArrayFlightSqlAccessor<TARGET_TYPE>::BinaryArrayFlightSqlAccessor(
    Array *array)
    : FlightSqlAccessor<BinaryArray, TARGET_TYPE,
                        BinaryArrayFlightSqlAccessor<TARGET_TYPE>>(array) {}

template <>
RowStatus BinaryArrayFlightSqlAccessor<CDataType_BINARY>::MoveSingleCell_impl(
    ColumnBinding *binding, BinaryArray *array, int64_t i,
    int64_t &value_offset, bool update_value_offset, odbcabstraction::Diagnostics &diagnostics) {
  return MoveSingleCellToBinaryBuffer(binding, array, i, value_offset, update_value_offset, diagnostics);
}

template <CDataType TARGET_TYPE>
size_t BinaryArrayFlightSqlAccessor<TARGET_TYPE>::GetCellLength_impl(ColumnBinding *binding) const {
  return binding->buffer_length;
}

template class BinaryArrayFlightSqlAccessor<odbcabstraction::CDataType_BINARY>;

} // namespace flight_sql
} // namespace driver

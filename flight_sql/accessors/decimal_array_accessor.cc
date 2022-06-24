/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include "decimal_array_accessor.h"

#include <arrow/array.h>
#include <arrow/scalar.h>

namespace driver {
namespace flight_sql {

using namespace arrow;
using namespace odbcabstraction;

template <typename ARROW_ARRAY, CDataType TARGET_TYPE>
DecimalArrayFlightSqlAccessor<ARROW_ARRAY, TARGET_TYPE>::DecimalArrayFlightSqlAccessor(
    Array *array)
    : FlightSqlAccessor<ARROW_ARRAY, TARGET_TYPE,
                        DecimalArrayFlightSqlAccessor<ARROW_ARRAY, TARGET_TYPE>>(array),
      data_type_(std::static_pointer_cast<Decimal128Type>(array->type())) {
}

template <>
RowStatus DecimalArrayFlightSqlAccessor<Decimal128Array, CDataType_NUMERIC>::MoveSingleCell_impl(
    ColumnBinding *binding, Decimal128Array *array, int64_t i,
    int64_t &value_offset, bool update_value_offset, odbcabstraction::Diagnostics &diagnostics) {
  auto result = &(static_cast<NUMERIC_STRUCT *>(binding->buffer)[i]);
  int32_t original_scale = data_type_->scale();

  std::shared_ptr<arrow::Scalar> scalar;
  ThrowIfNotOK(array->GetScalar(i).Value(&scalar));
  const auto &decimal_scalar = internal::checked_pointer_cast<Decimal128Scalar>(scalar);
  Decimal128 value = decimal_scalar->value;
  if (original_scale != binding->scale) {
    const Status &status = value.Rescale(original_scale, binding->scale).Value(&value);
    ThrowIfNotOK(status);
  }
  if (!value.FitsInPrecision(binding->precision)) {
    throw DriverException("Decimal value doesn't fit in precision " + std::to_string(binding->precision));
  }

  uint8_t bytes[16];
  value.ToBytes(bytes);
  memcpy(&result->val, bytes, 16);
  result->precision = static_cast<uint8_t>(binding->precision);
  result->scale = static_cast<int8_t>(binding->scale);

  // If the most significant bit is set this number is negative (sign = 1).
  result->sign = (static_cast<int8_t>(result->val[15]) >> 7) == 1;
  result->precision = data_type_->precision();

  if (binding->strlen_buffer) {
    binding->strlen_buffer[i] = static_cast<ssize_t>(GetCellLength_impl(binding));
  }

  return odbcabstraction::RowStatus_SUCCESS;
}

template <typename ARROW_ARRAY, CDataType TARGET_TYPE>
size_t DecimalArrayFlightSqlAccessor<ARROW_ARRAY, TARGET_TYPE>::GetCellLength_impl(ColumnBinding *binding) const {
  return sizeof(NUMERIC_STRUCT);
}

template class DecimalArrayFlightSqlAccessor<Decimal128Array, odbcabstraction::CDataType_NUMERIC>;

} // namespace flight_sql
} // namespace driver

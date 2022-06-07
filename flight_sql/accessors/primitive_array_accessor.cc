/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include "primitive_array_accessor.h"

namespace driver {
namespace flight_sql {

using namespace arrow;
using namespace odbcabstraction;

template <typename ARROW_ARRAY, CDataType TARGET_TYPE>
PrimitiveArrayFlightSqlAccessor<
    ARROW_ARRAY, TARGET_TYPE>::PrimitiveArrayFlightSqlAccessor(Array *array)
    : FlightSqlAccessor<
          ARROW_ARRAY, TARGET_TYPE,
          PrimitiveArrayFlightSqlAccessor<ARROW_ARRAY, TARGET_TYPE>>(array) {}

template <typename ARROW_ARRAY, CDataType TARGET_TYPE>
size_t
PrimitiveArrayFlightSqlAccessor<ARROW_ARRAY, TARGET_TYPE>::GetColumnarData_impl(
    const std::shared_ptr<ARROW_ARRAY> &sliced_array, ColumnBinding *binding,
    int64_t &value_offset, bool update_value_offset, odbcabstraction::Diagnostics &diagnostics) {
  return CopyFromArrayValuesToBinding<ARROW_ARRAY>(sliced_array, binding);
}

template <typename ARROW_ARRAY, CDataType TARGET_TYPE>
size_t PrimitiveArrayFlightSqlAccessor<ARROW_ARRAY, TARGET_TYPE>::GetCellLength_impl(ColumnBinding *binding) const {
  return sizeof(typename ARROW_ARRAY::TypeClass::c_type);
}

template class PrimitiveArrayFlightSqlAccessor<
    Int64Array, odbcabstraction::CDataType_SBIGINT>;
template class PrimitiveArrayFlightSqlAccessor<
    Int32Array, odbcabstraction::CDataType_SLONG>;
template class PrimitiveArrayFlightSqlAccessor<
    Int16Array, odbcabstraction::CDataType_SSHORT>;
template class PrimitiveArrayFlightSqlAccessor<
    Int8Array, odbcabstraction::CDataType_STINYINT>;
template class PrimitiveArrayFlightSqlAccessor<
    UInt64Array, odbcabstraction::CDataType_UBIGINT>;
template class PrimitiveArrayFlightSqlAccessor<
    UInt32Array, odbcabstraction::CDataType_ULONG>;
template class PrimitiveArrayFlightSqlAccessor<
    UInt16Array, odbcabstraction::CDataType_USHORT>;
template class PrimitiveArrayFlightSqlAccessor<
    UInt8Array, odbcabstraction::CDataType_UTINYINT>;
template class PrimitiveArrayFlightSqlAccessor<
    DoubleArray, odbcabstraction::CDataType_DOUBLE>;
template class PrimitiveArrayFlightSqlAccessor<
    FloatArray, odbcabstraction::CDataType_FLOAT>;

} // namespace flight_sql
} // namespace driver

/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#pragma once

#include "../flight_sql_result_set.h"
#include "common.h"
#include "types.h"
#include <arrow/array.h>
#include <arrow/scalar.h>
#include <odbcabstraction/types.h>

namespace driver {
namespace flight_sql {

using namespace arrow;
using namespace odbcabstraction;

template <typename ARROW_ARRAY, CDataType TARGET_TYPE>
class PrimitiveArrayFlightSqlAccessor
    : public FlightSqlAccessor<
          ARROW_ARRAY, TARGET_TYPE,
          PrimitiveArrayFlightSqlAccessor<ARROW_ARRAY, TARGET_TYPE>> {
public:
  explicit PrimitiveArrayFlightSqlAccessor(Array *array);

  size_t GetColumnarData_impl(ColumnBinding *binding, int64_t starting_row, int64_t cells,
                              int64_t &value_offset, bool update_value_offset,
                              odbcabstraction::Diagnostics &diagnostics, uint16_t* row_status_array);

  size_t GetCellLength_impl(ColumnBinding *binding) const;
};

} // namespace flight_sql
} // namespace driver

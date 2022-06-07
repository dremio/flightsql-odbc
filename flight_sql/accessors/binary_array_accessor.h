/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#pragma once

#include "arrow/type_fwd.h"
#include "types.h"
#include <odbcabstraction/types.h>

namespace driver {
namespace flight_sql {

using namespace arrow;
using namespace odbcabstraction;

template <CDataType TARGET_TYPE>
class BinaryArrayFlightSqlAccessor
    : public FlightSqlAccessor<BinaryArray, TARGET_TYPE,
                               BinaryArrayFlightSqlAccessor<TARGET_TYPE>> {
public:
  explicit BinaryArrayFlightSqlAccessor(Array *array);

  void MoveSingleCell_impl(ColumnBinding *binding, BinaryArray *array,
                           int64_t i, int64_t &value_offset, bool update_value_offset, odbcabstraction::Diagnostics &diagnostics);

  size_t GetCellLength_impl(ColumnBinding *binding) const;
};

} // namespace flight_sql
} // namespace driver

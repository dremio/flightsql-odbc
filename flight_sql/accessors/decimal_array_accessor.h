/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#pragma once

#include "arrow/type_fwd.h"
#include "types.h"
#include "utils.h"
#include <locale>
#include <odbcabstraction/types.h>

namespace driver {
namespace flight_sql {

using namespace arrow;
using namespace odbcabstraction;

template <typename ARROW_ARRAY, CDataType TARGET_TYPE>
class DecimalArrayFlightSqlAccessor
    : public FlightSqlAccessor<ARROW_ARRAY, TARGET_TYPE,
                               DecimalArrayFlightSqlAccessor<ARROW_ARRAY, TARGET_TYPE>> {
public:
  explicit DecimalArrayFlightSqlAccessor(Array *array);

  RowStatus MoveSingleCell_impl(ColumnBinding *binding, int64_t arrow_row, int64_t i,
                                int64_t &value_offset, bool update_value_offset,
                                odbcabstraction::Diagnostics &diagnostics);

  size_t GetCellLength_impl(ColumnBinding *binding) const;

private:
  Decimal128Type *data_type_;
};

} // namespace flight_sql
} // namespace driver

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

template <CDataType TARGET_TYPE>
class StringArrayFlightSqlAccessor
    : public FlightSqlAccessor<StringArray, TARGET_TYPE,
                               StringArrayFlightSqlAccessor<TARGET_TYPE>> {
public:
  explicit StringArrayFlightSqlAccessor(Array *array);

  RowStatus MoveSingleCell_impl(ColumnBinding *binding, StringArray *array,
                           int64_t i, int64_t &value_offset, bool update_value_offset,
                           odbcabstraction::Diagnostics &diagnostics);

  size_t GetCellLength_impl(ColumnBinding *binding) const;

private:
  CharToWStrConverter converter_;
};

} // namespace flight_sql
} // namespace driver

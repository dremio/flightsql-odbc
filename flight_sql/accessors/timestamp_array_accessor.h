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
class TimestampArrayFlightSqlAccessor
    : public FlightSqlAccessor<TimestampArray, TARGET_TYPE,
                               TimestampArrayFlightSqlAccessor<TARGET_TYPE>> {

public:
  explicit TimestampArrayFlightSqlAccessor(Array *array);

  RowStatus MoveSingleCell_impl(ColumnBinding *binding, TimestampArray *array, int64_t cell_counter,
                           int64_t &value_offset, bool update_value_offset,
                           odbcabstraction::Diagnostics &diagnostics);

  size_t GetCellLength_impl(ColumnBinding *binding) const;

private:
  std::shared_ptr<TimestampType> timestamp_type_;
};
} // namespace flight_sql
} // namespace driver

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
#include <odbcabstraction/encoding.h>

namespace driver {
namespace flight_sql {

using namespace arrow;
using namespace odbcabstraction;

template <CDataType TARGET_TYPE, typename CHAR_TYPE>
class StringArrayFlightSqlAccessor
    : public FlightSqlAccessor<StringArray, TARGET_TYPE,
                               StringArrayFlightSqlAccessor<TARGET_TYPE, CHAR_TYPE>> {
public:
  explicit StringArrayFlightSqlAccessor(Array *array);

  RowStatus MoveSingleCell_impl(
      ColumnBinding *binding, int64_t arrow_row, int64_t i, int64_t &value_offset,
      bool update_value_offset, odbcabstraction::Diagnostics &diagnostics);

  size_t GetCellLength_impl(ColumnBinding *binding) const;

private:
  std::vector<uint8_t> buffer_;
};

inline Accessor* CreateWCharStringArrayAccessor(arrow::Array *array) {
  switch(GetSqlWCharSize()) {
    case sizeof(char16_t):
      return new StringArrayFlightSqlAccessor<CDataType_WCHAR, char16_t>(array);
    case sizeof(char32_t):
      return new StringArrayFlightSqlAccessor<CDataType_WCHAR, char32_t>(array);
    default:
      assert(false);
      throw DriverException("Encoding is unsupported, SQLWCHAR size: " + std::to_string(GetSqlWCharSize()));
  }
}

} // namespace flight_sql
} // namespace driver

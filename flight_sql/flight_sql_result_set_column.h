/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#pragma once

#include <accessors/types.h>
#include <arrow/array.h>

namespace driver {
namespace flight_sql {

using arrow::Array;

class FlightSqlResultSet;

class FlightSqlResultSetColumn {
private:
  FlightSqlResultSet *result_set_;
  int column_n_;

  // TODO: Figure out if that's the best way of caching
  Array *cached_original_array_;
  std::shared_ptr<Array> cached_casted_array_;
  std::unique_ptr<Accessor> cached_accessor_;

  std::unique_ptr<Accessor> CreateAccessor(CDataType target_type);

  Accessor *GetAccessorForTargetType(CDataType target_type);

public:
  FlightSqlResultSetColumn();

  FlightSqlResultSetColumn(FlightSqlResultSet *result_set, int column_n);

  ColumnBinding binding;
  bool is_bound;

  Accessor *GetAccessorForBinding();

  Accessor *GetAccessorForGetData(CDataType target_type);

  void SetBinding(ColumnBinding new_binding);

  void ResetBinding();

  void ResetAccessor();
};
} // namespace flight_sql
} // namespace driver

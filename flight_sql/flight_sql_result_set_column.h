// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

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

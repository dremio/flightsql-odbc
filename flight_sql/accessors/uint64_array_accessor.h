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

template <CDataType TARGET_TYPE>
class UInt64ArrayFlightSqlAccessor
    : public FlightSqlAccessor<UInt64Array, TARGET_TYPE,
                               UInt64ArrayFlightSqlAccessor<TARGET_TYPE>> {
public:
  explicit UInt64ArrayFlightSqlAccessor(Array *array);

  size_t GetColumnarData_impl(const std::shared_ptr<UInt64Array> &sliced_array,
                              ColumnBinding *binding, int64_t value_offset);
  void MoveSingleCell_impl(ColumnBinding *binding, UInt64Array *array,
                           int64_t i, int64_t value_offset);
};

} // namespace flight_sql
} // namespace driver

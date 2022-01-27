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
#include "types.h"
#include <arrow/array.h>
#include <odbcabstraction/types.h>

namespace driver {
namespace flight_sql {

using arrow::Array;
using odbcabstraction::DataType;

template <typename ARROW_ARRAY, DataType TARGET_TYPE>
class FlightSqlAccessor : public Accessor {
public:
  DataType GetTargetType() override { return TARGET_TYPE; }

  size_t GetColumnarData(FlightSqlResultSet *result_set, ColumnBinding *binding,
                         int64_t starting_row, size_t cells,
                         int64_t value_offset) override {
    const std::shared_ptr<Array> &array =
        result_set->GetArrayForColumn(binding->column)
            ->Slice(starting_row, cells);
    for (int64_t i = 0; i < cells; ++i) {
      if (array->IsNull(i)) {
        binding->strlen_buffer[i] = odbcabstraction::NULL_DATA;
        continue;
      }

      MoveSingleCell<ARROW_ARRAY, TARGET_TYPE>(
          binding, reinterpret_cast<ARROW_ARRAY *>(array.get()), i,
          value_offset);
    }

    return cells;
  }
};

} // namespace flight_sql
} // namespace driver

#include "primitive_array_accessor.h"
#include "string_array_accessor.h"

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
using odbcabstraction::CDataType;

template <typename ARROW_ARRAY, CDataType TARGET_TYPE>
class FlightSqlAccessor : public Accessor {
public:
  CDataType GetTargetType() override { return TARGET_TYPE; }

  size_t GetColumnarData(FlightSqlResultSet *result_set, ColumnBinding *binding,
                         int64_t starting_row, size_t cells,
                         int64_t value_offset) override {
    const std::shared_ptr<Array> &array =
        result_set->GetArrayForColumn(binding->column)
            ->Slice(starting_row, cells);
    for (int64_t i = 0; i < cells; ++i) {
      if (array->IsNull(i)) {
        if (binding->strlen_buffer) {
          binding->strlen_buffer[i] = odbcabstraction::NULL_DATA;
        } else {
          // TODO: Report error when data is null bor strlen_buffer is nullptr
        }
        continue;
      }

      MoveSingleCell(binding, reinterpret_cast<ARROW_ARRAY *>(array.get()), i,
                     value_offset);
    }

    return cells;
  }

private:
  inline void MoveSingleCell(ColumnBinding *binding, ARROW_ARRAY *array,
                             int64_t i, int64_t value_offset) {
    std::stringstream ss;
    ss << "Unknown type conversion from " << typeid(ARROW_ARRAY).name()
       << " to target C type " << TARGET_TYPE;
    throw odbcabstraction::DriverException(ss.str());
  }
};

} // namespace flight_sql
} // namespace driver

#include "primitive_array_accessor.h"
#include "string_array_accessor.h"

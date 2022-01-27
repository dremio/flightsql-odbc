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

#include "types.h"
#include <arrow/array.h>
#include <odbcabstraction/types.h>

namespace driver {
namespace flight_sql {

#define DECLARE_MEMCPY_ACCESS(TYPE, TARGET_TYPE)                               \
  template <>                                                                  \
  inline size_t                                                                \
  MyGetColumnarData<arrow::TYPE##Type::type_id, TARGET_TYPE>::GetColumnarData( \
      FlightSqlResultSet *result_set, ColumnBinding *binding,                  \
      int64_t starting_row, size_t cells, int64_t value_offset) {              \
    const std::shared_ptr<Array> &array =                                      \
        result_set->GetArrayForColumn(binding->column)                         \
            ->Slice(starting_row, cells);                                      \
    auto *typed_array = reinterpret_cast<arrow::TYPE##Array *>(array.get());   \
    size_t element_size = sizeof(arrow::TYPE##Array::value_type);              \
                                                                               \
    for (int64_t i = 0; i < cells; ++i) {                                      \
      if (array->IsNull(i)) {                                                  \
        binding->strlen_buffer[i] = odbcabstraction::NULL_DATA;                \
      } else {                                                                 \
        binding->strlen_buffer[i] = element_size;                              \
      }                                                                        \
    }                                                                          \
                                                                               \
    const auto *values = typed_array->raw_values();                            \
    size_t value_length = std::min(static_cast<size_t>(typed_array->length()), \
                                   binding->buffer_length);                    \
    memcpy(binding->buffer, values, element_size *value_length);               \
                                                                               \
    return cells;                                                              \
  }

DECLARE_MEMCPY_ACCESS(Int64, odbcabstraction::BIGINT)
DECLARE_MEMCPY_ACCESS(Int32, odbcabstraction::INTEGER)
DECLARE_MEMCPY_ACCESS(Int16, odbcabstraction::SMALLINT)
DECLARE_MEMCPY_ACCESS(Int8, odbcabstraction::TINYINT)
DECLARE_MEMCPY_ACCESS(Float, odbcabstraction::FLOAT)
DECLARE_MEMCPY_ACCESS(Double, odbcabstraction::DOUBLE)

} // namespace flight_sql
} // namespace driver

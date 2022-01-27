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

template <arrow::Type::type SOURCE_TYPE, DataType TARGET_TYPE>
inline void MoveSingleCell(ColumnBinding *binding, arrow::Array *array,
                           int64_t i, int64_t value_offset) {
  throw odbcabstraction::DriverException("Unknown type conversion");
}

template <arrow::Type::type SOURCE_TYPE, DataType TARGET_TYPE>
class MyGetColumnarData : public Accessor {
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

      MoveSingleCell<SOURCE_TYPE, TARGET_TYPE>(binding, array.get(), i,
                                               value_offset);
    }

    return cells;
  }
};

template <>
inline void MoveSingleCell<arrow::Type::STRING, odbcabstraction::VARCHAR>(
    ColumnBinding *binding, arrow::Array *array, int64_t i,
    int64_t value_offset) {
  auto *string_array = reinterpret_cast<arrow::StringArray *>(array);
  // TODO: Handle truncation
  size_t value_length = std::min(
      static_cast<size_t>(string_array->value_length(i) - value_offset),
      binding->buffer_length);
  const char *value = string_array->Value(i).data();

  char *char_buffer = static_cast<char *>(binding->buffer);
  memcpy(&char_buffer[i * binding->buffer_length], value + value_offset,
         value_length);
  if (value_length + 1 < binding->buffer_length) {
    char_buffer[i * binding->buffer_length + value_length] = '\0';
  }
  binding->strlen_buffer[i] = string_array->value_length(i) + 1;
}

template <>
inline void MoveSingleCell<arrow::Type::STRING, odbcabstraction::BIGINT>(
    ColumnBinding *binding, arrow::Array *array, int64_t i,
    int64_t value_offset) {
  auto *string_array = reinterpret_cast<arrow::StringArray *>(array);
  static_cast<int64_t *>(binding->buffer)[i] =
      atoll(string_array->GetView(i).data());
  binding->strlen_buffer[i] = sizeof(int64_t);
}

template <>
inline void MoveSingleCell<arrow::Type::STRING, odbcabstraction::INTEGER>(
    ColumnBinding *binding, arrow::Array *array, int64_t i,
    int64_t value_offset) {
  auto *string_array = reinterpret_cast<arrow::StringArray *>(array);
  static_cast<int32_t *>(binding->buffer)[i] =
      atoi(string_array->Value(i).data());
  binding->strlen_buffer[i] = sizeof(int32_t);
}

template <>
inline void MoveSingleCell<arrow::Type::STRING, odbcabstraction::SMALLINT>(
    ColumnBinding *binding, arrow::Array *array, int64_t i,
    int64_t value_offset) {
  auto *string_array = reinterpret_cast<arrow::StringArray *>(array);
  static_cast<int16_t *>(binding->buffer)[i] =
      static_cast<int16_t>(atoi(string_array->Value(i).data()));
  binding->strlen_buffer[i] = sizeof(int16_t);
}

template <>
inline void MoveSingleCell<arrow::Type::STRING, odbcabstraction::TINYINT>(
    ColumnBinding *binding, arrow::Array *array, int64_t i,
    int64_t value_offset) {
  auto *string_array = reinterpret_cast<arrow::StringArray *>(array);
  static_cast<int8_t *>(binding->buffer)[i] =
      static_cast<int8_t>(atoi(string_array->Value(i).data()));
  binding->strlen_buffer[i] = sizeof(int8_t);
}

// TODO: Add other data type conversions

} // namespace flight_sql
} // namespace driver

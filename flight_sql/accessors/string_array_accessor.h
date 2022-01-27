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

template <DataType TARGET_TYPE> class StringArrayAccessor : public Accessor {
private:
  FlightSqlResultSet *result_set_;

  inline void MoveSingleCell(ColumnBinding *binding,
                             arrow::StringArray *string_array, int64_t i) {
    throw odbcabstraction::DriverException("Unknown type conversion");
  }

public:
  StringArrayAccessor(FlightSqlResultSet *result_set)
      : result_set_(result_set) {}

  size_t GetColumnarData(ColumnBinding *binding, size_t cells,
                         int64_t value_offset) {
    int64_t start_offset = result_set_->GetCurrentRow();
    const std::shared_ptr<Array> &array =
        result_set_->GetArrayForColumn(binding->column_)->Slice(start_offset);
    auto *string_array = reinterpret_cast<arrow::StringArray *>(array.get());

    for (int64_t i = 0; i < cells; ++i) {
      if (array->IsNull(i)) {
        binding->strlen_buffer_[i] = odbcabstraction::NULL_DATA;
        continue;
      }

      this->MoveSingleCell(binding, string_array, i);
    }

    return cells;
  }
};

template <>
inline void StringArrayAccessor<odbcabstraction::VARCHAR>::MoveSingleCell(
    ColumnBinding *binding, arrow::StringArray *string_array, int64_t i) {
  const uint8_t *string_array_data = string_array->value_data()->data();
  int value_offset = string_array->value_offset(i);

  // TODO: Handle truncation
  size_t value_length =
      std::min(static_cast<size_t>(string_array->value_length(i)),
               binding->buffer_length_);

  char *char_buffer = static_cast<char *>(binding->buffer_);
  memcpy(&char_buffer[i * binding->buffer_length_],
         &string_array_data[value_offset], value_length);
  char_buffer[i * binding->buffer_length_ + value_length] = '\0';
  binding->strlen_buffer_[i] = value_length + 1;
}

template <>
inline void StringArrayAccessor<odbcabstraction::BIGINT>::MoveSingleCell(
    ColumnBinding *binding, arrow::StringArray *string_array, int64_t i) {
  static_cast<int64_t *>(binding->buffer_)[i] =
      stol(string_array->GetString(i));
  binding->strlen_buffer_[i] = sizeof(int64_t);
}

template <>
inline void StringArrayAccessor<odbcabstraction::INTEGER>::MoveSingleCell(
    ColumnBinding *binding, arrow::StringArray *string_array, int64_t i) {
  static_cast<int32_t *>(binding->buffer_)[i] =
      stoi(string_array->GetString(i));
  binding->strlen_buffer_[i] = sizeof(int32_t);
}

template <>
inline void StringArrayAccessor<odbcabstraction::SMALLINT>::MoveSingleCell(
    ColumnBinding *binding, arrow::StringArray *string_array, int64_t i) {
  static_cast<int16_t *>(binding->buffer_)[i] =
      (int16_t)stoi(string_array->GetString(i));
  binding->strlen_buffer_[i] = sizeof(int16_t);
}

template <>
inline void StringArrayAccessor<odbcabstraction::TINYINT>::MoveSingleCell(
    ColumnBinding *binding, arrow::StringArray *string_array, int64_t i) {
  static_cast<int8_t *>(binding->buffer_)[i] =
      (int8_t)stoi(string_array->GetString(i));
  binding->strlen_buffer_[i] = sizeof(int8_t);
}

// TODO: Add other data type conversions

} // namespace flight_sql
} // namespace driver

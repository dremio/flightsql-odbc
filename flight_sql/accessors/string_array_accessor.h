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
#include <arrow/array.h>
#include <odbcabstraction/columnar_result_set.h>
#include <odbcabstraction/types.h>

namespace driver {
namespace flight_sql {

using arrow::Array;
using odbcabstraction::DataType;
using odbcabstraction::TypedAccessor;

template <DataType TARGET_TYPE>
class StringArrayAccessor
    : public TypedAccessor<StringArrayAccessor<TARGET_TYPE>, TARGET_TYPE> {
private:
  FlightSqlResultSet *result_set_;
  int column_;
  int precision_;
  int scale_;
  void *buffer_;
  size_t buffer_length_;
  size_t *strlen_buffer_;

public:
  StringArrayAccessor(FlightSqlResultSet *result_set, int column,
                      DataType target_type, int precision, int scale,
                      void *buffer, size_t buffer_length, size_t *strlen_buffer)
      : result_set_(result_set), column_(column), precision_(precision),
        scale_(scale), buffer_(buffer), buffer_length_(buffer_length),
        strlen_buffer_(strlen_buffer) {}

  size_t Move_VARCHAR(size_t cells) {
    const std::shared_ptr<Array> &array =
        result_set_->GetArrayForColumn(column_);
    auto *string_array = reinterpret_cast<arrow::StringArray *>(array.get());
    const uint8_t *string_array_data = string_array->value_data()->data();

    char *char_buffer = static_cast<char *>(buffer_);

    int64_t start_offset = result_set_->GetCurrentRow();
    for (int64_t i = 0; i < cells; ++i) {
      if (string_array->IsNull(start_offset + i)) {
        strlen_buffer_[i] = odbcabstraction::NULL_DATA;
        continue;
      }

      int value_offset = string_array->value_offset(start_offset + i);

      // TODO: Handle truncation
      size_t value_length = std::min(
          static_cast<size_t>(string_array->value_length(start_offset + i)),
          buffer_length_);

      memcpy(&char_buffer[i * buffer_length_], &string_array_data[value_offset],
             value_length);
      char_buffer[i * buffer_length_ + value_length] = '\0';
      strlen_buffer_[i] = value_length;
    }

    return cells;
  }

  size_t Move_BIGINT(size_t cells) {
    const std::shared_ptr<Array> &array =
        result_set_->GetArrayForColumn(column_);
    auto *string_array = reinterpret_cast<arrow::StringArray *>(array.get());

    long *long_buffer = static_cast<long *>(buffer_);

    int64_t start_offset = result_set_->GetCurrentRow();
    for (int64_t i = 0; i < cells; ++i) {
      if (string_array->IsNull(start_offset + i)) {
        strlen_buffer_[i] = odbcabstraction::NULL_DATA;
        continue;
      }

      long_buffer[i] = stol(string_array->GetString(start_offset + i));
      strlen_buffer_[i] = sizeof(long);
    }

    return cells;
  }
};

} // namespace flight_sql
} // namespace driver

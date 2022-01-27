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
#include <arrow/scalar.h>
#include <odbcabstraction/types.h>

namespace driver {
namespace flight_sql {

using namespace arrow;
using namespace odbcabstraction;

template <typename ARRAY_TYPE>
inline size_t CopyFromArrayValuesToBinding(FlightSqlResultSet *result_set,
                                           ColumnBinding *binding,
                                           int64_t starting_row, size_t cells) {
  const std::shared_ptr<Array> &array =
      result_set->GetArrayForColumn(binding->column)
          ->Slice(starting_row, static_cast<int64_t>(cells));
  auto *typed_array = reinterpret_cast<ARRAY_TYPE *>(array.get());
  ssize_t element_size = sizeof(typename ARRAY_TYPE::value_type);

  for (int64_t i = 0; i < cells; ++i) {
    if (array->IsNull(i)) {
      binding->strlen_buffer[i] = NULL_DATA;
    } else {
      binding->strlen_buffer[i] = element_size;
    }
  }

  const auto *values = typed_array->raw_values();
  size_t value_length = std::min(static_cast<size_t>(typed_array->length()),
                                 binding->buffer_length);
  memcpy(binding->buffer, values, element_size * value_length);

  return cells;
}

inline void MoveToCharBuffer(ColumnBinding *binding, Array *array, int64_t i,
                             int64_t value_offset) {
  const std::shared_ptr<Scalar> &scalar = array->GetScalar(i).ValueOrDie();
  const std::shared_ptr<StringScalar> &utf8_scalar =
      internal::checked_pointer_cast<StringScalar>(
          scalar->CastTo(utf8()).ValueOrDie());

  const uint8_t *value = utf8_scalar->value->data();

  size_t value_length =
      std::min(static_cast<size_t>(utf8_scalar->value->size() - value_offset),
               binding->buffer_length);

  char *char_buffer = static_cast<char *>(binding->buffer);
  memcpy(&char_buffer[i * binding->buffer_length], value + value_offset,
         value_length);
  if (value_length + 1 < binding->buffer_length) {
    char_buffer[i * binding->buffer_length + value_length] = '\0';
  }
  binding->strlen_buffer[i] = utf8_scalar->value->size() + 1;
}

} // namespace flight_sql
} // namespace driver

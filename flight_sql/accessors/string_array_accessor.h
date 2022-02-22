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

#include "arrow/type_fwd.h"
#include "types.h"
#include <arrow/array.h>
#include <iostream>
#include <odbcabstraction/types.h>

namespace driver {
namespace flight_sql {

using namespace arrow;
using namespace odbcabstraction;

template <>
inline void FlightSqlAccessor<StringArray, CDataType_CHAR>::MoveSingleCell(
    ColumnBinding *binding, StringArray *array, int64_t i,
    int64_t value_offset) {
  // TODO: Handle truncation
  size_t value_length =
      std::min(static_cast<size_t>(array->value_length(i) - value_offset),
               binding->buffer_length);
  const char *value = array->Value(i).data();

  char *char_buffer = static_cast<char *>(binding->buffer);
  memcpy(&char_buffer[i * binding->buffer_length], value + value_offset,
         value_length);
  if (value_length + 1 < binding->buffer_length) {
    char_buffer[i * binding->buffer_length + value_length] = '\0';
  }

  if (binding->strlen_buffer) {
    binding->strlen_buffer[i] = array->value_length(i) + 1;
  }
}

} // namespace flight_sql
} // namespace driver

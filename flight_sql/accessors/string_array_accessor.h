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

template <>
inline void FlightSqlAccessor<StringArray, CDataType_SBIGINT>::MoveSingleCell(
    ColumnBinding *binding, StringArray *array, int64_t i,
    int64_t value_offset) {
  char *end;
  static_cast<int64_t *>(binding->buffer)[i] =
      strtoll(array->GetView(i).data(), &end, 10);
  if (*end) {
    // TODO: Report error
  }

  if (binding->strlen_buffer) {
    binding->strlen_buffer[i] = sizeof(int64_t);
  }
}

template <>
inline void FlightSqlAccessor<StringArray, CDataType_UBIGINT>::MoveSingleCell(
    ColumnBinding *binding, StringArray *array, int64_t i,
    int64_t value_offset) {
  char *end;
  static_cast<uint64_t *>(binding->buffer)[i] =
      strtoull(array->GetView(i).data(), &end, 10);
  if (*end) {
    // TODO: Report error
  }

  if (binding->strlen_buffer) {
    binding->strlen_buffer[i] = sizeof(uint64_t);
  }
}

template <>
inline void FlightSqlAccessor<StringArray, CDataType_SLONG>::MoveSingleCell(
    ColumnBinding *binding, StringArray *array, int64_t i,
    int64_t value_offset) {
  char *end;
  static_cast<int32_t *>(binding->buffer)[i] =
      strtol(array->GetView(i).data(), &end, 10);
  if (*end) {
    // TODO: Report error
  }

  if (binding->strlen_buffer) {
    binding->strlen_buffer[i] = sizeof(int32_t);
  }
}

template <>
inline void FlightSqlAccessor<StringArray, CDataType_ULONG>::MoveSingleCell(
    ColumnBinding *binding, StringArray *array, int64_t i,
    int64_t value_offset) {
  char *end;
  static_cast<int32_t *>(binding->buffer)[i] =
      strtoul(array->GetView(i).data(), &end, 10);
  if (*end) {
    // TODO: Report error
  }

  if (binding->strlen_buffer) {
    binding->strlen_buffer[i] = sizeof(int32_t);
  }
}

template <>
inline void FlightSqlAccessor<StringArray, CDataType_SSHORT>::MoveSingleCell(
    ColumnBinding *binding, StringArray *array, int64_t i,
    int64_t value_offset) {
  char *end;
  static_cast<int16_t *>(binding->buffer)[i] =
      strtol(array->GetView(i).data(), &end, 10);
  if (*end) {
    // TODO: Report error
  }

  if (binding->strlen_buffer) {
    binding->strlen_buffer[i] = sizeof(int16_t);
  }
}

template <>
inline void FlightSqlAccessor<StringArray, CDataType_USHORT>::MoveSingleCell(
    ColumnBinding *binding, StringArray *array, int64_t i,
    int64_t value_offset) {
  char *end;
  static_cast<uint16_t *>(binding->buffer)[i] =
      strtoul(array->GetView(i).data(), &end, 10);
  if (*end) {
    // TODO: Report error
  }

  if (binding->strlen_buffer) {
    binding->strlen_buffer[i] = sizeof(uint16_t);
  }
}

template <>
inline void FlightSqlAccessor<StringArray, CDataType_STINYINT>::MoveSingleCell(
    ColumnBinding *binding, StringArray *array, int64_t i,
    int64_t value_offset) {
  char *end;
  static_cast<int8_t *>(binding->buffer)[i] =
      strtol(array->GetView(i).data(), &end, 10);
  if (*end) {
    // TODO: Report error
  }

  if (binding->strlen_buffer) {
    binding->strlen_buffer[i] = sizeof(int8_t);
  }
}

template <>
inline void FlightSqlAccessor<StringArray, CDataType_UTINYINT>::MoveSingleCell(
    ColumnBinding *binding, StringArray *array, int64_t i,
    int64_t value_offset) {
  char *end;
  static_cast<uint8_t *>(binding->buffer)[i] =
      strtoul(array->GetView(i).data(), &end, 10);
  if (*end) {
    // TODO: Report error
  }

  if (binding->strlen_buffer) {
    binding->strlen_buffer[i] = sizeof(uint8_t);
  }
}

// TODO: Add other data type conversions

} // namespace flight_sql
} // namespace driver

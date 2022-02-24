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
#include <codecvt>
#include <iostream>
#include <locale>
#include <odbcabstraction/types.h>

#ifdef WITH_IODBC
typedef char32_t SqlWChar;
typedef std::u32string SqlWString;
#else
typedef char16_t SqlWChar;
typedef std::u16string SqlWString;
#endif
typedef std::wstring_convert<std::codecvt_utf8<SqlWChar>, SqlWChar>
    CharToWStrConverter;

namespace driver {
namespace flight_sql {

using namespace arrow;
using namespace odbcabstraction;

namespace {

template <typename CHAR_TYPE>
inline void MoveSingleCellToCharBuffer(ColumnBinding *binding,
                                       StringArray *array, int64_t i,
                                       int64_t value_offset) {

  const char *raw_value = array->Value(i).data();
  const void *value;
  size_t size_in_bytes;
  SqlWString wstr;
  if (sizeof(CHAR_TYPE) > sizeof(char)) {
    wstr = CharToWStrConverter().from_bytes(raw_value,
                                            raw_value + array->value_length(i));
    value = wstr.data();
    size_in_bytes = wstr.size() * sizeof(CHAR_TYPE);
  } else {
    value = raw_value;
    size_in_bytes = array->value_length(i);
  }

  // TODO: Handle truncation
  size_t value_length =
      std::min(static_cast<size_t>(size_in_bytes - value_offset),
               binding->buffer_length);

  auto *byte_buffer =
      static_cast<char *>(binding->buffer) + i * binding->buffer_length;
  auto *char_buffer = (CHAR_TYPE *)byte_buffer;
  memcpy(char_buffer, ((char *)value) + value_offset, value_length);

  // Write a NUL terminator
  if (binding->buffer_length > size_in_bytes + sizeof(CHAR_TYPE)) {
    char_buffer[size_in_bytes / sizeof(CHAR_TYPE)] = '\0';
  } else {
    size_t chars_written = binding->buffer_length / sizeof(CHAR_TYPE);
    // If we failed to even write one char, the buffer is too small to hold a
    // NUL-terminator.
    if (chars_written > 0) {
      char_buffer[(chars_written - 1)] = '\0';
    }
  }

  if (binding->strlen_buffer) {
    binding->strlen_buffer[i] = static_cast<ssize_t>(size_in_bytes);
  }
}

} // namespace

template <>
inline void FlightSqlAccessor<StringArray, CDataType_CHAR>::MoveSingleCell(
    ColumnBinding *binding, StringArray *array, int64_t i,
    int64_t value_offset) {
  MoveSingleCellToCharBuffer<char>(binding, array, i, value_offset);
}

template <>
inline void FlightSqlAccessor<StringArray, CDataType_WCHAR>::MoveSingleCell(
    ColumnBinding *binding, StringArray *array, int64_t i,
    int64_t value_offset) {
  MoveSingleCellToCharBuffer<SqlWChar>(binding, array, i, value_offset);
}

} // namespace flight_sql
} // namespace driver

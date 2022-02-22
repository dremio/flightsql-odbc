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

#include <arrow/array.h>
#include <cstddef>
#include <cstdint>
#include <odbcabstraction/exceptions.h>
#include <odbcabstraction/types.h>
#include <sstream>

namespace driver {
namespace flight_sql {

using arrow::Array;
using odbcabstraction::CDataType;

class FlightSqlResultSet;

struct ColumnBinding {
  void *buffer;
  ssize_t *strlen_buffer;
  size_t buffer_length;
  CDataType target_type;
  int precision;
  int scale;

  ColumnBinding() = default;

  ColumnBinding(CDataType target_type, int precision, int scale, void *buffer,
                size_t buffer_length, ssize_t *strlen_buffer)
      : target_type(target_type), precision(precision), scale(scale),
        buffer(buffer), buffer_length(buffer_length),
        strlen_buffer(strlen_buffer) {}
};

/// \brief Accessor interface meant to provide a way of populating data of a
/// single column to buffers bound by `ColumnarResultSet::BindColumn`.
class Accessor {
public:
  const CDataType target_type_;

  Accessor(CDataType target_type) : target_type_(target_type) {}

  virtual ~Accessor() = default;

  /// \brief Populates next cells
  virtual size_t GetColumnarData(ColumnBinding *binding, int64_t starting_row,
                                 size_t cells, int64_t value_offset) = 0;
};

template <typename ARROW_ARRAY, CDataType TARGET_TYPE>
class FlightSqlAccessor : public Accessor {
public:
  explicit FlightSqlAccessor(Array *array);

  size_t GetColumnarData(ColumnBinding *binding, int64_t starting_row,
                         size_t cells, int64_t value_offset) override;

private:
  ARROW_ARRAY *array_;

  size_t GetColumnarData(const std::shared_ptr<ARROW_ARRAY> &sliced_array,
                         ColumnBinding *binding, int64_t value_offset);

  inline void MoveSingleCell(ColumnBinding *binding, ARROW_ARRAY *array,
                             int64_t i, int64_t value_offset);
};

} // namespace flight_sql
} // namespace driver
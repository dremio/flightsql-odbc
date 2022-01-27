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

#include <cstddef>
#include <cstdint>
#include <odbcabstraction/types.h>
#include <sstream>

namespace driver {
namespace flight_sql {

using odbcabstraction::CDataType;

struct ColumnBinding {
  void *buffer;
  ssize_t *strlen_buffer;
  size_t buffer_length;
  CDataType target_type;
  int column;
  int precision;
  int scale;

  ColumnBinding(int column, CDataType target_type, int precision, int scale,
                void *buffer, size_t buffer_length, ssize_t *strlen_buffer)
      : column(column), target_type(target_type), precision(precision),
        scale(scale), buffer(buffer), buffer_length(buffer_length),
        strlen_buffer(strlen_buffer) {}
};

/// \brief Accessor interface meant to provide a way of populating data of a
/// single column to buffers bound by `ColumnarResultSet::BindColumn`.
class Accessor {
public:
  virtual ~Accessor() = default;

  virtual CDataType GetTargetType() = 0;

  /// \brief Populates next cells
  virtual size_t GetColumnarData(FlightSqlResultSet *result_set,
                                 ColumnBinding *binding, int64_t starting_row,
                                 size_t cells, int64_t value_offset) = 0;
};

} // namespace flight_sql
} // namespace driver
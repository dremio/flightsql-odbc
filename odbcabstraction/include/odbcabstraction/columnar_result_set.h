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

#include <map>
#include <memory>
#include <odbcabstraction/result_set.h>
#include <sql.h>

#pragma once

namespace driver {
namespace odbcabstraction {

class Accessor {
private:
  DataType target_type;

public:
  /// Fetches next cells
  size_t Move(size_t cells) {
    if (target_type == CHAR) {
      return Move_CHAR(cells);
    } else if (target_type == NUMERIC) {
      return Move_NUMERIC(cells);
    } else if (target_type == DECIMAL) {
      return Move_DECIMAL(cells);
    } else if (target_type == INTEGER) {
      return Move_INTEGER(cells);
    } else if (target_type == SMALLINT) {
      return Move_SMALLINT(cells);
    } else if (target_type == FLOAT) {
      return Move_FLOAT(cells);
    } else if (target_type == REAL) {
      return Move_REAL(cells);
    } else if (target_type == DOUBLE) {
      return Move_DOUBLE(cells);
    } else if (target_type == DATETIME) {
      return Move_DATETIME(cells);
    } else if (target_type == VARCHAR) {
      return Move_VARCHAR(cells);
    }
  }

  virtual size_t Move_CHAR(size_t cells) = 0;

  virtual size_t Move_NUMERIC(size_t cells) = 0;

  virtual size_t Move_DECIMAL(size_t cells) = 0;

  virtual size_t Move_INTEGER(size_t cells) = 0;

  virtual size_t Move_SMALLINT(size_t cells) = 0;

  virtual size_t Move_FLOAT(size_t cells) = 0;

  virtual size_t Move_REAL(size_t cells) = 0;

  virtual size_t Move_DOUBLE(size_t cells) = 0;

  virtual size_t Move_DATETIME(size_t cells) = 0;

  virtual size_t Move_VARCHAR(size_t cells) = 0;
};

class AccessorFactory {
public:
  virtual std::unique_ptr<Accessor>
  CreateAccessor(int column, DataType target_type, int precision, int scale,
                 void *buffer, size_t buffer_length, size_t *strlen_buffer,
                 size_t strlen_buffer_len) = 0;
};

class ColumnarResultSet : public ResultSet {
private:
  std::unique_ptr<AccessorFactory> accessor_factory;
  std::map<int, std::unique_ptr<Accessor>> accessors;

public:
  void BindColumn(int column, DataType target_type, int precision, int scale,
                  void *buffer, size_t buffer_length, size_t *strlen_buffer,
                  size_t strlen_buffer_len) {
    accessors[column] = accessor_factory->CreateAccessor(
        column, target_type, precision, scale, buffer, buffer_length,
        strlen_buffer, strlen_buffer_len);
  }

  size_t Move(size_t rows) {
    for (auto it = accessors.begin(); it != accessors.end(); *it++) {
      size_t accessor_rows = it->second->Move(rows);
      if (it == accessors.begin()) rows = accessor_rows;
      else if (accessor_rows < rows) {
        throw std::runtime_error("Expected more rows");
      }
    }
    return rows;
  }
};

} // namespace odbcabstraction
} // namespace driver

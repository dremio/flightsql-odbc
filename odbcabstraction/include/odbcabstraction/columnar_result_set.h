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

#include <memory>
#include <vector>

#include <odbcabstraction/exceptions.h>
#include <odbcabstraction/result_set.h>
#include <odbcabstraction/result_set_metadata.h>

#pragma once

namespace driver {
namespace odbcabstraction {

/// \brief Accessor interface meant to provide a way of populating data of a
/// single column to buffers bound by `ColumnarResultSet::BindColumn`.
class Accessor {
public:
  /// \brief Populates next cells
  virtual size_t Move(size_t cells) = 0;
};

/// \brief Accessor CRTP base class, meant to enable efficient calls to
/// specialized `Move_*` methods for each possible target data type without
/// relying on virtual method calls.
template <typename T, DataType TARGET_TYPE>
class TypedAccessor : public Accessor {
  size_t Move(size_t cells) final {
#define ONE_CASE(TYPE)                                                         \
  if (TARGET_TYPE == TYPE) {                                                   \
    return Move_##TYPE(cells);                                                 \
  } else
    ONE_CASE(UNKNOWN_TYPE)
    ONE_CASE(CHAR)
    ONE_CASE(NUMERIC)
    ONE_CASE(DECIMAL)
    ONE_CASE(INTEGER)
    ONE_CASE(SMALLINT)
    ONE_CASE(FLOAT)
    ONE_CASE(REAL)
    ONE_CASE(DOUBLE)
    ONE_CASE(DATETIME)
    ONE_CASE(DATE)
    ONE_CASE(TIME)
    ONE_CASE(TIMESTAMP)
    ONE_CASE(VARCHAR)
    ONE_CASE(LONGVARCHAR)
    ONE_CASE(BINARY)
    ONE_CASE(VARBINARY)
    ONE_CASE(LONGVARBINARY)
    ONE_CASE(BIGINT)
    ONE_CASE(TINYINT)
    ONE_CASE(BIT)
    throw DriverException("Unknown type");
#undef ONE_CASE
  }

#define ONE_CASE(TYPE)                                                         \
  size_t Move_##TYPE(size_t cells) {                                           \
    return static_cast<T *>(this)->Move_##TYPE(cells);                         \
  }
  ONE_CASE(UNKNOWN_TYPE)
  ONE_CASE(CHAR)
  ONE_CASE(NUMERIC)
  ONE_CASE(DECIMAL)
  ONE_CASE(INTEGER)
  ONE_CASE(SMALLINT)
  ONE_CASE(FLOAT)
  ONE_CASE(REAL)
  ONE_CASE(DOUBLE)
  ONE_CASE(DATETIME)
  ONE_CASE(DATE)
  ONE_CASE(TIME)
  ONE_CASE(TIMESTAMP)
  ONE_CASE(VARCHAR)
  ONE_CASE(LONGVARCHAR)
  ONE_CASE(BINARY)
  ONE_CASE(VARBINARY)
  ONE_CASE(LONGVARBINARY)
  ONE_CASE(BIGINT)
  ONE_CASE(TINYINT)
  ONE_CASE(BIT)
#undef ONE_CASE
};

/// \brief ResultSet specialized for retrieving data in columnar format.
/// This requires a AccessorFactory instance for creating specialized column
/// accessors for each column bound by calling `BindColumn`.
class ColumnarResultSet : public ResultSet {
private:
  std::shared_ptr<ResultSetMetadata> metadata_;

protected:
  ColumnarResultSet(std::shared_ptr<ResultSetMetadata> metadata)
      : metadata_(std::move(metadata)),
        accessors_(metadata_->GetColumnCount()) {}

  /// \brief Creates a Accessor for given column and target data type bound to
  /// given buffers.
  virtual std::unique_ptr<Accessor>
  CreateAccessor(int column, DataType target_type, int precision, int scale,
                 void *buffer, size_t buffer_length, size_t *strlen_buffer) = 0;

  std::vector<std::unique_ptr<Accessor>> accessors_;

public:
  std::shared_ptr<ResultSetMetadata> GetMetadata() override {
    return std::move(metadata_);
  }

  void BindColumn(int column, DataType target_type, int precision, int scale,
                  void *buffer, size_t buffer_length,
                  size_t *strlen_buffer) override {
    if (buffer == nullptr) {
      accessors_[column - 1] = nullptr;
      return;
    }
    accessors_[column - 1] =
        CreateAccessor(column, target_type, precision, scale, buffer,
                       buffer_length, strlen_buffer);
  }

  size_t Move(size_t rows) override {
    size_t fetched_rows = -1;
    for (auto it = accessors_.begin(); it != accessors_.end(); *it++) {
      // There can be unbound columns.
      if (*it == nullptr)
        continue;

      size_t accessor_rows = (*it)->Move(rows);
      if (fetched_rows == -1) {
        // Stores the number of rows moved on the first bound column, expecting
        // that all columns move the same number of rows.
        fetched_rows = accessor_rows;
      } else if (accessor_rows != fetched_rows) {
        throw DriverException(
            "Expected the same number of rows for all columns");
      }
    }
    return fetched_rows;
  }
};

} // namespace odbcabstraction
} // namespace driver

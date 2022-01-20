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
  size_t Move(size_t cells) override {
    if (TARGET_TYPE == CHAR) {
      return Move_CHAR(cells);
    } else if (TARGET_TYPE == NUMERIC) {
      return Move_NUMERIC(cells);
    } else if (TARGET_TYPE == DECIMAL) {
      return Move_DECIMAL(cells);
    } else if (TARGET_TYPE == INTEGER) {
      return Move_INTEGER(cells);
    } else if (TARGET_TYPE == SMALLINT) {
      return Move_SMALLINT(cells);
    } else if (TARGET_TYPE == FLOAT) {
      return Move_FLOAT(cells);
    } else if (TARGET_TYPE == REAL) {
      return Move_REAL(cells);
    } else if (TARGET_TYPE == DOUBLE) {
      return Move_DOUBLE(cells);
    } else if (TARGET_TYPE == DATETIME) {
      return Move_DATETIME(cells);
    } else if (TARGET_TYPE == VARCHAR) {
      return Move_VARCHAR(cells);
    }
  }

  /// \brief Populates bound buffers with next cells as CHAR values.
  size_t Move_CHAR(size_t cells) {
    return static_cast<T *>(this)->Move_CHAR(cells);
  }

  /// \brief Populates bound buffers with next cells as NUMERIC values.
  size_t Move_NUMERIC(size_t cells) {
    return static_cast<T *>(this)->Move_NUMERIC(cells);
  }

  /// \brief Populates bound buffers with next cells as DECIMAL values.
  size_t Move_DECIMAL(size_t cells) {
    return static_cast<T *>(this)->Move_DECIMAL(cells);
  }

  /// \brief Populates bound buffers with next cells as INTEGER values.
  size_t Move_INTEGER(size_t cells) {
    return static_cast<T *>(this)->Move_INTEGER(cells);
  }

  /// \brief Populates bound buffers with next cells as SMALLINT values.
  size_t Move_SMALLINT(size_t cells) {
    return static_cast<T *>(this)->Move_SMALLINT(cells);
  }

  /// \brief Populates bound buffers with next cells as FLOAT values.
  size_t Move_FLOAT(size_t cells) {
    return static_cast<T *>(this)->Move_FLOAT(cells);
  }

  /// \brief Populates bound buffers with next cells as REAL values.
  size_t Move_REAL(size_t cells) {
    return static_cast<T *>(this)->Move_REAL(cells);
  }

  /// \brief Populates bound buffers with next cells as DOUBLE values.
  size_t Move_DOUBLE(size_t cells) {
    return static_cast<T *>(this)->Move_DOUBLE(cells);
  }

  /// \brief Populates bound buffers with next cells as DATETIME values.
  size_t Move_DATETIME(size_t cells) {
    return static_cast<T *>(this)->Move_DATETIME(cells);
  }

  /// \brief Populates bound buffers with next cells as VARCHAR values.
  size_t Move_VARCHAR(size_t cells) {
    return static_cast<T *>(this)->Move_VARCHAR(cells);
  }
};

/// \brief ResultSet specialized for retrieving data in columnar format.
/// This requires a AccessorFactory instance for creating specialized column
/// accessors for each column bound by calling `BindColumn`.
class ColumnarResultSet : public ResultSet {
private:
  std::shared_ptr<ResultSetMetadata> metadata_;
  std::vector<std::unique_ptr<Accessor>> accessors_;

protected:
  ColumnarResultSet(std::shared_ptr<ResultSetMetadata> metadata)
      : metadata_(std::move(metadata)), accessors_(metadata->GetColumnCount()) {
  }

  /// \brief Creates a Accessor for given column and target data type bound to
  /// given buffers.
  virtual std::unique_ptr<Accessor>
  CreateAccessor(int column, DataType target_type, int precision, int scale,
                 void *buffer, size_t buffer_length, size_t *strlen_buffer,
                 size_t strlen_buffer_len) = 0;

public:
  std::shared_ptr<ResultSetMetadata> GetMetadata() override {
    return std::move(metadata_);
  }

  void BindColumn(int column, DataType target_type, int precision, int scale,
                  void *buffer, size_t buffer_length, size_t *strlen_buffer,
                  size_t strlen_buffer_len) override {
    if (buffer == nullptr) {
      accessors_[column] = nullptr;
      return;
    }
    accessors_[column] =
        CreateAccessor(column, target_type, precision, scale, buffer,
                       buffer_length, strlen_buffer, strlen_buffer_len);
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

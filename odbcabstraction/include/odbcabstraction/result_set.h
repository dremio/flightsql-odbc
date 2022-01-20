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
#include <sql.h>

#pragma once

namespace driver {
namespace odbcabstraction {

class ResultSetMetadata;

enum DataType {
  UNKNOWN_TYPE = SQL_UNKNOWN_TYPE,
  CHAR = SQL_CHAR,
  NUMERIC = SQL_NUMERIC,
  DECIMAL = SQL_DECIMAL,
  INTEGER = SQL_INTEGER,
  SMALLINT = SQL_SMALLINT,
  FLOAT = SQL_FLOAT,
  REAL = SQL_REAL,
  DOUBLE = SQL_DOUBLE,
  DATETIME = SQL_DATETIME,
  VARCHAR = SQL_VARCHAR,
};

class ResultSet {
public:
  /// \brief Returns metadata for this ResultSet.
  virtual std::shared_ptr<ResultSetMetadata> GetMetadata() = 0;

  /// \brief Closes ResultSet, releasing any resources allocated by it.
  virtual void Close() = 0;

  /// \brief Binds a column with a result buffer. The buffer will be filled with
  /// up to `GetMaxBatchSize()` values.
  ///
  /// \param column Column number to be bound with (starts from 1).
  /// \param target_type Target data type expected by client.
  /// \param precision Column's precision
  /// \param scale Column's scale
  /// \param buffer Target buffer to be filled with column values.
  /// \param buffer_length Target buffer length.
  /// \param strlen_buffer Buffer that holds the length of each value contained
  /// on target buffer.
  /// \param strlen_buffer_len strlen_buffer's length.
  virtual void BindColumn(int column, DataType target_type, int precision,
                          int scale, void *buffer, size_t buffer_length,
                          size_t *strlen_buffer, size_t strlen_buffer_len) = 0;

  /// \brief Fetches next rows from ResultSet and load values on buffers
  /// previously bound with `BindColumn`.
  ///
  /// The parameters `buffer` and `strlen_buffer` passed to `BindColumn()`
  /// should have capacity to accommodate the rows requested, otherwise data
  /// will be truncated.
  ///
  /// \param rows The maximum number of rows to be fetched.
  /// \returns The number of rows fetched.
  virtual size_t Move(size_t rows) = 0;

  /// \brief Populates `buffer` with the value on current row for given column.
  /// If the value doesn't fit the buffer this method returns true and
  /// subsequent calls will fetch the rest of data.
  ///
  /// \param column Column number to be fetched.
  /// \param target_type Target data type expected by client.
  /// \param precision Column's precision
  /// \param scale Column's scale
  /// \param buffer Target buffer to be populated.
  /// \param buffer_length Target buffer length.
  /// \param strlen_buffer Buffer that holds the length of value being fetched.
  /// \returns true if there is more data to fetch from the current cell;
  ///          false if the whole value was already fetched.
  virtual bool GetData(int column, DataType target_type, int precision,
                       int scale, void *buffer, size_t buffer_length,
                       size_t *strlen_buffer) = 0;
};

template <class T, DataType TARGET_TYPE> class Accessor {
public:
  /// Fetches next cells
  void Move(size_t cells) {
    if (TARGET_TYPE == CHAR) {
      Move_CHAR(cells);
    } else if (TARGET_TYPE == NUMERIC) {
      Move_NUMERIC(cells);
    } else if (TARGET_TYPE == DECIMAL) {
      Move_DECIMAL(cells);
    } else if (TARGET_TYPE == INTEGER) {
      Move_INTEGER(cells);
    } else if (TARGET_TYPE == SMALLINT) {
      Move_SMALLINT(cells);
    } else if (TARGET_TYPE == FLOAT) {
      Move_FLOAT(cells);
    } else if (TARGET_TYPE == REAL) {
      Move_REAL(cells);
    } else if (TARGET_TYPE == DOUBLE) {
      Move_DOUBLE(cells);
    } else if (TARGET_TYPE == DATETIME) {
      Move_DATETIME(cells);
    } else if (TARGET_TYPE == VARCHAR) {
      Move_VARCHAR(cells);
    }
  }

  void Move_CHAR(size_t cells) { static_cast<T *>(this)->Move_CHAR(cells); }

  void Move_NUMERIC(size_t cells) {
    static_cast<T *>(this)->Move_NUMERIC(cells);
  }

  void Move_DECIMAL(size_t cells) {
    static_cast<T *>(this)->Move_DECIMAL(cells);
  }

  void Move_INTEGER(size_t cells) {
    static_cast<T *>(this)->Move_INTEGER(cells);
  }

  void Move_SMALLINT(size_t cells) {
    static_cast<T *>(this)->Move_SMALLINT(cells);
  }

  void Move_FLOAT(size_t cells) { static_cast<T *>(this)->Move_FLOAT(cells); }

  void Move_REAL(size_t cells) { static_cast<T *>(this)->Move_REAL(cells); }

  void Move_DOUBLE(size_t cells) { static_cast<T *>(this)->Move_DOUBLE(cells); }

  void Move_DATETIME(size_t cells) {
    static_cast<T *>(this)->Move_DATETIME(cells);
  }

  void Move_VARCHAR(size_t cells) {
    static_cast<T *>(this)->Move_VARCHAR(cells);
  }
};

} // namespace odbcabstraction
} // namespace driver

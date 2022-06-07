/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#pragma once

#include <map>
#include <memory>

#include <odbcabstraction/platform.h>

#include <odbcabstraction/types.h>

namespace driver {
namespace odbcabstraction {

class ResultSetMetadata;

class ResultSet {
protected:
  ResultSet() = default;

public:
  virtual ~ResultSet() = default;

  /// \brief Returns metadata for this ResultSet.
  virtual std::shared_ptr<ResultSetMetadata> GetMetadata() = 0;

  /// \brief Closes ResultSet, releasing any resources allocated by it.
  virtual void Close() = 0;

  /// \brief Cancels ResultSet.
  virtual void Cancel() = 0;

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
  virtual void BindColumn(int column, int16_t target_type, int precision,
                          int scale, void *buffer, size_t buffer_length,
                          ssize_t *strlen_buffer) = 0;

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
  virtual bool GetData(int column, int16_t target_type, int precision,
                       int scale, void *buffer, size_t buffer_length,
                       ssize_t *strlen_buffer) = 0;
};

} // namespace odbcabstraction
} // namespace driver

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

#include <boost/optional.hpp>

#pragma once

namespace driver {
namespace odbcabstraction {

class ResultSetMetadata;

class ResultSet {
public:
  /**
   * Returns metadata for this ResultSet.
   */
  virtual std::shared_ptr<ResultSetMetadata> GetMetadata() = 0;

  /**
   * Closes ResultSet, releasing any resources allocated by it.
   */
  virtual void Close() = 0;

  /**
   * Binds a column with a result buffer. The buffer will be filled with up to
   * `GetMaxBatchSize()` values.
   *
   * @param column Column number to be bound with (starts from 1).
   * @param target_type Target data type expected by client.
   * @param buffer Target buffer to be filled with column values.
   * @param buffer_length Target buffer length.
   * @param strlen_buffer Buffer that holds the length of each value contained
   * on target buffer.
   * @param strlen_buffer_len strlen_buffer's length. Must have at least
   * `GetMaxBatchSize() * sizeof(size_t)`.
   */
  virtual void BindColumn(int column, TargetType target_type, void *buffer,
                          size_t buffer_length, size_t *strlen_buffer,
                          size_t strlen_buffer_len) = 0;

  /**
   * Fetches next rows from ResultSet and load values on buffers
   * previously bound with `BindColumn`.
   *
   * @param rows The maximum number of rows to be fetched.
   * @return The number of rows fetched.
   */
  virtual size_t Move(size_t rows) = 0;
};

} // namespace odbcabstraction
} // namespace driver

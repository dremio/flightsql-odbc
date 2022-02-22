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

#include "flight_sql_result_set_accessors.h"
#include "flight_sql_stream_chunk_iterator.h"
#include "utils.h"
#include <accessors/types.h>
#include <arrow/array.h>
#include <arrow/datum.h>
#include <arrow/flight/sql/client.h>
#include <arrow/flight/types.h>
#include <iostream>
#include <odbcabstraction/exceptions.h>
#include <odbcabstraction/result_set.h>

namespace driver {
namespace flight_sql {

using arrow::Array;
using arrow::RecordBatch;
using arrow::Schema;
using arrow::Status;
using arrow::flight::FlightEndpoint;
using arrow::flight::FlightInfo;
using arrow::flight::FlightStreamChunk;
using arrow::flight::FlightStreamReader;
using arrow::flight::sql::FlightSqlClient;
using odbcabstraction::CDataType;
using odbcabstraction::DriverException;
using odbcabstraction::ResultSet;
using odbcabstraction::ResultSetMetadata;

class FlightSqlResultSetColumn;

class FlightSqlResultSet : public ResultSet {
private:
  std::vector<FlightSqlResultSetColumn> columns_;
  std::vector<int64_t> get_data_offsets_;

  int num_binding_;
  std::shared_ptr<ResultSetMetadata> metadata_;

  int64_t current_row_;
  FlightStreamChunkIterator chunk_iterator_;
  FlightStreamChunk current_chunk_;
  std::shared_ptr<Schema> schema_;

public:
  ~FlightSqlResultSet() override;

  FlightSqlResultSet(std::shared_ptr<ResultSetMetadata> metadata,
                     FlightSqlClient &flight_sql_client,
                     const arrow::flight::FlightCallOptions &call_options,
                     const std::shared_ptr<FlightInfo> &flight_info);

  void Close() override;

  bool GetData(int column_n, CDataType target_type, int precision, int scale,
               void *buffer, size_t buffer_length,
               ssize_t *strlen_buffer) override;

  size_t Move(size_t rows) override;

  std::shared_ptr<arrow::Array> GetArrayForColumn(int column);

  std::shared_ptr<ResultSetMetadata> GetMetadata() override;

  void BindColumn(int column_n, CDataType target_type, int precision, int scale,
                  void *buffer, size_t buffer_length,
                  ssize_t *strlen_buffer) override;
};

template <typename ARROW_ARRAY, CDataType TARGET_TYPE>
FlightSqlAccessor<ARROW_ARRAY, TARGET_TYPE>::FlightSqlAccessor(Array *array)
    : Accessor(TARGET_TYPE),
      array_(arrow::internal::checked_cast<ARROW_ARRAY *>(array)) {}

template <typename ARROW_ARRAY, CDataType TARGET_TYPE>
size_t FlightSqlAccessor<ARROW_ARRAY, TARGET_TYPE>::GetColumnarData(
    const std::shared_ptr<ARROW_ARRAY> &sliced_array, ColumnBinding *binding,
    int64_t value_offset) {
  int64_t length = sliced_array->length();
  for (int64_t i = 0; i < length; ++i) {
    if (sliced_array->IsNull(i)) {
      if (binding->strlen_buffer) {
        binding->strlen_buffer[i] = odbcabstraction::NULL_DATA;
      } else {
        // TODO: Report error when data is null bor strlen_buffer is nullptr
      }
      continue;
    }

    MoveSingleCell(binding, sliced_array.get(), i, value_offset);
  }

  return length;
}

template <typename ARROW_ARRAY, CDataType TARGET_TYPE>
size_t FlightSqlAccessor<ARROW_ARRAY, TARGET_TYPE>::GetColumnarData(
    ColumnBinding *binding, int64_t starting_row, size_t cells,
    int64_t value_offset) {
  const std::shared_ptr<Array> &array =
      array_->Slice(starting_row, static_cast<int64_t>(cells));

  return GetColumnarData(
      arrow::internal::checked_pointer_cast<ARROW_ARRAY>(array), binding,
      value_offset);
}

template <typename ARROW_ARRAY, CDataType TARGET_TYPE>
void FlightSqlAccessor<ARROW_ARRAY, TARGET_TYPE>::MoveSingleCell(
    ColumnBinding *binding, ARROW_ARRAY *array, int64_t i,
    int64_t value_offset) {
  std::stringstream ss;
  ss << "Unknown type conversion from " << typeid(ARROW_ARRAY).name()
     << " to target C type " << TARGET_TYPE;
  throw odbcabstraction::DriverException(ss.str());
}

} // namespace flight_sql
} // namespace driver

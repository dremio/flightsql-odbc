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

#include "flight_sql_result_set.h"
#include <odbcabstraction/platform.h>

#include <arrow/flight/types.h>
#include <arrow/scalar.h>
#include <iostream>
#include <utility>

#include "flight_sql_result_set_column.h"
#include "flight_sql_result_set_metadata.h"
#include "utils.h"

namespace driver {
namespace flight_sql {

using arrow::Array;
using arrow::RecordBatch;
using arrow::Scalar;
using arrow::Status;
using arrow::flight::FlightEndpoint;
using arrow::flight::FlightStreamChunk;
using arrow::flight::FlightStreamReader;
using odbcabstraction::CDataType;
using odbcabstraction::DriverException;

FlightSqlResultSet::FlightSqlResultSet(
    FlightSqlClient &flight_sql_client,
    const arrow::flight::FlightCallOptions &call_options,
    const std::shared_ptr<FlightInfo> &flight_info,
    const std::shared_ptr<RecordBatchTransformer> &transformer)
    : num_binding_(0), current_row_(0),
      chunk_iterator_(flight_sql_client, call_options, flight_info),
      transformer_(transformer),
      metadata_(transformer ? new FlightSqlResultSetMetadata(
                                  transformer->GetTransformedSchema())
                            : new FlightSqlResultSetMetadata(flight_info)),
      columns_(metadata_->GetColumnCount()),
      get_data_offsets_(metadata_->GetColumnCount()) {
  current_chunk_.data = nullptr;

  for (int i = 0; i < columns_.size(); ++i) {
    columns_[i] = FlightSqlResultSetColumn(this, i + 1);
  }

  ThrowIfNotOK(flight_info->GetSchema(nullptr, &schema_));
}

size_t FlightSqlResultSet::Move(size_t rows) {
  // Consider it might be the first call to Move() and current_chunk is not
  // populated yet
  assert(rows > 0);
  if (current_chunk_.data == nullptr) {
    if (!chunk_iterator_.GetNext(&current_chunk_)) {
      return 0;
    }

    if (transformer_) {
      current_chunk_.data = transformer_->Transform(current_chunk_.data);
    }
  }

  // Reset GetData value offsets.
  if (num_binding_ != get_data_offsets_.size()) {
    std::fill(get_data_offsets_.begin(), get_data_offsets_.end(), 0);
  }

  size_t fetched_rows = 0;
  while (fetched_rows < rows) {
    size_t batch_rows = current_chunk_.data->num_rows();
    size_t rows_to_fetch =
        std::min(static_cast<size_t>(rows - fetched_rows),
                 static_cast<size_t>(batch_rows - current_row_));

    if (rows_to_fetch == 0) {
      if (!chunk_iterator_.GetNext(&current_chunk_)) {
        break;
      }

      if (transformer_) {
        current_chunk_.data = transformer_->Transform(current_chunk_.data);
      }

      for (auto &column : columns_) {
        column.ResetAccessor();
      }
      current_row_ = 0;
      continue;
    }

    for (auto it = columns_.begin(); it != columns_.end(); *it++) {
      auto &column = *it;

      // There can be unbound columns.
      if (!column.is_bound)
        continue;

      size_t accessor_rows = column.GetAccessorForBinding()->GetColumnarData(
          &column.binding, current_row_, rows_to_fetch, 0);

      if (rows_to_fetch != accessor_rows) {
        throw DriverException(
            "Expected the same number of rows for all columns");
      }
    }

    current_row_ += static_cast<int64_t>(rows_to_fetch);
    fetched_rows += rows_to_fetch;
  }

  return fetched_rows;
}

void FlightSqlResultSet::Close() {
  chunk_iterator_.Close();
  current_chunk_.data = nullptr;
}

bool FlightSqlResultSet::GetData(int column_n, CDataType target_type,
                                 int precision, int scale, void *buffer,
                                 size_t buffer_length, ssize_t *strlen_buffer) {
  ColumnBinding binding(target_type, precision, scale, buffer, buffer_length,
                        strlen_buffer);

  auto &column = columns_[column_n - 1];
  Accessor *accessor = column.GetAccessorForGetData(target_type);

  int64_t &value_offset = get_data_offsets_[column_n - 1];

  // Note: current_row_ is always positioned at the index _after_ the one we are
  // on after calling Move(). So if we want to get data from the _last_ row
  // fetched, we need to subtract one from the current row.
  accessor->GetColumnarData(&binding, current_row_ - 1, 1, value_offset);

  if (strlen_buffer && strlen_buffer[0] != odbcabstraction::NULL_DATA) {
    bool has_more = value_offset + buffer_length <= strlen_buffer[0];
    value_offset += static_cast<int64_t>(buffer_length);
    return has_more;
  } else {
    return false;
  }
}

std::shared_ptr<arrow::Array>
FlightSqlResultSet::GetArrayForColumn(int column) {
  std::shared_ptr<Array> original_array =
      current_chunk_.data->column(column - 1);

  return original_array;
}

std::shared_ptr<ResultSetMetadata> FlightSqlResultSet::GetMetadata() {
  return metadata_;
}

void FlightSqlResultSet::BindColumn(int column_n, CDataType target_type,
                                    int precision, int scale, void *buffer,
                                    size_t buffer_length,
                                    ssize_t *strlen_buffer) {
  auto &column = columns_[column_n - 1];
  if (buffer == nullptr) {
    if (column.is_bound) {
      num_binding_--;
    }
    column.ResetBinding();
    return;
  }

  if (!column.is_bound) {
    num_binding_++;
  }

  ColumnBinding binding(target_type, precision, scale, buffer, buffer_length,
                        strlen_buffer);
  column.SetBinding(binding);
}

FlightSqlResultSet::~FlightSqlResultSet() = default;
} // namespace flight_sql
} // namespace driver

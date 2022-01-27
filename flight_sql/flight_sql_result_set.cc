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

#include <arrow/flight/types.h>
#include <arrow/scalar.h>
#include <iostream>
#include <utility>

#include "flight_sql_result_set_accessors.h"
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
using odbcabstraction::DataType;
using odbcabstraction::DriverException;

class FlightStreamChunkIterator {
private:
  std::vector<std::unique_ptr<FlightStreamReader>> stream_readers_;
  std::vector<std::unique_ptr<FlightStreamReader>>::iterator stream_readers_it_;

public:
  FlightStreamChunkIterator(
      FlightSqlClient &flight_sql_client,
      const arrow::flight::FlightCallOptions &call_options,
      const std::shared_ptr<FlightInfo> &flight_info) {
    const std::vector<FlightEndpoint> &vector = flight_info->endpoints();

    stream_readers_.reserve(vector.size());
    for (int i = 0; i < vector.size(); ++i) {
      auto result = flight_sql_client.DoGet(call_options, vector[0].ticket);
      ThrowIfNotOK(result.status());
      stream_readers_.push_back(std::move(result.ValueOrDie()));
    }

    stream_readers_it_ = stream_readers_.begin();
  }

  bool GetNext(FlightStreamChunk *chunk) {
    chunk->data = nullptr;
    while (stream_readers_it_ != stream_readers_.end()) {
      ThrowIfNotOK((*stream_readers_it_)->Next(chunk));
      if (chunk->data != nullptr) {
        return true;
      }
      stream_readers_it_++;
    }
    return false;
  }

  void Close() {
    for (const auto &item : stream_readers_) {
      item->Cancel();
    }
  }
};

FlightSqlResultSet::FlightSqlResultSet(
    std::shared_ptr<ResultSetMetadata> metadata,
    FlightSqlClient &flight_sql_client,
    const arrow::flight::FlightCallOptions &call_options,
    const std::shared_ptr<FlightInfo> &flight_info)
    : metadata_(std::move(metadata)), binding_(metadata->GetColumnCount()),
      accessors_(metadata->GetColumnCount()),
      get_data_offsets_(metadata->GetColumnCount(), 0), current_row_(0) {
  chunk_iterator_.reset(new FlightStreamChunkIterator(
      flight_sql_client, call_options, flight_info));
  current_chunk_.data = nullptr;

  ThrowIfNotOK(flight_info->GetSchema(nullptr, &schema_));
}

size_t FlightSqlResultSet::Move(size_t rows) {
  // Consider it might be the first call to Move() and current_chunk is not
  // populated yet
  if (current_chunk_.data == nullptr) {
    if (!chunk_iterator_->GetNext(&current_chunk_)) {
      return 0;
    }
  }

  // Reset GetData value offsets.
  std::fill(get_data_offsets_.begin(), get_data_offsets_.end(), 0);

  size_t fetched_rows = 0;

  while (fetched_rows < rows) {
    size_t batch_rows = current_chunk_.data->num_rows();
    size_t rows_to_fetch =
        std::min(static_cast<size_t>(rows - fetched_rows), static_cast<size_t>(batch_rows - current_row_));

    if (rows_to_fetch == 0) {
      if (!chunk_iterator_->GetNext(&current_chunk_)) {
        break;
      }
      current_row_ = 0;
      continue;
    }

    for (auto it = binding_.begin(); it != binding_.end(); *it++) {
      // There can be unbound columns.
      ColumnBinding *binding = (*it).get();
      if (binding == nullptr)
        continue;

      Accessor *accessor = GetAccessor(binding->column, binding->target_type);
      size_t accessor_rows =
          accessor->GetColumnarData(this, binding, rows_to_fetch, 0);

      if (rows_to_fetch != accessor_rows) {
        throw DriverException(
            "Expected the same number of rows for all columns");
      }
    }

    current_row_ += rows_to_fetch;
    fetched_rows += rows_to_fetch;
  }

  return fetched_rows;
}

void FlightSqlResultSet::Close() {
  chunk_iterator_->Close();
  current_chunk_.data = nullptr;
}

bool FlightSqlResultSet::GetData(int column,
                                 odbcabstraction::DataType target_type,
                                 int precision, int scale, void *buffer,
                                 size_t buffer_length, size_t *strlen_buffer) {
  ColumnBinding binding(column, target_type, precision, scale, buffer,
                        buffer_length, strlen_buffer);

  const std::shared_ptr<arrow::DataType> &source_type =
      schema_->field(column - 1)->type();
  Accessor *accessor = GetAccessor(column, binding.target_type);

  int64_t *value_offset = &get_data_offsets_[column - 1];
  accessor->GetColumnarData(this, &binding, 1, *value_offset);

  if (strlen_buffer) {
    bool has_more = *value_offset + buffer_length <= strlen_buffer[0];
    *value_offset += buffer_length;
    return has_more;
  } else {
    return false;
  }
}

std::shared_ptr<arrow::Array>
FlightSqlResultSet::GetArrayForColumn(int column) {
  std::shared_ptr<Array> result = current_chunk_.data->column(column - 1);
  return result;
}

int64_t FlightSqlResultSet::GetCurrentRow() { return current_row_; }

std::shared_ptr<ResultSetMetadata> FlightSqlResultSet::GetMetadata() {
  return metadata_;
}

void FlightSqlResultSet::BindColumn(int column, DataType target_type,
                                    int precision, int scale, void *buffer,
                                    size_t buffer_length,
                                    size_t *strlen_buffer) {
  if (buffer == nullptr) {
    binding_[column - 1].reset();
    return;
  }

  binding_[column - 1].reset(new ColumnBinding(column, target_type, precision,
                                               scale, buffer, buffer_length,
                                               strlen_buffer));
}

Accessor *FlightSqlResultSet::GetAccessor(int column, DataType target_type) {
  std::unique_ptr<Accessor> &accessor = accessors_[column - 1];
  if (accessor && accessor->GetTargetType() == target_type) {
    return accessor.get();
  }

  const std::shared_ptr<arrow::DataType> &source_type =
      schema_->field(column - 1)->type();

  accessors_[column - 1] = CreateAccessor(*source_type, target_type);

  return accessors_[column - 1].get();
}

FlightSqlResultSet::~FlightSqlResultSet() = default;

} // namespace flight_sql
} // namespace driver

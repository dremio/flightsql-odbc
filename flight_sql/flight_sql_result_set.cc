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

#include <arrow/array.h>
#include <arrow/flight/types.h>
#include <iostream>
#include <odbcabstraction/columnar_result_set.h>
#include <utility>

#include "flight_sql_result_set_accessors.h"

namespace driver {
namespace flight_sql {

using arrow::Array;
using arrow::RecordBatch;
using arrow::Status;
using arrow::flight::FlightEndpoint;
using arrow::flight::FlightStreamChunk;
using arrow::flight::FlightStreamReader;
using odbcabstraction::Accessor;
using odbcabstraction::ColumnarResultSet;
using odbcabstraction::DataType;
using odbcabstraction::DriverException;
using odbcabstraction::TypedAccessor;

namespace {
// TODO: Create a utils/macros.h file?
inline void ThrowIfNotOK(const Status &status) {
  if (!status.ok()) {
    throw DriverException(status.ToString());
  }
}
} // namespace

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
};

FlightSqlResultSet::FlightSqlResultSet(
    const std::shared_ptr<ResultSetMetadata> &metadata,
    FlightSqlClient &flight_sql_client,
    const arrow::flight::FlightCallOptions &call_options,
    const std::shared_ptr<FlightInfo> &flight_info)
    : ColumnarResultSet(metadata), current_row_(0) {
  chunk_iterator_.reset(new FlightStreamChunkIterator(
      flight_sql_client, call_options, flight_info));

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

  size_t fetched_rows = 0;

  while (fetched_rows < rows) {
    size_t batch_rows = current_chunk_.data->num_rows();
    size_t rows_to_fetch =
        std::min(rows - fetched_rows, batch_rows - current_row_);

    if (rows_to_fetch == 0) {
      if (!chunk_iterator_->GetNext(&current_chunk_)) {
        break;
      }
      current_row_ = 0;
      continue;
    }

    for (auto it = accessors_.begin(); it != accessors_.end(); *it++) {
      // There can be unbound columns.
      if (*it == nullptr)
        continue;

      size_t accessor_rows = (*it)->Move(rows_to_fetch);
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

void FlightSqlResultSet::Close() {}

bool FlightSqlResultSet::GetData(int column,
                                 odbcabstraction::DataType target_type,
                                 int precision, int scale, void *buffer,
                                 size_t buffer_length, size_t *strlen_buffer) {
  return false;
}

std::shared_ptr<arrow::Array>
FlightSqlResultSet::GetArrayForColumn(int column) {
  std::shared_ptr<Array> result = current_chunk_.data->column(column - 1);
  return result;
}

int64_t FlightSqlResultSet::GetCurrentRow() { return current_row_; }

std::unique_ptr<Accessor> FlightSqlResultSet::CreateAccessor(
    int column, DataType target_type, int precision, int scale, void *buffer,
    size_t buffer_length, size_t *strlen_buffer) {
  const std::shared_ptr<arrow::DataType> &data_type =
      schema_->field(column - 1)->type();
  return CreateAccessorX(this, column, *data_type, target_type, precision,
                         scale, buffer, buffer_length, strlen_buffer);
}

FlightSqlResultSet::~FlightSqlResultSet() = default;

} // namespace flight_sql
} // namespace driver

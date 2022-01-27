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
#include <utility>

namespace driver {
namespace flight_sql {

using arrow::Array;
using arrow::RecordBatch;
using arrow::Status;
using arrow::flight::FlightEndpoint;
using arrow::flight::FlightStreamChunk;
using arrow::flight::FlightStreamReader;
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

template <odbcabstraction::DataType TARGET_TYPE>
class MyAccessor : public TypedAccessor<MyAccessor<TARGET_TYPE>, TARGET_TYPE> {
private:
  int column_;
  int precision_;
  int scale_;
  void *buffer_;
  size_t buffer_length_;
  size_t *strlen_buffer_;

public:
  MyAccessor(int column, odbcabstraction::DataType target_type, int precision,
             int scale, void *buffer, size_t buffer_length,
             size_t *strlen_buffer)
      : column_(column), precision_(precision), scale_(scale), buffer_(buffer),
        buffer_length_(buffer_length), strlen_buffer_(strlen_buffer) {}

  size_t Move_VARCHAR(size_t cells) { return cells; }
};

class FlightStreamChunkIterator {
private:
  std::vector<std::unique_ptr<FlightStreamReader>> stream_readers_;

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
      stream_readers_[i] = std::move(result.ValueOrDie());
    }
  }

  void GetNext(FlightStreamChunk *chunk) {
    ThrowIfNotOK(stream_readers_[0]->Next(chunk));
  }
};

FlightSqlResultSet::FlightSqlResultSet(
    std::shared_ptr<ResultSetMetadata> metadata,
    FlightSqlClient &flight_sql_client,
    const arrow::flight::FlightCallOptions &call_options,
    const std::shared_ptr<FlightInfo> &flight_info)
    : ColumnarResultSet(std::move(metadata)), current_row(0) {
  std::vector<FlightEndpoint> &vector = flight_info->endpoints();
  const auto &result = flight_sql_client.DoGet(call_options, vector[0].ticket);
  ThrowIfNotOK(result.status());

  const std::unique_ptr<FlightStreamReader> &reader = result.ValueOrDie();

  FlightStreamChunk chunk;
  ThrowIfNotOK(reader->Next(&chunk));

  std::shared_ptr<RecordBatch> &record_batch = chunk.data;
  const std::shared_ptr<Array> &ptr = record_batch->column(0);

  std::cout << "Chunk: " << ptr->ToString() << std::endl;
}

std::shared_ptr<arrow::Array> FlightSqlResultSet::GetArray(int column) {
  const std::shared_ptr<Array> &ptr = record_batch->column(column - 1);
  return std::shared_ptr<arrow::Array>();
}

std::unique_ptr<Accessor> FlightSqlResultSet::CreateAccessor(
    int column, odbcabstraction::DataType target_type, int precision, int scale,
    void *buffer, size_t buffer_length, size_t *strlen_buffer) {
  // TODO: Handle other types
  return std::unique_ptr<Accessor>(new MyAccessor<odbcabstraction::VARCHAR>(
      column, target_type, precision, scale, buffer, buffer_length,
      strlen_buffer));
}

size_t FlightSqlResultSet::Move(size_t rows) {
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
      throw DriverException("Expected the same number of rows for all columns");
    }
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

} // namespace flight_sql
} // namespace driver

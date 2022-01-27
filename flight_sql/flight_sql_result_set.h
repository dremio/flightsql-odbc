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

#include <arrow/array.h>
#include <arrow/flight/sql/client.h>
#include <arrow/flight/types.h>
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
using odbcabstraction::DataType;
using odbcabstraction::DriverException;
using odbcabstraction::ResultSet;
using odbcabstraction::ResultSetMetadata;

class FlightStreamChunkIterator;
class Accessor;
struct ColumnBinding;

class FlightSqlResultSet : public ResultSet {
private:
  std::vector<std::unique_ptr<ColumnBinding>> binding_;
  std::shared_ptr<ResultSetMetadata> metadata_;

  int64_t current_row_;
  std::unique_ptr<FlightStreamChunkIterator> chunk_iterator_;
  FlightStreamChunk current_chunk_;
  std::shared_ptr<Schema> schema_;

public:
  ~FlightSqlResultSet() override;

  FlightSqlResultSet(std::shared_ptr<ResultSetMetadata> metadata,
                     FlightSqlClient &flight_sql_client,
                     const arrow::flight::FlightCallOptions &call_options,
                     const std::shared_ptr<FlightInfo> &flight_info);

  void Close() override;

  bool GetData(int column, odbcabstraction::DataType target_type, int precision,
               int scale, void *buffer, size_t buffer_length,
               size_t *strlen_buffer, int64_t value_offset) override;

  size_t Move(size_t rows) override;

  std::shared_ptr<arrow::Array> GetArrayForColumn(int column);

  std::shared_ptr<ResultSetMetadata> GetMetadata() override;

  void BindColumn(int column, DataType target_type, int precision, int scale,
                  void *buffer, size_t buffer_length,
                  size_t *strlen_buffer) override;

  int64_t GetCurrentRow();
};
} // namespace flight_sql
} // namespace driver

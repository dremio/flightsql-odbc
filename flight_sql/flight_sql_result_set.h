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

} // namespace flight_sql
} // namespace driver

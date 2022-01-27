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

#include <arrow/flight/sql/client.h>
#include <arrow/flight/types.h>
#include <odbcabstraction/columnar_result_set.h>

namespace driver {
namespace flight_sql {

using arrow::flight::FlightInfo;
using arrow::flight::sql::FlightSqlClient;
using odbcabstraction::Accessor;
using odbcabstraction::ResultSetMetadata;

class FlightSqlResultSet : public odbcabstraction::ColumnarResultSet {
private:
  std::shared_ptr<FlightInfo> flight_info_;

protected:
  std::unique_ptr<Accessor>
  CreateAccessor(int column, odbcabstraction::DataType target_type,
                 int precision, int scale, void *buffer, size_t buffer_length,
                 size_t *strlen_buffer) override;

public:
  FlightSqlResultSet(std::shared_ptr<ResultSetMetadata> metadata,
                     arrow::flight::sql::FlightSqlClient &flight_sql_client,
                     std::shared_ptr<FlightInfo> flight_info);

  void Close() override;

  bool GetData(int column, odbcabstraction::DataType target_type, int precision,
               int scale, void *buffer, size_t buffer_length,
               size_t *strlen_buffer) override;
};
} // namespace flight_sql
} // namespace driver

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
#include <utility>

namespace driver {
namespace flight_sql {

using odbcabstraction::DataType;
using odbcabstraction::TypedAccessor;

template <odbcabstraction::DataType TARGET_TYPE>
class MyAccessor : public TypedAccessor<MyAccessor<TARGET_TYPE>, TARGET_TYPE> {
private:
  int column;
  int precision;
  int scale;
  void *buffer;
  size_t buffer_length;
  size_t *strlen_buffer;

public:
  size_t Move_VARCHAR(size_t cells) { return cells; }
};

FlightSqlResultSet::FlightSqlResultSet(
    std::shared_ptr<ResultSetMetadata> metadata,
    FlightSqlClient &flight_sql_client, std::shared_ptr<FlightInfo> flight_info)
    : ColumnarResultSet(std::move(metadata)) {
  const auto &result =
      flight_sql_client.DoGet({}, flight_info->endpoints()[0].ticket);
}

std::unique_ptr<Accessor> FlightSqlResultSet::CreateAccessor(
    int column, odbcabstraction::DataType target_type, int precision, int scale,
    void *buffer, size_t buffer_length, size_t *strlen_buffer) {
  // TODO: Handle other types
  return std::unique_ptr<Accessor>(new MyAccessor<odbcabstraction::VARCHAR>());
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

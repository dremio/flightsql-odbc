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

#include "flight_sql_statement_get_tables.h"
#include "arrow/flight/api.h"
#include "arrow/flight/types.h"
#include "record_batch_transformer.h"
#include "utils.h"
#include "flight_sql_result_set.h"

namespace driver {
namespace flight_sql {

using arrow::Result;
using arrow::flight::FlightClientOptions;
using arrow::flight::FlightInfo;
using arrow::flight::sql::FlightSqlClient;

void ParseTableTypes(const std::string * table_type, std::vector<std::string>& table_types) {
  std::stringstream ss(*table_type);

  while (ss.good()) {
    std::string _type;
    getline(ss, _type, ',');
    _type.erase(remove(_type.begin(), _type.end(), '\''), _type.end());
    _type.erase(remove(_type.begin(), _type.end(), ' '), _type.end());

    table_types.push_back(_type);
  }
}

std::shared_ptr<ResultSet> GetTablesV3ForSQLAllCatalogs(FlightCallOptions &call_options_,
                                                        FlightSqlClient &sql_client_) {
  Result<std::shared_ptr<FlightInfo>> result =
      sql_client_.GetCatalogs(call_options_);

  std::shared_ptr<Schema> schema;
  std::shared_ptr<FlightInfo> flight_info;

  ThrowIfNotOK(result.status());
  flight_info = result.ValueOrDie();
  ThrowIfNotOK(flight_info->GetSchema(nullptr, &schema));

  auto transformer = RecordBatchTransformerWithTasksBuilder(schema)
                         .RenameField("catalog_name", "TABLE_CAT")
                         .AddFieldOfNulls("TABLE_SCHEM", utf8())
                         .AddFieldOfNulls("TABLE_NAME", utf8())
                         .AddFieldOfNulls("TABLE_TYPE", utf8())
                         .AddFieldOfNulls("REMARKS", utf8())
                         .Build();

  return std::make_shared<FlightSqlResultSet>(
    sql_client_, call_options_, flight_info,
    transformer);
}

std::shared_ptr<ResultSet> GetTablesV3ForSQLAllDbSchemas(FlightCallOptions &call_options_,
                                                         FlightSqlClient &sql_client_,
                                                         const std::string *schema_name) {
  Result<std::shared_ptr<FlightInfo>> result =
      sql_client_.GetDbSchemas(call_options_, nullptr, schema_name);

  std::shared_ptr<Schema> schema;
  std::shared_ptr<FlightInfo> flight_info;

  ThrowIfNotOK(result.status());
  flight_info = result.ValueOrDie();
  ThrowIfNotOK(flight_info->GetSchema(nullptr, &schema));

  auto transformer = RecordBatchTransformerWithTasksBuilder(schema)
                         .AddFieldOfNulls("TABLE_CAT", utf8())
                         .RenameField("schema_name", "TABLE_SCHEM")
                         .AddFieldOfNulls("TABLE_NAME", utf8())
                         .AddFieldOfNulls("TABLE_TYPE", utf8())
                         .AddFieldOfNulls("REMARKS", utf8())
                         .Build();

  return std::make_shared<FlightSqlResultSet>(
    sql_client_, call_options_, flight_info,
    transformer);
}

std::shared_ptr<ResultSet> GetTablesV3ForSQLAllTableTypes(FlightCallOptions &call_options_,
                                                          FlightSqlClient &sql_client_) {
  Result<std::shared_ptr<FlightInfo>> result =
      sql_client_.GetTableTypes(call_options_);

  std::shared_ptr<Schema> schema;
  std::shared_ptr<FlightInfo> flight_info;

  ThrowIfNotOK(result.status());
  flight_info = result.ValueOrDie();
  ThrowIfNotOK(flight_info->GetSchema(nullptr, &schema));

  auto transformer = RecordBatchTransformerWithTasksBuilder(schema)
                         .AddFieldOfNulls("TABLE_CAT", utf8())
                         .AddFieldOfNulls("TABLE_SCHEM", utf8())
                         .AddFieldOfNulls("TABLE_NAME", utf8())
                         .RenameField("table_type", "TABLE_TYPE")
                         .AddFieldOfNulls("REMARKS", utf8())
                         .Build();

  return std::make_shared<FlightSqlResultSet>(
    sql_client_, call_options_, flight_info,
    transformer);
}

std::shared_ptr<ResultSet> GetTablesV3ForGenericUse(FlightCallOptions &call_options_,
                              FlightSqlClient &sql_client_,
                              const std::string *catalog_name,
                              const std::string *schema_name,
                              const std::string *table_name,
                              const std::vector<std::string> &table_types) {
  Result<std::shared_ptr<FlightInfo>> result =
      sql_client_.GetTables(call_options_, catalog_name, schema_name,
                            table_name, false, &table_types);

  std::shared_ptr<Schema> schema;
  std::shared_ptr<FlightInfo> flight_info;

  ThrowIfNotOK(result.status());
  flight_info = result.ValueOrDie();
  ThrowIfNotOK(flight_info->GetSchema(nullptr, &schema));

  auto transformer = RecordBatchTransformerWithTasksBuilder(schema)
                         .RenameField("catalog_name", "TABLE_CAT")
                         .RenameField("schema_name", "TABLE_SCHEM")
                         .RenameField("table_name", "TABLE_NAME")
                         .RenameField("table_type", "TABLE_TYPE")
                         .AddFieldOfNulls("REMARKS", utf8())
                         .Build();

  return std::make_shared<FlightSqlResultSet>(
    sql_client_, call_options_, flight_info,
    transformer);
}

} // namespace flight_sql
} // namespace driver

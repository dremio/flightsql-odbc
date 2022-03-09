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
#include "flight_sql_result_set.h"
#include "record_batch_transformer.h"
#include "utils.h"

#include <boost/tokenizer.hpp>

namespace driver {
namespace flight_sql {

using arrow::Result;
using arrow::flight::FlightClientOptions;
using arrow::flight::FlightInfo;
using arrow::flight::sql::FlightSqlClient;

void ParseTableTypes(const std::string &table_type,
                     std::vector<std::string> &table_types) {
  boost::char_separator<char> quote_separator(",");
  boost::tokenizer<boost::char_separator<char>> tokens(table_type,
                                                       quote_separator);

  for (std::string item : tokens) {
    item.erase(remove(item.begin(), item.end(), '\''), item.end());
    item.erase(remove(item.begin(), item.end(), ' '), item.end());
    table_types.emplace_back(item);
  }
}

std::shared_ptr<ResultSet>
GetTablesV3ForSQLAllCatalogs(FlightCallOptions &call_options,
                             FlightSqlClient &sql_client) {
  Result<std::shared_ptr<FlightInfo>> result =
      sql_client.GetCatalogs(call_options);

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

  return std::make_shared<FlightSqlResultSet>(sql_client, call_options,
                                              flight_info, transformer);
}

std::shared_ptr<ResultSet>
GetTablesV3ForSQLAllDbSchemas(FlightCallOptions &call_options,
                              FlightSqlClient &sql_client,
                              const std::string *schema_name) {
  Result<std::shared_ptr<FlightInfo>> result =
      sql_client.GetDbSchemas(call_options, nullptr, schema_name);

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

  return std::make_shared<FlightSqlResultSet>(sql_client, call_options,
                                              flight_info, transformer);
}

std::shared_ptr<ResultSet>
GetTablesV3ForSQLAllTableTypes(FlightCallOptions &call_options,
                               FlightSqlClient &sql_client) {
  Result<std::shared_ptr<FlightInfo>> result =
      sql_client.GetTableTypes(call_options);

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

  return std::make_shared<FlightSqlResultSet>(sql_client, call_options,
                                              flight_info, transformer);
}

std::shared_ptr<ResultSet> GetTablesV3ForGenericUse(
    FlightCallOptions &call_options, FlightSqlClient &sql_client,
    const std::string *catalog_name, const std::string *schema_name,
    const std::string *table_name,
    const std::vector<std::string> &table_types) {
  Result<std::shared_ptr<FlightInfo>> result = sql_client.GetTables(
      call_options, catalog_name, schema_name, table_name, false, &table_types);

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

  return std::make_shared<FlightSqlResultSet>(sql_client, call_options,
                                              flight_info, transformer);
}

} // namespace flight_sql
} // namespace driver

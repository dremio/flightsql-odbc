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

#include <boost/algorithm/string/trim.hpp>
#include <boost/tokenizer.hpp>

namespace driver {
namespace flight_sql {

using arrow::Result;
using arrow::flight::FlightClientOptions;
using arrow::flight::FlightInfo;
using arrow::flight::sql::FlightSqlClient;

typedef boost::tokenizer<boost::escaped_list_separator<char>> Tokenizer;

void ParseTableTypes(const std::string &table_type,
                     std::vector<std::string> &table_types) {
  std::stringstream string_stream(table_type);

  Tokenizer tok(table_type);

  for (Tokenizer::iterator it(tok.begin()), end(tok.end()); it != end; ++it) {
    std::string ss(*it);

    boost::algorithm::trim_if(ss, [](char c) { return c == '\''; });
    table_types.emplace_back(ss);
  }
}

std::shared_ptr<ResultSet>
GetTablesForSQLAllCatalogs(const ColumnNames &names,
                           FlightCallOptions &call_options,
                           FlightSqlClient &sql_client) {
  Result<std::shared_ptr<FlightInfo>> result =
      sql_client.GetCatalogs(call_options);

  std::shared_ptr<Schema> schema;
  std::shared_ptr<FlightInfo> flight_info;

  ThrowIfNotOK(result.status());
  flight_info = result.ValueOrDie();
  ThrowIfNotOK(flight_info->GetSchema(nullptr, &schema));

  auto transformer = RecordBatchTransformerWithTasksBuilder(schema)
                         .RenameField("catalog_name", names.catalog_column)
                         .AddFieldOfNulls(names.schema_column, utf8())
                         .AddFieldOfNulls(names.table_column, utf8())
                         .AddFieldOfNulls(names.table_type_column, utf8())
                         .AddFieldOfNulls(names.remarks_column, utf8())
                         .Build();

  return std::make_shared<FlightSqlResultSet>(sql_client, call_options,
                                              flight_info, transformer);
}

std::shared_ptr<ResultSet> GetTablesForSQLAllDbSchemas(
    const ColumnNames &names, FlightCallOptions &call_options,
    FlightSqlClient &sql_client, const std::string *schema_name) {
  Result<std::shared_ptr<FlightInfo>> result =
      sql_client.GetDbSchemas(call_options, nullptr, schema_name);

  std::shared_ptr<Schema> schema;
  std::shared_ptr<FlightInfo> flight_info;

  ThrowIfNotOK(result.status());
  flight_info = result.ValueOrDie();
  ThrowIfNotOK(flight_info->GetSchema(nullptr, &schema));

  auto transformer = RecordBatchTransformerWithTasksBuilder(schema)
                         .AddFieldOfNulls(names.catalog_column, utf8())
                         .RenameField("db_schema_name", names.schema_column)
                         .AddFieldOfNulls(names.table_column, utf8())
                         .AddFieldOfNulls(names.table_type_column, utf8())
                         .AddFieldOfNulls(names.remarks_column, utf8())
                         .Build();

  return std::make_shared<FlightSqlResultSet>(sql_client, call_options,
                                              flight_info, transformer);
}

std::shared_ptr<ResultSet>
GetTablesForSQLAllTableTypes(const ColumnNames &names,
                             FlightCallOptions &call_options,
                             FlightSqlClient &sql_client) {
  Result<std::shared_ptr<FlightInfo>> result =
      sql_client.GetTableTypes(call_options);

  std::shared_ptr<Schema> schema;
  std::shared_ptr<FlightInfo> flight_info;

  ThrowIfNotOK(result.status());
  flight_info = result.ValueOrDie();
  ThrowIfNotOK(flight_info->GetSchema(nullptr, &schema));

  auto transformer = RecordBatchTransformerWithTasksBuilder(schema)
                         .AddFieldOfNulls(names.catalog_column, utf8())
                         .AddFieldOfNulls(names.schema_column, utf8())
                         .AddFieldOfNulls(names.table_column, utf8())
                         .RenameField("table_type", names.table_type_column)
                         .AddFieldOfNulls(names.remarks_column, utf8())
                         .Build();

  return std::make_shared<FlightSqlResultSet>(sql_client, call_options,
                                              flight_info, transformer);
}

std::shared_ptr<ResultSet> GetTablesForGenericUse(
    const ColumnNames &names, FlightCallOptions &call_options,
    FlightSqlClient &sql_client, const std::string *catalog_name,
    const std::string *schema_name, const std::string *table_name,
    const std::vector<std::string> &table_types) {
  Result<std::shared_ptr<FlightInfo>> result = sql_client.GetTables(
      call_options, catalog_name, schema_name, table_name, false, &table_types);

  std::shared_ptr<Schema> schema;
  std::shared_ptr<FlightInfo> flight_info;

  ThrowIfNotOK(result.status());
  flight_info = result.ValueOrDie();
  ThrowIfNotOK(flight_info->GetSchema(nullptr, &schema));

  auto transformer = RecordBatchTransformerWithTasksBuilder(schema)
                         .RenameField("catalog_name", names.catalog_column)
                         .RenameField("db_schema_name", names.schema_column)
                         .RenameField("table_name", names.table_column)
                         .RenameField("table_type", names.table_type_column)
                         .AddFieldOfNulls(names.remarks_column, utf8())
                         .Build();

  return std::make_shared<FlightSqlResultSet>(sql_client, call_options,
                                              flight_info, transformer);
}

} // namespace flight_sql
} // namespace driver

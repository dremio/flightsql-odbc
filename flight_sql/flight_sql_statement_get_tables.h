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

#include "arrow/flight/types.h"
#include "record_batch_transformer.h"
#include "odbcabstraction/result_set.h"
#include <arrow/flight/sql/client.h>
#include <arrow/type.h>

namespace driver {
namespace flight_sql {

using odbcabstraction::ResultSet;
using arrow::flight::FlightCallOptions;
using arrow::flight::sql::FlightSqlClient;


void ParseTableTypes(const std::string * table_type, std::vector<std::string>& table_types);

std::shared_ptr<ResultSet> GetTablesV3ForSQLAllCatalogs(FlightCallOptions& call_options,
                                                        FlightSqlClient &sql_client);
std::shared_ptr<ResultSet> GetTablesV3ForSQLAllDbSchemas(FlightCallOptions& call_options,
                                                         FlightSqlClient &sql_client,
                                                         const std::string *schema_name);
std::shared_ptr<ResultSet> GetTablesV3ForSQLAllTableTypes(FlightCallOptions& call_options,
                                                          FlightSqlClient &sql_client);
std::shared_ptr<ResultSet> GetTablesV3ForGenericUse(FlightCallOptions& call_options,
                              FlightSqlClient &sql_client,
                              const std::string *catalog_name,
                              const std::string *schema_name,
                              const std::string *table_name,
                              const std::vector<std::string> &table_types);
} // namespace flight_sql
} // namespace driver

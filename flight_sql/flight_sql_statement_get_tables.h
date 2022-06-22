/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#pragma once

#include "flight_sql_connection.h"
#include "arrow/flight/types.h"
#include <odbcabstraction/spi/result_set.h>
#include <odbcabstraction/diagnostics.h>
#include "record_batch_transformer.h"
#include "odbcabstraction/types.h"
#include <arrow/flight/sql/client.h>
#include <arrow/type.h>

namespace driver {
namespace flight_sql {

using arrow::flight::FlightCallOptions;
using arrow::flight::sql::FlightSqlClient;
using odbcabstraction::ResultSet;

typedef struct {
  std::string catalog_column;
  std::string schema_column;
  std::string table_column;
  std::string table_type_column;
  std::string remarks_column;
} ColumnNames;

void ParseTableTypes(const std::string &table_type,
                     std::vector<std::string> &table_types);

std::shared_ptr<ResultSet>
GetTablesForSQLAllCatalogs(const ColumnNames &column_names,
                           FlightCallOptions &call_options,
                           FlightSqlClient &sql_client,
                           odbcabstraction::Diagnostics &diagnostics,
                           const odbcabstraction::MetadataSettings &metadata_settings);

std::shared_ptr<ResultSet> GetTablesForSQLAllDbSchemas(
    const ColumnNames &column_names, FlightCallOptions &call_options,
    FlightSqlClient &sql_client, const std::string *schema_name,
    odbcabstraction::Diagnostics &diagnostics, const odbcabstraction::MetadataSettings &metadata_settings);

std::shared_ptr<ResultSet>
GetTablesForSQLAllTableTypes(const ColumnNames &column_names,
                             FlightCallOptions &call_options,
                             FlightSqlClient &sql_client,
                             odbcabstraction::Diagnostics &diagnostics,
                             const odbcabstraction::MetadataSettings &metadata_settings);

std::shared_ptr<ResultSet> GetTablesForGenericUse(
    const ColumnNames &column_names, FlightCallOptions &call_options,
    FlightSqlClient &sql_client, const std::string *catalog_name,
    const std::string *schema_name, const std::string *table_name,
    const std::vector<std::string> &table_types,
    odbcabstraction::Diagnostics &diagnostics,
    const odbcabstraction::MetadataSettings &metadata_settings);
} // namespace flight_sql
} // namespace driver

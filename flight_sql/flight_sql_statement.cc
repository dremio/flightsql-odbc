/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include "flight_sql_statement.h"
#include <odbcabstraction/platform.h>
#include "flight_sql_result_set.h"
#include "flight_sql_result_set_metadata.h"
#include "flight_sql_statement_get_columns.h"
#include "flight_sql_statement_get_tables.h"
#include "flight_sql_statement_get_type_info.h"
#include "record_batch_transformer.h"
#include "utils.h"
#include <arrow/io/memory.h>
#include <sql.h>
#include <sqlext.h>

#include <boost/optional.hpp>
#include <utility>
#include <odbcabstraction/exceptions.h>

namespace driver {
namespace flight_sql {

using arrow::Result;
using arrow::Status;
using arrow::flight::FlightCallOptions;
using arrow::flight::FlightClientOptions;
using arrow::flight::FlightInfo;
using arrow::flight::Location;
using arrow::flight::TimeoutDuration;
using arrow::flight::sql::FlightSqlClient;
using arrow::flight::sql::PreparedStatement;
using driver::odbcabstraction::DriverException;
using driver::odbcabstraction::ResultSet;
using driver::odbcabstraction::ResultSetMetadata;
using driver::odbcabstraction::Statement;

namespace {

void ClosePreparedStatementIfAny(
    std::shared_ptr<arrow::flight::sql::PreparedStatement>
        &prepared_statement) {
  if (prepared_statement != nullptr) {
    ThrowIfNotOK(prepared_statement->Close());
    prepared_statement.reset();
  }
}

} // namespace

FlightSqlStatement::FlightSqlStatement(
    const odbcabstraction::Diagnostics& diagnostics,
    FlightSqlClient &sql_client,
    FlightCallOptions call_options,
    const odbcabstraction::MetadataSettings& metadata_settings)
    : diagnostics_("Apache Arrow", diagnostics.GetDataSourceComponent(), diagnostics.GetOdbcVersion()),
      sql_client_(sql_client), call_options_(std::move(call_options)), metadata_settings_(metadata_settings) {
  attribute_[METADATA_ID] = static_cast<size_t>(SQL_FALSE);
  attribute_[MAX_LENGTH] = static_cast<size_t>(0);
  attribute_[NOSCAN] = static_cast<size_t>(SQL_NOSCAN_OFF);
  attribute_[QUERY_TIMEOUT] = static_cast<size_t>(0);
  call_options_.timeout = TimeoutDuration{-1};
}

bool FlightSqlStatement::SetAttribute(StatementAttributeId attribute,
                                      const Attribute &value) {
  switch (attribute) {
  case METADATA_ID:
    return CheckIfSetToOnlyValidValue(value, static_cast<size_t>(SQL_FALSE));
  case NOSCAN:
    return CheckIfSetToOnlyValidValue(value, static_cast<size_t>(SQL_NOSCAN_OFF));
  case MAX_LENGTH:
    return CheckIfSetToOnlyValidValue(value, static_cast<size_t>(0));
  case QUERY_TIMEOUT:
    if (boost::get<size_t>(value) > 0) {
      call_options_.timeout =
          TimeoutDuration{static_cast<double>(boost::get<size_t>(value))};
    } else {
      call_options_.timeout = TimeoutDuration{-1};
      // Intentional fall-through.
    }
  default:
    attribute_[attribute] = value;
    return true;
  }
}

boost::optional<Statement::Attribute>
FlightSqlStatement::GetAttribute(StatementAttributeId attribute) {
  const auto &it = attribute_.find(attribute);
  return boost::make_optional(it != attribute_.end(), it->second);
}

boost::optional<std::shared_ptr<ResultSetMetadata>>
FlightSqlStatement::Prepare(const std::string &query) {
  ClosePreparedStatementIfAny(prepared_statement_);

  Result<std::shared_ptr<PreparedStatement>> result =
      sql_client_.Prepare(call_options_, query);
  ThrowIfNotOK(result.status());

  prepared_statement_ = *result;

  const auto &result_set_metadata =
      std::make_shared<FlightSqlResultSetMetadata>(
          prepared_statement_->dataset_schema(), metadata_settings_);
  return boost::optional<std::shared_ptr<ResultSetMetadata>>(
      result_set_metadata);
}

bool FlightSqlStatement::ExecutePrepared() {
  assert(prepared_statement_.get() != nullptr);

  Result<std::shared_ptr<FlightInfo>> result = prepared_statement_->Execute();
  ThrowIfNotOK(result.status());

  current_result_set_ = std::make_shared<FlightSqlResultSet>(
      sql_client_, call_options_, result.ValueOrDie(), nullptr, diagnostics_, metadata_settings_);

  return true;
}

bool FlightSqlStatement::Execute(const std::string &query) {
  ClosePreparedStatementIfAny(prepared_statement_);

  Result<std::shared_ptr<FlightInfo>> result =
      sql_client_.Execute(call_options_, query);
  ThrowIfNotOK(result.status());

  current_result_set_ = std::make_shared<FlightSqlResultSet>(
      sql_client_, call_options_, result.ValueOrDie(), nullptr, diagnostics_, metadata_settings_);

  return true;
}

std::shared_ptr<ResultSet> FlightSqlStatement::GetResultSet() {
  return current_result_set_;
}

long FlightSqlStatement::GetUpdateCount() { return -1; }

std::shared_ptr<odbcabstraction::ResultSet> FlightSqlStatement::GetTables(
    const std::string *catalog_name, const std::string *schema_name,
    const std::string *table_name, const std::string *table_type,
    const ColumnNames &column_names) {
  ClosePreparedStatementIfAny(prepared_statement_);

  std::vector<std::string> table_types;

  if ((catalog_name && *catalog_name == "%") &&
      (schema_name && schema_name->empty()) &&
      (table_name && table_name->empty())) {
    current_result_set_ =
        GetTablesForSQLAllCatalogs(
                column_names, call_options_, sql_client_, diagnostics_, metadata_settings_);
  } else if ((catalog_name && catalog_name->empty()) &&
             (schema_name && *schema_name == "%") &&
             (table_name && table_name->empty())) {
    current_result_set_ = GetTablesForSQLAllDbSchemas(
        column_names, call_options_, sql_client_, schema_name, diagnostics_, metadata_settings_);
  } else if ((catalog_name && catalog_name->empty()) &&
             (schema_name && schema_name->empty()) &&
             (table_name && table_name->empty()) &&
             (table_type && *table_type == "%")) {
    current_result_set_ =
        GetTablesForSQLAllTableTypes(
                column_names, call_options_, sql_client_, diagnostics_, metadata_settings_);
  } else {
    if (table_type) {
      ParseTableTypes(*table_type, table_types);
    }

    current_result_set_ = GetTablesForGenericUse(
        column_names, call_options_, sql_client_, catalog_name, schema_name,
        table_name, table_types, diagnostics_, metadata_settings_);
  }

  return current_result_set_;
}

std::shared_ptr<ResultSet> FlightSqlStatement::GetTables_V2(
    const std::string *catalog_name, const std::string *schema_name,
    const std::string *table_name, const std::string *table_type) {
  ColumnNames column_names{"TABLE_QUALIFIER", "TABLE_OWNER", "TABLE_NAME",
                           "TABLE_TYPE", "REMARKS"};

  return GetTables(catalog_name, schema_name, table_name, table_type,
                   column_names);
}

std::shared_ptr<ResultSet> FlightSqlStatement::GetTables_V3(
    const std::string *catalog_name, const std::string *schema_name,
    const std::string *table_name, const std::string *table_type) {
  ColumnNames column_names{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME",
                           "TABLE_TYPE", "REMARKS"};

  return GetTables(catalog_name, schema_name, table_name, table_type,
                   column_names);
}

std::shared_ptr<ResultSet> FlightSqlStatement::GetColumns_V2(
    const std::string *catalog_name, const std::string *schema_name,
    const std::string *table_name, const std::string *column_name) {
  ClosePreparedStatementIfAny(prepared_statement_);

  Result<std::shared_ptr<FlightInfo>> result = sql_client_.GetTables(
      call_options_, catalog_name, schema_name, table_name, true, nullptr);
  ThrowIfNotOK(result.status());

  auto flight_info = result.ValueOrDie();

  auto transformer = std::make_shared<GetColumns_Transformer>(
      metadata_settings_, odbcabstraction::V_2, column_name);

  current_result_set_ = std::make_shared<FlightSqlResultSet>(
      sql_client_, call_options_, flight_info, transformer, diagnostics_, metadata_settings_);

  return current_result_set_;
}

std::shared_ptr<ResultSet> FlightSqlStatement::GetColumns_V3(
    const std::string *catalog_name, const std::string *schema_name,
    const std::string *table_name, const std::string *column_name) {
  ClosePreparedStatementIfAny(prepared_statement_);

  Result<std::shared_ptr<FlightInfo>> result = sql_client_.GetTables(
      call_options_, catalog_name, schema_name, table_name, true, nullptr);
  ThrowIfNotOK(result.status());

  auto flight_info = result.ValueOrDie();

  auto transformer = std::make_shared<GetColumns_Transformer>(
      metadata_settings_, odbcabstraction::V_3, column_name);

  current_result_set_ = std::make_shared<FlightSqlResultSet>(
      sql_client_, call_options_, flight_info, transformer, diagnostics_, metadata_settings_);

  return current_result_set_;
}

std::shared_ptr<ResultSet> FlightSqlStatement::GetTypeInfo_V2(int16_t data_type) {
  ClosePreparedStatementIfAny(prepared_statement_);

  Result<std::shared_ptr<FlightInfo>> result = sql_client_.GetXdbcTypeInfo(
          call_options_);
  ThrowIfNotOK(result.status());

  auto flight_info = result.ValueOrDie();

  auto transformer = std::make_shared<GetTypeInfo_Transformer>(
          metadata_settings_, odbcabstraction::V_2, data_type);

  current_result_set_ = std::make_shared<FlightSqlResultSet>(
      sql_client_, call_options_, flight_info, transformer, diagnostics_, metadata_settings_);

  return current_result_set_;
}

std::shared_ptr<ResultSet> FlightSqlStatement::GetTypeInfo_V3(int16_t data_type) {
  ClosePreparedStatementIfAny(prepared_statement_);

  Result<std::shared_ptr<FlightInfo>> result = sql_client_.GetXdbcTypeInfo(
          call_options_);
  ThrowIfNotOK(result.status());

  auto flight_info = result.ValueOrDie();

  auto transformer = std::make_shared<GetTypeInfo_Transformer>(
          metadata_settings_, odbcabstraction::V_3, data_type);

  current_result_set_ = std::make_shared<FlightSqlResultSet>(
      sql_client_, call_options_, flight_info, transformer, diagnostics_, metadata_settings_);

  return current_result_set_;
}

std::shared_ptr<ResultSet> FlightSqlStatement::GetPrimaryKeys(
    const std::string *catalog_name, const std::string *schema_name,
    const std::string *table_name) {
  ClosePreparedStatementIfAny(prepared_statement_);

  auto schema = arrow::schema({
    arrow::field("TABLE_CAT", arrow::utf8(), true),      // nullable
    arrow::field("TABLE_SCHEM", arrow::utf8(), true),    // nullable
    arrow::field("TABLE_NAME", arrow::utf8(), false),    // not nullable
    arrow::field("COLUMN_NAME", arrow::utf8(), false),   // not nullable
    arrow::field("KEY_SEQ", arrow::int16(), false),      // not nullable
    arrow::field("PK_NAME", arrow::utf8(), true)         // nullable
  });

  auto flight_info_result = arrow::flight::FlightInfo::Make(
    *schema, arrow::flight::FlightDescriptor::Command(""),
    std::vector<arrow::flight::FlightEndpoint>(), 0, 0);
  ThrowIfNotOK(flight_info_result.status());
  auto flight_info = std::make_shared<arrow::flight::FlightInfo>(std::move(flight_info_result.ValueOrDie()));

  current_result_set_ = std::make_shared<FlightSqlResultSet>(
    sql_client_, call_options_, flight_info, nullptr, diagnostics_, metadata_settings_);

  return current_result_set_;
}

std::shared_ptr<ResultSet> FlightSqlStatement::GetForeignKeys(
    const std::string *pk_catalog_name, const std::string *pk_schema_name,
    const std::string *pk_table_name, const std::string *fk_catalog_name,
    const std::string *fk_schema_name, const std::string *fk_table_name) {
  ClosePreparedStatementIfAny(prepared_statement_);

  auto schema = arrow::schema({
    arrow::field("PKTABLE_CAT", arrow::utf8(), true),     // nullable
    arrow::field("PKTABLE_SCHEM", arrow::utf8(), true),   // nullable
    arrow::field("PKTABLE_NAME", arrow::utf8(), false),   // not nullable
    arrow::field("PKCOLUMN_NAME", arrow::utf8(), false),  // not nullable
    arrow::field("FKTABLE_CAT", arrow::utf8(), true),     // nullable
    arrow::field("FKTABLE_SCHEM", arrow::utf8(), true),   // nullable
    arrow::field("FKTABLE_NAME", arrow::utf8(), false),   // not nullable
    arrow::field("FKCOLUMN_NAME", arrow::utf8(), false),  // not nullable
    arrow::field("KEY_SEQ", arrow::int16(), false),       // not nullable
    arrow::field("UPDATE_RULE", arrow::int16(), true),    // nullable
    arrow::field("DELETE_RULE", arrow::int16(), true),    // nullable
    arrow::field("FK_NAME", arrow::utf8(), true),         // nullable
    arrow::field("PK_NAME", arrow::utf8(), true),         // nullable
    arrow::field("DEFERRABILITY", arrow::int16(), true)   // nullable
  });

  auto flight_info_result = arrow::flight::FlightInfo::Make(
    *schema, arrow::flight::FlightDescriptor::Command(""),
    std::vector<arrow::flight::FlightEndpoint>(), 0, 0);
  ThrowIfNotOK(flight_info_result.status());
  auto flight_info = std::make_shared<arrow::flight::FlightInfo>(std::move(flight_info_result.ValueOrDie()));

  current_result_set_ = std::make_shared<FlightSqlResultSet>(
    sql_client_, call_options_, flight_info, nullptr, diagnostics_, metadata_settings_);

  return current_result_set_;
}

odbcabstraction::Diagnostics &FlightSqlStatement::GetDiagnostics() {
  return diagnostics_;
}

void FlightSqlStatement::Cancel() {
  if (!current_result_set_) return;
  current_result_set_->Cancel();
}

} // namespace flight_sql
} // namespace driver

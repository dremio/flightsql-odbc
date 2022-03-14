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

#include "flight_sql_statement.h"
#include "flight_sql_result_set.h"
#include "flight_sql_result_set_metadata.h"
#include "record_batch_transformer.h"
#include "utils.h"
#include <arrow/flight/sql/server.h>

#include <boost/optional.hpp>
#include <boost/algorithm/string.hpp>
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
using arrow::flight::sql::SqlSchema;
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

FlightSqlStatement::FlightSqlStatement(FlightSqlClient &sql_client,
                                       FlightCallOptions call_options)
    : sql_client_(sql_client), call_options_(call_options) {}

void FlightSqlStatement::SetAttribute(StatementAttributeId attribute,
                                      const Attribute &value) {
  attribute_[attribute] = value;
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
          prepared_statement_->dataset_schema());
  return boost::optional<std::shared_ptr<ResultSetMetadata>>(
      result_set_metadata);
}

bool FlightSqlStatement::ExecutePrepared() {
  assert(prepared_statement_.get() != nullptr);

  Result<std::shared_ptr<FlightInfo>> result = prepared_statement_->Execute();
  ThrowIfNotOK(result.status());

  current_result_set_ = std::make_shared<FlightSqlResultSet>(
      sql_client_, call_options_, result.ValueOrDie(), nullptr);

  return true;
}

bool FlightSqlStatement::Execute(const std::string &query) {
  ClosePreparedStatementIfAny(prepared_statement_);

  Result<std::shared_ptr<FlightInfo>> result =
      sql_client_.Execute(call_options_, query);
  ThrowIfNotOK(result.status());

  current_result_set_ = std::make_shared<FlightSqlResultSet>(
      sql_client_, call_options_, result.ValueOrDie(), nullptr);

  return true;
}

std::shared_ptr<ResultSet> FlightSqlStatement::GetResultSet() {
  return current_result_set_;
}

long FlightSqlStatement::GetUpdateCount() { return -1; }

std::shared_ptr<ResultSet> FlightSqlStatement::GetTables_V2(
    const std::string *catalog_name, const std::string *schema_name,
    const std::string *table_name, const std::string *table_type) {
  ClosePreparedStatementIfAny(prepared_statement_);

  std::vector<std::string> table_types(table_type ? 1 : 0);
  if (table_type) {
    table_types.push_back(*table_type);
  }

  Result<std::shared_ptr<FlightInfo>> result =
      sql_client_.GetTables(call_options_, catalog_name, schema_name,
                            table_name, false, &table_types);
  ThrowIfNotOK(result.status());

  auto flight_info = result.ValueOrDie();

  std::shared_ptr<Schema> schema;
  ThrowIfNotOK(flight_info->GetSchema(nullptr, &schema));

  auto transformer =
      RecordBatchTransformerWithTaskBuilder(schema)
          .RenameRecord(std::string("catalog_name"), std::string("TABLE_CAT"))
          .RenameRecord(std::string("schema_name"), std::string("TABLE_SCHEM"))
          .RenameRecord(std::string("table_name"), std::string("TABLE_NAME"))
          .RenameRecord(std::string("table_type"), std::string("TABLE_TYPE"))
          .AddEmptyFields("REMARKS", utf8())
          .Build();

  current_result_set_ = std::make_shared<FlightSqlResultSet>(
      sql_client_, call_options_, flight_info, transformer);

  return current_result_set_;
}

std::shared_ptr<ResultSet> FlightSqlStatement::GetTables_V3(
    const std::string *catalog_name, const std::string *schema_name,
    const std::string *table_name, const std::string *table_type) {
  ClosePreparedStatementIfAny(prepared_statement_);

  std::vector<std::string> table_types(table_type ? 1 : 0);
  if (table_type) {
    table_types.push_back(*table_type);
  }

  Result<std::shared_ptr<FlightInfo>> result;
  std::shared_ptr<Schema> schema;
  std::shared_ptr<FlightInfo> flight_info;


  std::shared_ptr<RecordBatchTransformer> transformer;

  if((*catalog_name == std::string("%"))
     && (schema_name->empty())
     && (table_name->empty())) {

    result = sql_client_.GetCatalogs(call_options_);
    ThrowIfNotOK(result.status());

    flight_info = result.ValueOrDie();

    ThrowIfNotOK(flight_info->GetSchema(nullptr, &schema));

    transformer = RecordBatchTransformer::Builder(schema)
      .RenameRecord(std::string("catalog_name"), std::string("TABLE_CAT"))
      .AddEmptyFields(std::string("TABLE_SCHEM"), utf8())
      .AddEmptyFields(std::string("TABLE_NAME"), utf8())
      .AddEmptyFields(std::string("TABLE_TYPE"), utf8())
      .AddEmptyFields(std::string("REMARKS"), utf8())
      .Build();
  } else if((*catalog_name ==  "empty_string")
            && (*schema_name ==  "%")
            && (*table_name ==  "empty_string")) {
    transformer = RecordBatchTransformer::Builder(schema)
      .AddEmptyFields(std::string("TABLE_CAT"), utf8())
      .RenameRecord(std::string("schema_name"), std::string("TABLE_SCHEM"))
      .AddEmptyFields("TABLE_NAME", utf8())
      .AddEmptyFields("TABLE_TYPE", utf8())
      .AddEmptyFields("REMARKS", utf8())
      .Build();
  } else if((*catalog_name == "empty_string")
            && (*schema_name == "empty_string")
            && (*table_name == "%")) {
    transformer = RecordBatchTransformer::Builder(schema)
      .AddEmptyFields(std::string("TABLE_CAT"), utf8())
      .AddEmptyFields(std::string("TABLE_SCHEM"), utf8())
      .RenameRecord(std::string("table_name"), std::string("TABLE_NAME"))
      .AddEmptyFields("TABLE_TYPE", utf8())
      .AddEmptyFields("REMARKS", utf8())
      .Build();
  } else {
    transformer = RecordBatchTransformer::Builder(schema)
      .RenameRecord(std::string("catalog_name"), std::string("TABLE_CAT"))
      .RenameRecord(std::string("schema_name"), std::string("TABLE_SCHEM"))
      .RenameRecord(std::string("table_name"), std::string("TABLE_NAME"))
      .RenameRecord(std::string("table_type"), std::string("TABLE_TYPE"))
      .AddEmptyFields("REMARKS", utf8())
      .Build();
  }

  current_result_set_ = std::make_shared<FlightSqlResultSet>(
    sql_client_, call_options_, flight_info, transformer);

  return current_result_set_;
}

std::shared_ptr<ResultSet> FlightSqlStatement::GetColumns_V2(
    const std::string *catalog_name, const std::string *schema_name,
    const std::string *table_name, const std::string *column_name) {
  return current_result_set_;
}

std::shared_ptr<ResultSet> FlightSqlStatement::GetColumns_V3(
    const std::string *catalog_name, const std::string *schema_name,
    const std::string *table_name, const std::string *column_name) {
  return current_result_set_;
}

std::shared_ptr<ResultSet> FlightSqlStatement::GetTypeInfo(int dataType) {
  return current_result_set_;
}

} // namespace flight_sql
} // namespace driver

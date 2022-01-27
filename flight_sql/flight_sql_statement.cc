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

#include <boost/optional.hpp>
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
inline void ThrowIfNotOK(const Status &status) {
  if (!status.ok()) {
    throw DriverException(status.ToString());
  }
}

std::shared_ptr<FlightSqlResultSetMetadata>
CreateResultSetMetaData(const std::shared_ptr<FlightInfo> &flight_info) {
  std::shared_ptr<arrow::Schema> schema;
  arrow::ipc::DictionaryMemo dict_memo;

  ThrowIfNotOK(flight_info->GetSchema(&dict_memo, &schema));

  return std::make_shared<FlightSqlResultSetMetadata>(schema);
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
  if (prepared_statement_.get() != nullptr) {
    ThrowIfNotOK(prepared_statement_->Close());
    prepared_statement_.reset();
  }

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

  current_result_set_metadata_ = CreateResultSetMetaData(result.ValueOrDie());

  // TODO: make use of the returned FlightInfo to populate ResultSet.
  current_result_set_ = std::shared_ptr<ResultSet>(
      new FlightSqlResultSet(current_result_set_metadata_));
  return true;
}

bool FlightSqlStatement::Execute(const std::string &query) {
  if (prepared_statement_ != nullptr) {
    ThrowIfNotOK(prepared_statement_->Close());
    prepared_statement_.reset();
  }

  Result<std::shared_ptr<FlightInfo>> result =
      sql_client_.Execute(call_options_, query);
  ThrowIfNotOK(result.status());

  current_result_set_metadata_ = CreateResultSetMetaData(result.ValueOrDie());

  // TODO: make use of the returned FlightInfo to populate ResultSet.
  current_result_set_ = std::shared_ptr<ResultSet>(
      new FlightSqlResultSet(current_result_set_metadata_));
  return true;
}

std::shared_ptr<ResultSet> FlightSqlStatement::GetResultSet() {
  return current_result_set_;
}

long FlightSqlStatement::GetUpdateCount() { return -1; }

std::shared_ptr<ResultSet> FlightSqlStatement::GetTables_V2(
    const std::string *catalog_name, const std::string *schema_name,
    const std::string *table_name, const std::string *table_type) {
  return current_result_set_;
}

std::shared_ptr<ResultSet> FlightSqlStatement::GetTables_V3(
    const std::string *catalog_name, const std::string *schema_name,
    const std::string *table_name, const std::string *table_type) {
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

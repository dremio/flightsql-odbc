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


#include "flight_sql_result_set_metadata.h"
#include "arrow/util/key_value_metadata.h"


#include <utility>

size_t driver::flight_sql::FlightSqlResultSetMetadata::GetColumnCount() {
  return schema.use_count();
}

std::string
driver::flight_sql::FlightSqlResultSetMetadata::GetColumnName(int column_position) {
  return schema->field(column_position)->name();
}

size_t driver::flight_sql::FlightSqlResultSetMetadata::GetPrecision(int column_position) {
  //TODO Implement after the PR from column metadata is merged
  return 0;
}

size_t driver::flight_sql::FlightSqlResultSetMetadata::GetScale(int column_position) {
  //TODO Implement after the PR from column metadata is merged
  return 0;
}

driver::odbcabstraction::DataType
driver::flight_sql::FlightSqlResultSetMetadata::GetDataType(int column_position) {
  return odbcabstraction::FLOAT;
}

driver::odbcabstraction::Nullability
driver::flight_sql::FlightSqlResultSetMetadata::IsNullable(int column_position) {
  //TODO Implement after the PR from column metadata is merged
  return odbcabstraction::NULLABILITY_NO_NULLS;
}

std::string
driver::flight_sql::FlightSqlResultSetMetadata::GetSchemaName(int column_position) {
  //TODO Implement after the PR from column metadata is merged
  return std::string();
}

std::string
driver::flight_sql::FlightSqlResultSetMetadata::GetCatalogName(int column_position) {
  //TODO Implement after the PR from column metadata is merged
  return std::string();
}

std::string
driver::flight_sql::FlightSqlResultSetMetadata::GetTableName(int column_position) {
  //TODO Implement after the PR from column metadata is merged
  return std::string();
}

std::string
driver::flight_sql::FlightSqlResultSetMetadata::GetColumnLabel(int column_position) {
  //TODO Implement after the PR from column metadata is merged
  return std::string();
}

size_t driver::flight_sql::FlightSqlResultSetMetadata::GetColumnDisplaySize(
  //TODO Implement after the PR from column metadata is merged
  int column_position) {
  return 0;
}

std::string
driver::flight_sql::FlightSqlResultSetMetadata::GetBaseColumnName(int column_position) {
  //TODO Implement after the PR from column metadata is merged
  return std::string();
}

std::string
driver::flight_sql::FlightSqlResultSetMetadata::GetBaseTableName(int column_position) {
  //TODO Implement after the PR from column metadata is merged
  return std::string();
}

std::string
driver::flight_sql::FlightSqlResultSetMetadata::GetConciseType(int column_position) {
  //TODO Implement after the PR from column metadata is merged
  return std::string();
}

size_t driver::flight_sql::FlightSqlResultSetMetadata::GetLength(int column_position) {
  //TODO Implement after the PR from column metadata is merged
  return 0;
}

std::string
driver::flight_sql::FlightSqlResultSetMetadata::GetLiteralPrefix(int column_position) {
  //TODO Implement after the PR from column metadata is merged
  return std::string();
}

std::string
driver::flight_sql::FlightSqlResultSetMetadata::GetLiteralSuffix(int column_position) {
  //TODO Implement after the PR from column metadata is merged
  return std::string();
}

std::string
driver::flight_sql::FlightSqlResultSetMetadata::GetLocalTypeName(int column_position) {
  //TODO Implement after the PR from column metadata is merged
  return std::string();
}

std::string driver::flight_sql::FlightSqlResultSetMetadata::GetName(int column_position) {
  //TODO Implement after the PR from column metadata is merged
  return std::string();
}

size_t
driver::flight_sql::FlightSqlResultSetMetadata::GetNumPrecRadix(int column_position) {
  //TODO Implement after the PR from column metadata is merged
  return 0;
}

size_t
driver::flight_sql::FlightSqlResultSetMetadata::GetOctetLength(int column_position) {
  //TODO Implement after the PR from column metadata is merged
  return 0;
}

std::string
driver::flight_sql::FlightSqlResultSetMetadata::GetTypeName(int column_position) {
  //TODO Implement after the PR from column metadata is merged
  return std::string();
}

driver::odbcabstraction::Updatability
driver::flight_sql::FlightSqlResultSetMetadata::GetUpdatable(int column_position) {
  //TODO Implement after the PR from column metadata is merged
  return odbcabstraction::UPDATABILITY_READWRITE_UNKNOWN;
}

bool driver::flight_sql::FlightSqlResultSetMetadata::IsAutoUnique(int column_position) {
  //TODO Implement after the PR from column metadata is merged
  return false;
}

bool
driver::flight_sql::FlightSqlResultSetMetadata::IsCaseSensitive(int column_position) {
  //TODO Implement after the PR from column metadata is merged
  return false;
}

driver::odbcabstraction::Searchability
driver::flight_sql::FlightSqlResultSetMetadata::IsSearchable(int column_position) {
  //TODO Implement after the PR from column metadata is merged
  return odbcabstraction::SEARCHABILITY_NONE;
}

bool driver::flight_sql::FlightSqlResultSetMetadata::IsUnsigned(int column_position) {
  //TODO Implement after the PR from column metadata is merged
  return false;
}

bool
driver::flight_sql::FlightSqlResultSetMetadata::IsFixedPrecScale(int column_position) {
  //TODO Implement after the PR from column metadata is merged
  return false;
}

driver::flight_sql::FlightSqlResultSetMetadata::FlightSqlResultSetMetadata(
  std::shared_ptr<arrow::Schema> schema) : schema(std::move(schema)) {}

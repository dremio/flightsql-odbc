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

#include <odbcabstraction/exceptions.h>
#include <utility>

namespace driver {
namespace flight_sql {

using arrow::DataType;
using arrow::Field;

size_t FlightSqlResultSetMetadata::GetColumnCount() {
  return schema->num_fields();
}

std::string FlightSqlResultSetMetadata::GetColumnName(int column_position) {
  return schema->field(column_position - 1)->name();
}

std::string FlightSqlResultSetMetadata::GetName(int column_position) {
  // TODO Get column alias from column metadata
  return schema->field(column_position - 1)->name();
}

size_t FlightSqlResultSetMetadata::GetPrecision(int column_position) {
  // TODO Implement after the PR from column metadata is merged
  return 0;
}

size_t FlightSqlResultSetMetadata::GetScale(int column_position) {
  // TODO Implement after the PR from column metadata is merged
  return 0;
}

driver::odbcabstraction::DataType
FlightSqlResultSetMetadata::GetDataType(int column_position) {
  const std::shared_ptr<Field> &field = schema->field(column_position - 1);
  const std::shared_ptr<DataType> &type = field->type();

  switch (type->id()) {
  case arrow::Type::BOOL:
    return odbcabstraction::BIT;
  case arrow::Type::UINT8:
  case arrow::Type::INT8:
    return odbcabstraction::TINYINT;
  case arrow::Type::UINT16:
  case arrow::Type::INT16:
    return odbcabstraction::SMALLINT;
  case arrow::Type::UINT32:
  case arrow::Type::INT32:
    return odbcabstraction::INTEGER;
  case arrow::Type::UINT64:
  case arrow::Type::INT64:
    return odbcabstraction::BIGINT;
  case arrow::Type::HALF_FLOAT:
  case arrow::Type::FLOAT:
    return odbcabstraction::FLOAT;
  case arrow::Type::DOUBLE:
    return odbcabstraction::DOUBLE;
  case arrow::Type::BINARY:
  case arrow::Type::FIXED_SIZE_BINARY:
  case arrow::Type::LARGE_BINARY:
    return odbcabstraction::BINARY;
  case arrow::Type::STRING:
  case arrow::Type::LARGE_STRING:
    return odbcabstraction::VARCHAR;
  case arrow::Type::DATE32:
  case arrow::Type::DATE64:
  case arrow::Type::TIMESTAMP:
    return odbcabstraction::DATETIME;
  case arrow::Type::DECIMAL128:
  case arrow::Type::DECIMAL256:
    return odbcabstraction::DECIMAL;
  case arrow::Type::TIME32:
  case arrow::Type::TIME64:
    return odbcabstraction::TIME;

  // TODO: Handle remaining types.
  case arrow::Type::INTERVAL_MONTHS:
  case arrow::Type::INTERVAL_DAY_TIME:
  case arrow::Type::INTERVAL_MONTH_DAY_NANO:
  case arrow::Type::LIST:
  case arrow::Type::STRUCT:
  case arrow::Type::SPARSE_UNION:
  case arrow::Type::DENSE_UNION:
  case arrow::Type::DICTIONARY:
  case arrow::Type::MAP:
  case arrow::Type::EXTENSION:
  case arrow::Type::FIXED_SIZE_LIST:
  case arrow::Type::DURATION:
  case arrow::Type::LARGE_LIST:
  case arrow::Type::MAX_ID:
  case arrow::Type::NA:
    break;
  }

  throw driver::odbcabstraction::DriverException("Unsupported data type: " +
                                                 type->ToString());
}

driver::odbcabstraction::Nullability
FlightSqlResultSetMetadata::IsNullable(int column_position) {
  // TODO Implement after the PR from column metadata is merged
  return odbcabstraction::NULLABILITY_NO_NULLS;
}

std::string FlightSqlResultSetMetadata::GetSchemaName(int column_position) {
  // TODO Implement after the PR from column metadata is merged
  return "";
}

std::string FlightSqlResultSetMetadata::GetCatalogName(int column_position) {
  // TODO Implement after the PR from column metadata is merged
  return "";
}

std::string FlightSqlResultSetMetadata::GetTableName(int column_position) {
  // TODO Implement after the PR from column metadata is merged
  return "";
}

std::string FlightSqlResultSetMetadata::GetColumnLabel(int column_position) {
  // TODO Implement after the PR from column metadata is merged
  return "";
}

size_t FlightSqlResultSetMetadata::GetColumnDisplaySize(
    // TODO Implement after the PR from column metadata is merged
    int column_position) {
  return 0;
}

std::string FlightSqlResultSetMetadata::GetBaseColumnName(int column_position) {
  // TODO Implement after the PR from column metadata is merged
  return "";
}

std::string FlightSqlResultSetMetadata::GetBaseTableName(int column_position) {
  // TODO Implement after the PR from column metadata is merged
  return "";
}

std::string FlightSqlResultSetMetadata::GetConciseType(int column_position) {
  // TODO Implement after the PR from column metadata is merged
  return "";
}

size_t FlightSqlResultSetMetadata::GetLength(int column_position) {
  // TODO Implement after the PR from column metadata is merged
  return 0;
}

std::string FlightSqlResultSetMetadata::GetLiteralPrefix(int column_position) {
  // TODO Implement after the PR from column metadata is merged
  return "";
}

std::string FlightSqlResultSetMetadata::GetLiteralSuffix(int column_position) {
  // TODO Implement after the PR from column metadata is merged
  return "";
}

std::string FlightSqlResultSetMetadata::GetLocalTypeName(int column_position) {
  // TODO Implement after the PR from column metadata is merged
  return "";
}

size_t FlightSqlResultSetMetadata::GetNumPrecRadix(int column_position) {
  // TODO Implement after the PR from column metadata is merged
  return 0;
}

size_t FlightSqlResultSetMetadata::GetOctetLength(int column_position) {
  // TODO Implement after the PR from column metadata is merged
  return 0;
}

std::string FlightSqlResultSetMetadata::GetTypeName(int column_position) {
  // TODO Implement after the PR from column metadata is merged
  return "";
}

driver::odbcabstraction::Updatability
FlightSqlResultSetMetadata::GetUpdatable(int column_position) {
  // TODO Implement after the PR from column metadata is merged
  return odbcabstraction::UPDATABILITY_READWRITE_UNKNOWN;
}

bool FlightSqlResultSetMetadata::IsAutoUnique(int column_position) {
  // TODO Implement after the PR from column metadata is merged
  return false;
}

bool FlightSqlResultSetMetadata::IsCaseSensitive(int column_position) {
  // TODO Implement after the PR from column metadata is merged
  return false;
}

driver::odbcabstraction::Searchability
FlightSqlResultSetMetadata::IsSearchable(int column_position) {
  // TODO Implement after the PR from column metadata is merged
  return odbcabstraction::SEARCHABILITY_NONE;
}

bool FlightSqlResultSetMetadata::IsUnsigned(int column_position) {
  // TODO Implement after the PR from column metadata is merged
  return false;
}

bool FlightSqlResultSetMetadata::IsFixedPrecScale(int column_position) {
  // TODO Implement after the PR from column metadata is merged
  return false;
}

FlightSqlResultSetMetadata::FlightSqlResultSetMetadata(
    std::shared_ptr<arrow::Schema> schema)
    : schema(std::move(schema)) {}

} // namespace flight_sql
} // namespace driver
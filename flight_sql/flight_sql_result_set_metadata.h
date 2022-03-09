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

#include <arrow/flight/types.h>
#include <arrow/type.h>
#include <odbcabstraction/result_set_metadata.h>

namespace driver {
namespace flight_sql {
class FlightSqlResultSetMetadata : public odbcabstraction::ResultSetMetadata {
private:
  std::shared_ptr<arrow::Schema> schema_;

public:
  explicit FlightSqlResultSetMetadata(
      const std::shared_ptr<arrow::flight::FlightInfo> &flight_info);

  explicit FlightSqlResultSetMetadata(std::shared_ptr<arrow::Schema> schema);

  size_t GetColumnCount() override;

  std::string GetColumnName(int column_position) override;

  size_t GetPrecision(int column_position) override;

  size_t GetScale(int column_position) override;

  odbcabstraction::SqlDataType GetDataType(int column_position) override;

  odbcabstraction::Nullability IsNullable(int column_position) override;

  std::string GetSchemaName(int column_position) override;

  std::string GetCatalogName(int column_position) override;

  std::string GetTableName(int column_position) override;

  std::string GetColumnLabel(int column_position) override;

  size_t GetColumnDisplaySize(int column_position) override;

  std::string GetBaseColumnName(int column_position) override;

  std::string GetBaseTableName(int column_position) override;

  std::string GetConciseType(int column_position) override;

  size_t GetLength(int column_position) override;

  std::string GetLiteralPrefix(int column_position) override;

  std::string GetLiteralSuffix(int column_position) override;

  std::string GetLocalTypeName(int column_position) override;

  std::string GetName(int column_position) override;

  size_t GetNumPrecRadix(int column_position) override;

  size_t GetOctetLength(int column_position) override;

  std::string GetTypeName(int column_position) override;

  odbcabstraction::Updatability GetUpdatable(int column_position) override;

  bool IsAutoUnique(int column_position) override;

  bool IsCaseSensitive(int column_position) override;

  odbcabstraction::Searchability IsSearchable(int column_position) override;

  bool IsUnsigned(int column_position) override;

  bool IsFixedPrecScale(int column_position) override;
};
} // namespace flight_sql
} // namespace driver

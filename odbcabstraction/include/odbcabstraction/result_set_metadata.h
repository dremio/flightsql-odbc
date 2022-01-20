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

#include "result_set_metadata.h"

#pragma once

namespace driver {
namespace odbcabstraction {

/// \brief High Level representation of the ResultSetMetadata from ODBC.
class ResultSetMetadata {
  /// \brief It returns the total amount of the columns in the ResultSet.
  /// \return the amount of columns.
  virtual size_t GetColumnCount() = 0;

  /// \brief It retrieves the name of a specific column.
  /// \param columnPosition[in] the position of the column.
  /// \return the column name.
  virtual std::string GetColumnName(unsigned int columnPosition) = 0;

  /// \brief It retrieves the size of a specific column.
  /// \param columnPosition[in] the position of the column.
  /// \return the column size.
  virtual size_t GetPrecision(unsigned int columnPosition) = 0;

  /// \brief It retrieves the total of number of decimal digits.
  /// \param columnPosition[in] the position of the column.
  /// \return amount of decimal digits.
  virtual size_t GetScale(unsigned int columnPosition) = 0;

  /// \brief It retrieves the SQL_DATA_TYPE of the column.
  /// \param columnPosition[in] the position of the column.
  /// \return the SQL_DATA_TYPE
  virtual DataType GetDataType(unsigned int columnPosition) = 0;

  /// \brief It returns a boolean value indicating if the column can have
  ///        null values.
  /// \param columnPosition[in] the position of the column.
  /// \return boolean values for nullability.
  virtual bool IsNullable(unsigned int columnPosition) = 0;

  /// \brief It returns the Schema name for a specific column.
  /// \return the Schema Name.
  virtual std::string GetSchemaName() = 0;

  /// \brief It returns the Catalog Name for a specific column.
  /// \return the catalog Name.
  virtual std::string GetCatalogName() = 0;

  /// \brief It returns the Table Name for a specific column.
  /// \return the Table Name.
  virtual std::string GetTableName() = 0;

  /// \brief It retrieves the column label.
  /// \param columnPosition[in] the position of the column.
  /// \return column label.
  virtual std::string GetColumnLabel(unsigned int columnPosition) = 0;

  /// \brief It retrieves the designated column's normal maximum width in characters.
  /// \param columnPosition[in] the position of the column.
  /// \return column normal maximum width.
  virtual size_t GetColumnDisplaySize(unsigned int columnPosition) = 0;

  /// \brief It returns a boolean value indicating if the column can have
  ///        null values.
  /// \param columnPosition[in] the position of the column.
  /// \return boolean values for nullability.
  virtual bool IsNullable(unsigned int columnPosition) = 0;

  /// \brief It returns a boolean value indicating if the column is auto
  ///        auto incremental.
  /// \param columnPosition[in] the position of the column.
  /// \return boolean values if column is auto incremental.
  virtual bool IsAutoIncremental(unsigned int columnPosition) = 0;

  /// \brief It returns a boolean value indicating if the column is
  ///        case sensitive.
  /// \param columnPosition[in] the position of the column.
  /// \return boolean values if column is case sensitive.
  virtual bool IsCaseSensitive(unsigned int columnPosition) = 0;

  /// \brief It returns a boolean value indicating if the column can be used
  ///        in where clauses.
  /// \param columnPosition[in] the position of the column.
  /// \return boolean values if column can be used in where clauses.
  virtual bool IsSearchable(unsigned int columnPosition) = 0;

  /// \brief It checks if a numeric column is signed or unsigned.
  /// \param columnPosition[in] the position of the column.
  /// \return check if the column is signed or not.
  virtual bool IsSigned(unsigned int columnPosition) = 0;
};

} // namespace odbcabstraction
} // namespace driver

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

#include <odbcabstraction/types.h>
#include <string>

namespace driver {
namespace odbcabstraction {

/// \brief High Level representation of the ResultSetMetadata from ODBC.
class ResultSetMetadata {
  /// \brief It returns the total amount of the columns in the ResultSet.
  /// \return the amount of columns.
  virtual size_t GetColumnCount() = 0;

  /// \brief It retrieves the name of a specific column.
  /// \param column_position[in] the position of the column.
  /// \return the column name.
  virtual std::string GetColumnName(int column_position) = 0;

  /// \brief It retrieves the size of a specific column.
  /// \param column_position[in] the position of the column.
  /// \return the column size.
  virtual size_t GetPrecision(int column_position) = 0;

  /// \brief It retrieves the total of number of decimal digits.
  /// \param column_position[in] the position of the column.
  /// \return amount of decimal digits.
  virtual size_t GetScale(int column_position) = 0;

  /// \brief It retrieves the SQL_DATA_TYPE of the column.
  /// \param column_position[in] the position of the column.
  /// \return the SQL_DATA_TYPE
  virtual DataType GetDataType(int column_position) = 0;

  /// \brief It returns a boolean value indicating if the column can have
  ///        null values.
  /// \param column_position[in] the position of the column.
  /// \return true if column is nullable.
  virtual Nullability IsNullable(int column_position) = 0;

  /// \brief It returns the Schema name for a specific column.
  /// \param column_position[in] the position of the column.
  /// \return the Schema name for given column.
  virtual std::string GetSchemaName(int column_position) = 0;

  /// \brief It returns the Catalog Name for a specific column.
  /// \param column_position[in] the position of the column.
  /// \return the catalog name for given column.
  virtual std::string GetCatalogName(int column_position) = 0;

  /// \brief It returns the Table Name for a specific column.
  /// \param column_position[in] the position of the column.
  /// \return the Table name for given column.
  virtual std::string GetTableName(int column_position) = 0;

  /// \brief It retrieves the column label.
  /// \param column_position[in] the position of the column.
  /// \return column label.
  virtual std::string GetColumnLabel(int column_position) = 0;

  /// \brief It retrieves the designated column's normal maximum width in
  /// characters. \param column_position[in] the position of the column. \return
  /// column normal maximum width.
  virtual size_t GetColumnDisplaySize(int column_position) = 0;

  /// \brief It retrieves the base name for the column.
  /// \param column_position[in] the position of the column.
  /// \return the base column name.
  virtual std::string GetBaseColumnName(int column_position) = 0;

  /// \brief It retrieves the base table name that contains the column.
  /// \param column_position[in] the position of the column.
  /// \return the base table name.
  virtual std::string GetBaseTableName(int column_position) = 0;

  /// \brief It retrieves the concise data type.
  /// \param column_position[in] the position of the column.
  /// \return the concise data type.
  virtual std::string GetConciseType(int column_position) = 0;

  /// \brief It retrieves the maximum or the actual character length
  ///        of a character string or binary data type.
  /// \param column_position[in] the position of the column.
  /// \return the maximum length
  virtual size_t GetLength(int column_position) = 0;

  /// \brief It retrieves the character or characters that the driver uses
  ///        as prefix for literal values.
  /// \param column_position[in] the position of the column.
  /// \return the prefix character(s).
  virtual std::string GetLiteralPrefix(int column_position) = 0;

  /// \brief It retrieves the character or characters that the driver uses
  ///        as prefix for literal values.
  /// \param column_position[in] the position of the column.
  /// \return the suffix character(s).
  virtual std::string GetLiteralSuffix(int column_position) = 0;

  /// \brief It retrieves the local type name for a specific column.
  /// \param column_position[in] the position of the column.
  /// \return the local type name.
  virtual std::string GetLocalTypeName(int column_position) = 0;

  /// \brief It returns the column name alias. If it has no alias
  ///        it returns the column name.
  /// \param column_position[in] the position of the column.
  /// \return the column name alias.
  virtual std::string GetName(int column_position) = 0;

  /// \brief It returns a numeric value to indicate if the data
  ///        is an approximate or exact numeric data type.
  /// \param column_position[in] the position of the column.
  virtual size_t GetNumPrecRadix(int column_position) = 0;

  /// \brief It returns the length in bytes from a string or binary data.
  /// \param column_position[in] the position of the column.
  /// \return the length in bytes.
  virtual size_t GetOctetLength(int column_position) = 0;

  /// \brief It returns the data type as a string.
  /// \param column_position[in] the position of the column.
  /// \return the data type string.
  virtual std::string GetTypeName(int column_position) = 0;

  /// \brief It returns a numeric values indicate the updatability of the
  /// column. \param column_position[in] the position of the column. \return the
  /// updatability of the column.
  virtual Updatability GetUpdatable(int column_position) = 0;

  /// \brief It returns a boolean value indicating if the column is
  /// autoincrementing. \param column_position[in] the position of the column.
  /// \return boolean values if column is auto incremental.
  virtual bool IsAutoUnique(int column_position) = 0;

  /// \brief It returns a boolean value indicating if the column is
  ///        case sensitive.
  /// \param column_position[in] the position of the column.
  /// \return boolean values if column is case sensitive.
  virtual bool IsCaseSensitive(int column_position) = 0;

  /// \brief It returns a boolean value indicating if the column can be used
  ///        in where clauses.
  /// \param column_position[in] the position of the column.
  /// \return boolean values if column can be used in where clauses.
  virtual Searchability IsSearchable(int column_position) = 0;

  /// \brief It checks if a numeric column is signed or unsigned.
  /// \param column_position[in] the position of the column.
  /// \return check if the column is signed or not.
  virtual bool IsUnsigned(int column_position) = 0;

  /// \brief It check if the columns has fixed precision and a nonzero
  ///        scale.
  /// \param column_position[in] the position of the column.
  /// \return if column has a fixed precision and non zero scale.
  virtual bool IsFixedPrecScale(int column_position) = 0;
};

} // namespace odbcabstraction
} // namespace driver

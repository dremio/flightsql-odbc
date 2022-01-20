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

namespace driver {
namespace odbcabstraction {

/// \brief High Level representation of the ResultSetMetadata from ODBC.
class ResultSetMetadata {
  //TODO There are some parameters in the flight sql metadata that the ODBC does not have, such as: IsAutoIncremental, etc;

  /// \brief It returns the total amount of the columns in the ResultSet.
  /// \return the amount of columns.
  virtual size_t GetColumnCount() = 0;

  /// \brief It retrieves the name of a specific column.
  /// \return the column name.
  virtual std::string GetColumnName() = 0;

  /// \brief It retrieves the size of a specific column.
  /// \return the column size.
  virtual size_t GetPrecision() = 0;

  /// \brief It retrieves the total of number of decimal digits.
  /// \return amount of decimal digits.
  virtual size_t GetScale() = 0;

  /// \brief It Retrieves the SQL_DATA_TYPE of the column.
  /// \return the SQL_DATA_TYPE
  // TODO check if the return type here needs to be a enum with the SQL_DATA_TYPES
  virtual size_t GetDataType() = 0;

  /// \brief It returns a boolean value indicating if the column can have
  /// null values.
  /// \return boolean values for nullability.
  virtual bool IsNullable() = 0;

  // TODO Should this interface have this getter? The ODBC does not have a function to retrieve this, it just via SQLTables.
  /// \brief It returns the Schema name for a specific column.
  /// \return the Schema Name.
  virtual std::string GetSchemaName() = 0;

  // TODO Should this interface have this getter? The ODBC does not have a function to retrieve this, it just via SQLTables.
  /// \brief It returns the Catalog Name for a specific column.
  /// \return the catalog Name.
  virtual std::string getCatalogName() = 0;
};

} // namespace odbcabstraction
} // namespace driver

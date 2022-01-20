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

#include <sql.h>
#include <sqlext.h>

namespace driver {
namespace odbcabstraction {

/// \brief Supported ODBC versions.
enum OdbcVersion { V_2, V_3, V_4 };

enum DataType {
  UNKNOWN_TYPE = SQL_UNKNOWN_TYPE,
  CHAR = SQL_CHAR,
  NUMERIC = SQL_NUMERIC,
  DECIMAL = SQL_DECIMAL,
  INTEGER = SQL_INTEGER,
  SMALLINT = SQL_SMALLINT,
  FLOAT = SQL_FLOAT,
  REAL = SQL_REAL,
  DOUBLE = SQL_DOUBLE,
  DATETIME = SQL_DATETIME,
  VARCHAR = SQL_VARCHAR,
};

enum Nullability {
  NULLABILITY_NO_NULLS = SQL_NO_NULLS,
  NULLABILITY_NULLABLE = SQL_NULLABLE,
  NULLABILITY_UNKNOWN = SQL_NULLABLE_UNKNOWN,
};

enum Searchability {
  SEARCHABILITY_NONE = SQL_PRED_NONE,
  SEARCHABILITY_LIKE_ONLY = SQL_LIKE_ONLY,
  SEARCHABILITY_ALL_EXPECT_LIKE = SQL_ALL_EXCEPT_LIKE,
  SEARCHABILITY_ALL = SQL_SEARCHABLE,
};

enum Updatability {
  UPDATABILITY_READONLY = SQL_ATTR_READONLY,
  UPDATABILITY_WRITE = SQL_ATTR_WRITE,
  UPDATABILITY_READWRITE_UNKNOWN = SQL_ATTR_READWRITE_UNKNOWN,
};

} // namespace odbcabstraction
} // namespace driver

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

/// \brief Supported ODBC versions.
enum OdbcVersion { V_2, V_3, V_4 };

// Based on ODBC sql.h and sqlext.h definitions.
enum SqlDataType {
  SQL_CHAR = 1,
  SQL_VARCHAR = 12,
  SQL_LONGVARCHAR = (-1),
  SQL_WCHAR = (-8),
  SQL_WVARCHAR = (-9),
  SQL_WLONGVARCHAR = (-10),
  SQL_DECIMAL = 3,
  SQL_NUMERIC = 2,
  SQL_SMALLINT = 5,
  SQL_INTEGER = 4,
  SQL_REAL = 7,
  SQL_FLOAT = 6,
  SQL_DOUBLE = 8,
  SQL_BIT = (-7),
  SQL_TINYINT = (-6),
  SQL_BIGINT = (-5),
  SQL_BINARY = (-2),
  SQL_VARBINARY = (-3),
  SQL_LONGVARBINARY = (-4),
  SQL_TYPE_DATE = 91,
  SQL_TYPE_TIME = 92,
  SQL_TYPE_TIMESTAMP = 93,
  SQL_INTERVAL_MONTH = (100 + 2),
  SQL_INTERVAL_YEAR = (100 + 1),
  SQL_INTERVAL_YEAR_TO_MONTH = (100 + 7),
  SQL_INTERVAL_DAY = (100 + 3),
  SQL_INTERVAL_HOUR = (100 + 4),
  SQL_INTERVAL_MINUTE = (100 + 5),
  SQL_INTERVAL_SECOND = (100 + 6),
  SQL_INTERVAL_DAY_TO_HOUR = (100 + 8),
  SQL_INTERVAL_DAY_TO_MINUTE = (100 + 9),
  SQL_INTERVAL_DAY_TO_SECOND = (100 + 10),
  SQL_INTERVAL_HOUR_TO_MINUTE = (100 + 11),
  SQL_INTERVAL_HOUR_TO_SECOND = (100 + 12),
  SQL_INTERVAL_MINUTE_TO_SECOND = (100 + 13),
  SQL_GUID = (-11),
};

// Based on ODBC sql.h and sqlext.h definitions.
enum CDataType {
  C_CHAR = 1,
  C_WCHAR = -8,
  C_SSHORT = (5 + (-20)),
  C_USHORT = (5 + (-22)),
  C_SLONG = (4 + (-20)),
  C_ULONG = (4 + (-22)),
  C_FLOAT = 7,
  C_DOUBLE = 8,
  C_BIT = -7,
  C_STINYINT = ((-6) + (-20)),
  C_UTINYINT = ((-6) + (-22)),
  C_SBIGINT = ((-5) + (-20)),
  C_UBIGINT = ((-5) + (-22)),
  C_BINARY = (-2),
};

enum Nullability {
  NULLABILITY_NO_NULLS = 0,
  NULLABILITY_NULLABLE = 1,
  NULLABILITY_UNKNOWN = 2,
};

enum Searchability {
  SEARCHABILITY_NONE = 0,
  SEARCHABILITY_LIKE_ONLY = 1,
  SEARCHABILITY_ALL_EXPECT_LIKE = 2,
  SEARCHABILITY_ALL = 3,
};

enum Updatability {
  UPDATABILITY_READONLY = 0,
  UPDATABILITY_WRITE = 1,
  UPDATABILITY_READWRITE_UNKNOWN = 2,
};

constexpr int NULL_DATA = -1;

} // namespace odbcabstraction
} // namespace driver

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
  SqlDataType_CHAR = 1,
  SqlDataType_VARCHAR = 12,
  SqlDataType_LONGVARCHAR = (-1),
  SqlDataType_WCHAR = (-8),
  SqlDataType_WVARCHAR = (-9),
  SqlDataType_WLONGVARCHAR = (-10),
  SqlDataType_DECIMAL = 3,
  SqlDataType_NUMERIC = 2,
  SqlDataType_SMALLINT = 5,
  SqlDataType_INTEGER = 4,
  SqlDataType_REAL = 7,
  SqlDataType_FLOAT = 6,
  SqlDataType_DOUBLE = 8,
  SqlDataType_BIT = (-7),
  SqlDataType_TINYINT = (-6),
  SqlDataType_BIGINT = (-5),
  SqlDataType_BINARY = (-2),
  SqlDataType_VARBINARY = (-3),
  SqlDataType_LONGVARBINARY = (-4),
  SqlDataType_TYPE_DATE = 91,
  SqlDataType_TYPE_TIME = 92,
  SqlDataType_TYPE_TIMESTAMP = 93,
  SqlDataType_INTERVAL_MONTH = (100 + 2),
  SqlDataType_INTERVAL_YEAR = (100 + 1),
  SqlDataType_INTERVAL_YEAR_TO_MONTH = (100 + 7),
  SqlDataType_INTERVAL_DAY = (100 + 3),
  SqlDataType_INTERVAL_HOUR = (100 + 4),
  SqlDataType_INTERVAL_MINUTE = (100 + 5),
  SqlDataType_INTERVAL_SECOND = (100 + 6),
  SqlDataType_INTERVAL_DAY_TO_HOUR = (100 + 8),
  SqlDataType_INTERVAL_DAY_TO_MINUTE = (100 + 9),
  SqlDataType_INTERVAL_DAY_TO_SECOND = (100 + 10),
  SqlDataType_INTERVAL_HOUR_TO_MINUTE = (100 + 11),
  SqlDataType_INTERVAL_HOUR_TO_SECOND = (100 + 12),
  SqlDataType_INTERVAL_MINUTE_TO_SECOND = (100 + 13),
  SqlDataType_GUID = (-11),
};

// Based on ODBC sql.h and sqlext.h definitions.
enum CDataType {
  CDataType_CHAR = 1,
  CDataType_WCHAR = -8,
  CDataType_SSHORT = (5 + (-20)),
  CDataType_USHORT = (5 + (-22)),
  CDataType_SLONG = (4 + (-20)),
  CDataType_ULONG = (4 + (-22)),
  CDataType_FLOAT = 7,
  CDataType_DOUBLE = 8,
  CDataType_BIT = -7,
  CDataType_STINYINT = ((-6) + (-20)),
  CDataType_UTINYINT = ((-6) + (-22)),
  CDataType_SBIGINT = ((-5) + (-20)),
  CDataType_UBIGINT = ((-5) + (-22)),
  CDataType_BINARY = (-2),
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

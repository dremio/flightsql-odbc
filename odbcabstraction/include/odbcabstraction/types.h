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

// Based on ODBC sql.h definitions.
enum DataType {
  UNKNOWN_TYPE = 0,
  CHAR = 1,
  NUMERIC = 2,
  DECIMAL = 3,
  INTEGER = 4,
  SMALLINT = 5,
  FLOAT = 6,
  REAL = 7,
  DOUBLE = 8,
  DATETIME = 9,
  VARCHAR = 12,
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

} // namespace odbcabstraction
} // namespace driver

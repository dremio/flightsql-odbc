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

#include "flight_sql_connection.h"

#include "flight_sql_auth_method.h"
#include "flight_sql_statement.h"
#include "flight_sql_stream_chunk_iterator.h"
#include "scalar_function_reporter.h"
#include <arrow/flight/client_cookie_middleware.h>
#include <arrow/array.h>
#include <arrow/array/array_nested.h>
#include <arrow/scalar.h>
#include <arrow/type_fwd.h>
#include <boost/algorithm/string/join.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/optional.hpp>
#include <iostream>
#include <mutex>
#include <odbcabstraction/exceptions.h>
#include <sqlext.h>
#include <unordered_map>

namespace driver {
namespace flight_sql {

using arrow::Result;
using arrow::Status;
using arrow::flight::FlightCallOptions;
using arrow::flight::FlightClient;
using arrow::flight::FlightClientOptions;
using arrow::flight::Location;
using arrow::flight::TimeoutDuration;
using arrow::flight::sql::FlightSqlClient;
using driver::odbcabstraction::Connection;
using driver::odbcabstraction::DriverException;
using driver::odbcabstraction::OdbcVersion;
using driver::odbcabstraction::Statement;

const std::string FlightSqlConnection::HOST = "host";
const std::string FlightSqlConnection::PORT = "port";
const std::string FlightSqlConnection::USER = "user";
const std::string FlightSqlConnection::PASSWORD = "password";
const std::string FlightSqlConnection::USE_TLS = "useTls";

// Aliases for entries in SqlInfoOptions::SqlInfo that are defined here
// due to causing compilation errors conflicting with ODBC definitions.
#define ARROW_SQL_IDENTIFIER_CASE 503
#define ARROW_SQL_IDENTIFIER_QUOTE_CHAR 504
#define ARROW_SQL_QUOTED_IDENTIFIER_CASE 505
#define ARROW_SQL_KEYWORDS 508
#define ARROW_SQL_NUMERIC_FUNCTIONS 509
#define ARROW_SQL_STRING_FUNCTIONS 510
#define ARROW_SQL_SYSTEM_FUNCTIONS 511
#define ARROW_SQL_SCHEMA_TERM 529
#define ARROW_SQL_PROCEDURE_TERM 530
#define ARROW_SQL_CATALOG_TERM 531
#define ARROW_SQL_MAX_COLUMNS_IN_GROUP_BY 544
#define ARROW_SQL_MAX_COLUMNS_IN_INDEX 545
#define ARROW_SQL_MAX_COLUMNS_IN_ORDER_BY 546
#define ARROW_SQL_MAX_COLUMNS_IN_SELECT 547
#define ARROW_SQL_MAX_COLUMNS_IN_TABLE 548
#define ARROW_SQL_MAX_ROW_SIZE 555
#define ARROW_SQL_MAX_TABLES_IN_SELECT 560

#define ARROW_CONVERT_BIGINT 0
#define ARROW_CONVERT_BINARY 1
#define ARROW_CONVERT_BIT 2
#define ARROW_CONVERT_CHAR 3
#define ARROW_CONVERT_DATE 4
#define ARROW_CONVERT_DECIMAL 5
#define ARROW_CONVERT_FLOAT 6
#define ARROW_CONVERT_INTEGER 7
#define ARROW_CONVERT_INTERVAL_DAY_TIME 8
#define ARROW_CONVERT_INTERVAL_YEAR_MONTH 9
#define ARROW_CONVERT_LONGVARBINARY 10
#define ARROW_CONVERT_LONGVARCHAR 11
#define ARROW_CONVERT_NUMERIC 12
#define ARROW_CONVERT_REAL 13
#define ARROW_CONVERT_SMALLINT 14
#define ARROW_CONVERT_TIME 15
#define ARROW_CONVERT_TIMESTAMP 16
#define ARROW_CONVERT_TINYINT 17
#define ARROW_CONVERT_VARBINARY 18
#define ARROW_CONVERT_VARCHAR 19

namespace {
// TODO: Add properties for getting the certificates
// TODO: Check if gRPC can use the system truststore, if not copy from Drill

inline void ThrowIfNotOK(const Status &status) {
  if (!status.ok()) {
    throw DriverException(status.ToString());
  }
}

Connection::ConnPropertyMap::const_iterator
TrackMissingRequiredProperty(const std::string &property,
                             const Connection::ConnPropertyMap &properties,
                             std::vector<std::string> &missing_attr) {
  Connection::ConnPropertyMap::const_iterator prop_iter =
      properties.find(property);
  if (properties.end() == prop_iter) {
    missing_attr.push_back(property);
  }
  return prop_iter;
}

// Return the corresponding field in SQLGetInfo's SQL_CONVERT_* field
// types for the given Arrow SqlConvert enum value.
//
// The caller is responsible for casting the result to a uint16. Note
// that -1 is returned if there's no corresponding entry.
int32_t GetInfoTypeForArrowConvertEntry(int32_t convert_entry) {
  switch (convert_entry) {
    case ARROW_CONVERT_BIGINT:
      return SQL_CONVERT_BIGINT;
    case ARROW_CONVERT_BINARY:
      return SQL_CONVERT_BINARY;
    case ARROW_CONVERT_BIT:
      return SQL_CONVERT_BIT;
    case ARROW_CONVERT_CHAR:
      return SQL_CONVERT_CHAR;
    case ARROW_CONVERT_DATE:
      return SQL_CONVERT_DATE;
    case ARROW_CONVERT_DECIMAL:
      return SQL_CONVERT_DECIMAL;
    case ARROW_CONVERT_FLOAT:
      return SQL_CONVERT_FLOAT;
    case ARROW_CONVERT_INTEGER:
      return SQL_CONVERT_INTEGER;
    case ARROW_CONVERT_INTERVAL_DAY_TIME:
      return SQL_CONVERT_INTERVAL_DAY_TIME;
    case ARROW_CONVERT_INTERVAL_YEAR_MONTH:
      return SQL_CONVERT_INTERVAL_YEAR_MONTH;
    case ARROW_CONVERT_LONGVARBINARY:
      return SQL_CONVERT_LONGVARBINARY;
    case ARROW_CONVERT_LONGVARCHAR:
      return SQL_CONVERT_LONGVARCHAR;
    case ARROW_CONVERT_NUMERIC:
      return SQL_CONVERT_NUMERIC;
    case ARROW_CONVERT_REAL:
      return SQL_CONVERT_REAL;
    case ARROW_CONVERT_SMALLINT:
      return SQL_CONVERT_SMALLINT;
    case ARROW_CONVERT_TIME:
      return SQL_CONVERT_TIME;
    case ARROW_CONVERT_TIMESTAMP:
      return SQL_CONVERT_TIMESTAMP;
    case ARROW_CONVERT_TINYINT:
      return SQL_CONVERT_TINYINT;
    case ARROW_CONVERT_VARBINARY:
      return SQL_CONVERT_VARBINARY;
    case ARROW_CONVERT_VARCHAR:
      return SQL_CONVERT_VARCHAR;
  }
  // Arbitrarily return a negative value
  return -1;
}

// Return the corresponding bitmask to OR in SQLGetInfo's SQL_CONVERT_* field
// value for the given Arrow SqlConvert enum value.
//
// This is _not_ a bit position, it is an integer with only a single bit set.
uint32_t GetCvtBitForArrowConvertEntry(int32_t convert_entry) {
  switch (convert_entry) {
    case ARROW_CONVERT_BIGINT:
      return SQL_CVT_BIGINT;
    case ARROW_CONVERT_BINARY:
      return SQL_CVT_BINARY;
    case ARROW_CONVERT_BIT:
      return SQL_CVT_BIT;
    case ARROW_CONVERT_CHAR:
      return SQL_CVT_CHAR;
    case ARROW_CONVERT_DATE:
      return SQL_CVT_DATE;
    case ARROW_CONVERT_DECIMAL:
      return SQL_CVT_DECIMAL;
    case ARROW_CONVERT_FLOAT:
      return SQL_CVT_FLOAT;
    case ARROW_CONVERT_INTEGER:
      return SQL_CVT_INTEGER;
    case ARROW_CONVERT_INTERVAL_DAY_TIME:
      return SQL_CVT_INTERVAL_DAY_TIME;
    case ARROW_CONVERT_INTERVAL_YEAR_MONTH:
      return SQL_CVT_INTERVAL_YEAR_MONTH;
    case ARROW_CONVERT_LONGVARBINARY:
      return SQL_CVT_LONGVARBINARY;
    case ARROW_CONVERT_LONGVARCHAR:
      return SQL_CVT_LONGVARCHAR;
    case ARROW_CONVERT_NUMERIC:
      return SQL_CVT_NUMERIC;
    case ARROW_CONVERT_REAL:
      return SQL_CVT_REAL;
    case ARROW_CONVERT_SMALLINT:
      return SQL_CVT_SMALLINT;
    case ARROW_CONVERT_TIME:
      return SQL_CVT_TIME;
    case ARROW_CONVERT_TIMESTAMP:
      return SQL_CVT_TIMESTAMP;
    case ARROW_CONVERT_TINYINT:
      return SQL_CVT_TINYINT;
    case ARROW_CONVERT_VARBINARY:
      return SQL_CVT_VARBINARY;
    case ARROW_CONVERT_VARCHAR:
      return SQL_CVT_VARCHAR;
  }
  // Return zero, which has no bits set.
  return 0;
}

} // namespace

bool FlightSqlConnection::LoadInfoFromServer() {
  if (has_server_info_.exchange(true)) {
    std::unique_lock<std::mutex> lock(mutex_);
    arrow::Result<std::shared_ptr<arrow::flight::FlightInfo>> result = sql_client_->GetSqlInfo(call_options_, {});
    ThrowIfNotOK(result.status());
    FlightStreamChunkIterator chunk_iter(*sql_client_, call_options_, result.ValueOrDie());

    FlightStreamChunk chunk;
    bool supports_correlation_name = false;
    bool requires_different_correlation_name = false;
    bool transactions_supported = false;
    bool transaction_ddl_commit = false;
    bool transaction_ddl_ignore = false;
    while (chunk_iter.GetNext(&chunk)) {
      auto name_array = chunk.data->GetColumnByName("info_name");
      auto value_array = chunk.data->GetColumnByName("value");

      arrow::UInt32Array* info_type_array = static_cast<arrow::UInt32Array*>(name_array.get());
      arrow::UnionArray* value_union_array = static_cast<arrow::UnionArray*>(value_array.get());
      for (int64_t i = 0; i < chunk.data->num_rows(); ++i) {
        if (!value_array->IsNull(i)) {
          auto info_type = static_cast<arrow::flight::sql::SqlInfoOptions::SqlInfo>(info_type_array->Value(i));
          auto result_scalar = value_union_array->GetScalar(i);
          ThrowIfNotOK(result_scalar.status());
          std::shared_ptr<arrow::Scalar> scalar_ptr = result_scalar.ValueOrDie();
          arrow::UnionScalar* scalar = reinterpret_cast<arrow::UnionScalar*>(scalar_ptr.get());
          switch (info_type) {
            // String properties
            case arrow::flight::sql::SqlInfoOptions::FLIGHT_SQL_SERVER_NAME:
            {
              std::string server_name = scalar->ToString();

              // TODO: Consider creating different properties in GetSqlInfo.
              // TODO: Investigate if SQL_SERVER_NAME should just be the host address as well.
              // In JDBC, FLIGHT_SQL_SERVER_NAME is only used for the DatabaseProductName.
              info_[SQL_SERVER_NAME] = server_name;
              info_[SQL_DBMS_NAME] = server_name;
              info_[SQL_DATABASE_NAME] = server_name; // This is usually the current catalog. May need to throw HYC00 instead.
              break;
            }
            case arrow::flight::sql::SqlInfoOptions::FLIGHT_SQL_SERVER_VERSION:
            {
              info_[SQL_DBMS_VER] = scalar->ToString();
              break;
            }
            case arrow::flight::sql::SqlInfoOptions::FLIGHT_SQL_SERVER_ARROW_VERSION:
            {
              // Unused.
              break;
            }
            case arrow::flight::sql::SqlInfoOptions::SQL_SEARCH_STRING_ESCAPE:
            {
              info_[SQL_SEARCH_PATTERN_ESCAPE] = scalar->ToString();
              break;
            }
            case ARROW_SQL_IDENTIFIER_QUOTE_CHAR:
            {
              info_[SQL_IDENTIFIER_QUOTE_CHAR] = scalar->ToString();
              break;
            }
            case ARROW_SQL_KEYWORDS:
            {
              info_[SQL_KEYWORDS] = scalar->ToString();
              break;
            }
            case arrow::flight::sql::SqlInfoOptions::SQL_EXTRA_NAME_CHARACTERS:
            {
              info_[SQL_SPECIAL_CHARACTERS] = scalar->ToString();
              break;
            }
            case ARROW_SQL_SCHEMA_TERM:
            {
              info_[SQL_SCHEMA_TERM] = scalar->ToString();
              break;
            }
            case ARROW_SQL_PROCEDURE_TERM:
            {
              info_[SQL_PROCEDURE_TERM] = scalar->ToString();
              break;
            }
            case ARROW_SQL_CATALOG_TERM:
            {
              info_[SQL_CATALOG_TERM] = scalar->ToString();

              // This property implies catalogs are supported.
              info_[SQL_CATALOG_NAME] = "Y";
              info_[SQL_CATALOG_NAME_SEPARATOR] = ".";
              break;
            }

            // Bool properties
            case arrow::flight::sql::SqlInfoOptions::FLIGHT_SQL_SERVER_READ_ONLY:
            {
              info_[SQL_DATA_SOURCE_READ_ONLY] = reinterpret_cast<arrow::BooleanScalar*>(scalar)->value ? "Y" : "N";

              // Assume all forms of insert are supported, however this should come
              // from a property.
              info_[SQL_INSERT_STATEMENT] = static_cast<uint32_t>(SQL_IS_INSERT_LITERALS | SQL_IS_INSERT_SEARCHED | SQL_IS_SELECT_INTO);
              break;
            }
            case arrow::flight::sql::SqlInfoOptions::SQL_DDL_CATALOG:
              // Unused by ODBC.
              break;
            case arrow::flight::sql::SqlInfoOptions::SQL_DDL_SCHEMA:
            {
              bool supports_schema_ddl = reinterpret_cast<arrow::BooleanScalar*>(scalar)->value;
              // Note: this is a bitmask and we can't describe cascade or restrict flags.
              info_[SQL_DROP_SCHEMA] = static_cast<uint32_t>(SQL_DS_DROP_SCHEMA);

              // Note: this is a bitmask and we can't describe authorization or collation
              info_[SQL_CREATE_SCHEMA] = static_cast<uint32_t>(SQL_CS_CREATE_SCHEMA);
              break;
            }
            case arrow::flight::sql::SqlInfoOptions::SQL_DDL_TABLE:
            {
              bool supports_table_ddl = reinterpret_cast<arrow::BooleanScalar*>(scalar)->value;
              // This is a bitmask and we cannot describe all clauses.
              info_[SQL_CREATE_TABLE] = static_cast<uint32_t>(SQL_CT_CREATE_TABLE);
              info_[SQL_DROP_TABLE] = static_cast<uint32_t>(SQL_DT_DROP_TABLE);
              break;
            }
            case arrow::flight::sql::SqlInfoOptions::SQL_ALL_TABLES_ARE_SELECTABLE:
            {
              info_[SQL_ACCESSIBLE_TABLES] = reinterpret_cast<arrow::BooleanScalar*>(scalar)->value ? "Y" : "N";
              break;
            }
            case arrow::flight::sql::SqlInfoOptions::SQL_SUPPORTS_COLUMN_ALIASING:
            {
              info_[SQL_COLUMN_ALIAS] = reinterpret_cast<arrow::BooleanScalar*>(scalar)->value ? "Y" : "N";
              break;
            }
            case arrow::flight::sql::SqlInfoOptions::SQL_NULL_PLUS_NULL_IS_NULL:
            {
              info_[SQL_CONCAT_NULL_BEHAVIOR] = static_cast<uint16_t>(reinterpret_cast<arrow::BooleanScalar*>(scalar)->value ?
                SQL_CB_NULL : SQL_CB_NON_NULL);
              break;
            }
            case arrow::flight::sql::SqlInfoOptions::SQL_SUPPORTS_TABLE_CORRELATION_NAMES:
            {
              // Simply cache SQL_SUPPORTS_TABLE_CORRELATION_NAMES and SQL_SUPPORTS_DIFFERENT_TABLE_CORRELATION_NAMES
              // since we need both properties to determine the value for SQL_CORRELATION_NAME.
              supports_correlation_name = reinterpret_cast<arrow::BooleanScalar*>(scalar)->value;
              break;
            }
            case arrow::flight::sql::SqlInfoOptions::SQL_SUPPORTS_DIFFERENT_TABLE_CORRELATION_NAMES:
            {
              // Simply cache SQL_SUPPORTS_TABLE_CORRELATION_NAMES and SQL_SUPPORTS_DIFFERENT_TABLE_CORRELATION_NAMES
              // since we need both properties to determine the value for SQL_CORRELATION_NAME.
              requires_different_correlation_name = reinterpret_cast<arrow::BooleanScalar*>(scalar)->value;
              break;
            }
            case arrow::flight::sql::SqlInfoOptions::SQL_SUPPORTS_EXPRESSIONS_IN_ORDER_BY:
            {
              info_[SQL_EXPRESSIONS_IN_ORDERBY] = reinterpret_cast<arrow::BooleanScalar*>(scalar)->value ? "Y" : "N";
              break;
            }
            case arrow::flight::sql::SqlInfoOptions::SQL_SUPPORTS_ORDER_BY_UNRELATED:
            {
              // Note: this is the negation of the Flight SQL property.
              info_[SQL_ORDER_BY_COLUMNS_IN_SELECT] = reinterpret_cast<arrow::BooleanScalar*>(scalar)->value ? "N" : "Y";
              break;
            }
            case arrow::flight::sql::SqlInfoOptions::SQL_SUPPORTS_LIKE_ESCAPE_CLAUSE:
            {
              info_[SQL_LIKE_ESCAPE_CLAUSE] = reinterpret_cast<arrow::BooleanScalar*>(scalar)->value ? "Y" : "N";
              break;
            }
            case arrow::flight::sql::SqlInfoOptions::SQL_SUPPORTS_NON_NULLABLE_COLUMNS:
            {
              info_[SQL_NON_NULLABLE_COLUMNS] = static_cast<uint16_t>(reinterpret_cast<arrow::BooleanScalar*>(scalar)->value ? SQL_NNC_NON_NULL : SQL_NNC_NULL);
              break;
            }
            case arrow::flight::sql::SqlInfoOptions::SQL_SUPPORTS_INTEGRITY_ENHANCEMENT_FACILITY:
            {
              info_[SQL_INTEGRITY] = reinterpret_cast<arrow::BooleanScalar*>(scalar)->value ? "Y" : "N";
              break;
            }
            case arrow::flight::sql::SqlInfoOptions::SQL_CATALOG_AT_START:
            {
              info_[SQL_CATALOG_LOCATION] = static_cast<uint16_t>(reinterpret_cast<arrow::BooleanScalar*>(scalar)->value ? SQL_CL_START : SQL_CL_END);
              break;
            }
            case arrow::flight::sql::SqlInfoOptions::SQL_SELECT_FOR_UPDATE_SUPPORTED:
              // Not used.
              break;
            case arrow::flight::sql::SqlInfoOptions::SQL_STORED_PROCEDURES_SUPPORTED:
            {
              info_[SQL_PROCEDURES] = reinterpret_cast<arrow::BooleanScalar*>(scalar)->value ? "Y" : "N";
              break;
            }
            case arrow::flight::sql::SqlInfoOptions::SQL_MAX_ROW_SIZE_INCLUDES_BLOBS:
            {
              info_[SQL_MAX_ROW_SIZE_INCLUDES_LONG] = reinterpret_cast<arrow::BooleanScalar*>(scalar)->value ? "Y" : "N";
              break;
            }
            case arrow::flight::sql::SqlInfoOptions::SQL_TRANSACTIONS_SUPPORTED:
            {
              transactions_supported = reinterpret_cast<arrow::BooleanScalar*>(scalar)->value;
              break;
            }
            case arrow::flight::sql::SqlInfoOptions::SQL_DATA_DEFINITION_CAUSES_TRANSACTION_COMMIT:
            {
              transaction_ddl_commit = reinterpret_cast<arrow::BooleanScalar*>(scalar)->value;
              break;
            }
            case arrow::flight::sql::SqlInfoOptions::SQL_DATA_DEFINITIONS_IN_TRANSACTIONS_IGNORED:
            {
              transaction_ddl_ignore = reinterpret_cast<arrow::BooleanScalar*>(scalar)->value;
              break;
            }
            case arrow::flight::sql::SqlInfoOptions::SQL_BATCH_UPDATES_SUPPORTED:
            {
              info_[SQL_BATCH_SUPPORT] = static_cast<uint32_t>(reinterpret_cast<arrow::BooleanScalar*>(scalar)->value ? SQL_BS_ROW_COUNT_EXPLICIT : 0);
              break;
            }
            case arrow::flight::sql::SqlInfoOptions::SQL_SAVEPOINTS_SUPPORTED:
              // Not used.
              break;
            case arrow::flight::sql::SqlInfoOptions::SQL_NAMED_PARAMETERS_SUPPORTED:
              // Not used.
              break;
            case arrow::flight::sql::SqlInfoOptions::SQL_LOCATORS_UPDATE_COPY:
              // Not used.
              break;
            case arrow::flight::sql::SqlInfoOptions::SQL_STORED_FUNCTIONS_USING_CALL_SYNTAX_SUPPORTED:
              // Not used.
              break;
            case arrow::flight::sql::SqlInfoOptions::SQL_CORRELATED_SUBQUERIES_SUPPORTED:
              // Not used. This is implied by SQL_SUPPORTED_SUBQUERIES.
              break;

            // Int64 properties
            case ARROW_SQL_IDENTIFIER_CASE:
            {
              bool is_case_insensitive = static_cast<int64_t>(reinterpret_cast<arrow::Int64Scalar*>(scalar)->value & (1 << arrow::flight::sql::SqlInfoOptions::SQL_CASE_SENSITIVITY_CASE_INSENSITIVE)) != 0;
              bool is_upper_case = static_cast<int64_t>(reinterpret_cast<arrow::Int64Scalar*>(scalar)->value & (1 << arrow::flight::sql::SqlInfoOptions::SQL_CASE_SENSITIVITY_UPPERCASE)) != 0;
              bool is_mixed_case = static_cast<int64_t>(reinterpret_cast<arrow::Int64Scalar*>(scalar)->value & (1 << arrow::flight::sql::SqlInfoOptions::SQL_CASE_SENSITIVITY_UNKNOWN)) != 0;
              
              uint16_t value = 0;
              if (!is_case_insensitive) {
                value = SQL_IC_SENSITIVE;
              } else if (is_upper_case) {
                value = SQL_IC_UPPER;
              } else if (is_mixed_case) {
                value = SQL_IC_MIXED;
              } else {
                value = SQL_IC_LOWER;
              }
              info_[SQL_IDENTIFIER_CASE] = value;
              break;
            }
            case arrow::flight::sql::SqlInfoOptions::SQL_NULL_ORDERING:
            {
              uint16_t value = 0;
              int64_t scalar_value = reinterpret_cast<arrow::Int64Scalar*>(scalar)->value;
              // This is an enum but really each value should be mutually exclusive.
              // Give higher precedence to start/end options, then high, then low.
              if ((scalar_value & (1 << arrow::flight::sql::SqlInfoOptions::SQL_NULLS_SORTED_AT_START)) != 0) {
                value = SQL_NC_START;
              } else if ((scalar_value & (1 << arrow::flight::sql::SqlInfoOptions::SQL_NULLS_SORTED_AT_END)) != 0) {
                value = SQL_NC_END;
              } else if ((scalar_value & (1 << arrow::flight::sql::SqlInfoOptions::SQL_NULLS_SORTED_HIGH)) != 0) {
                value = SQL_NC_HIGH;
              } else {
                value = SQL_NC_LOW;
              }
              info_[SQL_NULL_COLLATION] = value;
              break;
            }
            case ARROW_SQL_QUOTED_IDENTIFIER_CASE:
            {
              bool is_case_insensitive = static_cast<int64_t>(reinterpret_cast<arrow::Int64Scalar*>(scalar)->value & (1 << arrow::flight::sql::SqlInfoOptions::SQL_CASE_SENSITIVITY_CASE_INSENSITIVE)) != 0;
              bool is_upper_case = static_cast<int64_t>(reinterpret_cast<arrow::Int64Scalar*>(scalar)->value & (1 << arrow::flight::sql::SqlInfoOptions::SQL_CASE_SENSITIVITY_UPPERCASE)) != 0;
              bool is_mixed_case = static_cast<int64_t>(reinterpret_cast<arrow::Int64Scalar*>(scalar)->value & (1 << arrow::flight::sql::SqlInfoOptions::SQL_CASE_SENSITIVITY_UNKNOWN)) != 0;
              
              uint16_t value = 0;
              if (!is_case_insensitive) {
                value = SQL_IC_SENSITIVE;
              } else if (is_upper_case) {
                value = SQL_IC_UPPER;
              } else if (is_mixed_case) {
                value = SQL_IC_MIXED;
              } else {
                value = SQL_IC_LOWER;
              }
              info_[SQL_QUOTED_IDENTIFIER_CASE] = value;
              break;
            }
            case arrow::flight::sql::SqlInfoOptions::SQL_MAX_BINARY_LITERAL_LENGTH:
            {
              info_[SQL_MAX_BINARY_LITERAL_LEN] = static_cast<uint32_t>(reinterpret_cast<arrow::Int64Scalar*>(scalar)->value);
              break;
            }
            case arrow::flight::sql::SqlInfoOptions::SQL_MAX_CHAR_LITERAL_LENGTH:
            {
              info_[SQL_MAX_CHAR_LITERAL_LEN] = static_cast<uint32_t>(reinterpret_cast<arrow::Int64Scalar*>(scalar)->value);
              break;
            }
            case arrow::flight::sql::SqlInfoOptions::SQL_MAX_COLUMN_NAME_LENGTH:
            {
              info_[SQL_MAX_COLUMN_NAME_LEN] = static_cast<uint16_t>(reinterpret_cast<arrow::Int64Scalar*>(scalar)->value);
              break;
            }
            case ARROW_SQL_MAX_COLUMNS_IN_GROUP_BY:
            {
              info_[SQL_MAX_COLUMNS_IN_GROUP_BY] = static_cast<uint16_t>(reinterpret_cast<arrow::Int64Scalar*>(scalar)->value);
              break;
            }
            case ARROW_SQL_MAX_COLUMNS_IN_INDEX:
            {
              info_[SQL_MAX_COLUMNS_IN_INDEX] = static_cast<uint16_t>(reinterpret_cast<arrow::Int64Scalar*>(scalar)->value);
              break;
            }
            case ARROW_SQL_MAX_COLUMNS_IN_ORDER_BY:
            {
              info_[SQL_MAX_COLUMNS_IN_ORDER_BY] = static_cast<uint16_t>(reinterpret_cast<arrow::Int64Scalar*>(scalar)->value);
              break;
            }
            case ARROW_SQL_MAX_COLUMNS_IN_SELECT:
            {
              info_[SQL_MAX_COLUMNS_IN_SELECT] = static_cast<uint16_t>(reinterpret_cast<arrow::Int64Scalar*>(scalar)->value);
              break;
            }
            case ARROW_SQL_MAX_COLUMNS_IN_TABLE:
            {
              info_[SQL_MAX_COLUMNS_IN_TABLE] = static_cast<uint16_t>(reinterpret_cast<arrow::Int64Scalar*>(scalar)->value);
              break;
            }
            case arrow::flight::sql::SqlInfoOptions::SQL_MAX_CONNECTIONS:
            {
              info_[SQL_MAX_DRIVER_CONNECTIONS] = static_cast<uint16_t>(reinterpret_cast<arrow::Int64Scalar*>(scalar)->value);
              break;
            }
            case arrow::flight::sql::SqlInfoOptions::SQL_MAX_CURSOR_NAME_LENGTH:
            {
              info_[SQL_MAX_CURSOR_NAME_LEN] = static_cast<uint16_t>(reinterpret_cast<arrow::Int64Scalar*>(scalar)->value);
              break;
            }
            case arrow::flight::sql::SqlInfoOptions::SQL_MAX_INDEX_LENGTH:
            {
              info_[SQL_MAX_INDEX_SIZE] = static_cast<uint32_t>(reinterpret_cast<arrow::Int64Scalar*>(scalar)->value);
              break;
            }
            case arrow::flight::sql::SqlInfoOptions::SQL_SCHEMA_NAME_LENGTH:
            {
              info_[SQL_MAX_SCHEMA_NAME_LEN] = static_cast<uint16_t>(reinterpret_cast<arrow::Int64Scalar*>(scalar)->value);
              break;
            }
            case arrow::flight::sql::SqlInfoOptions::SQL_MAX_PROCEDURE_NAME_LENGTH:
            {
              info_[SQL_MAX_SCHEMA_NAME_LEN] = static_cast<uint16_t>(reinterpret_cast<arrow::Int64Scalar*>(scalar)->value);
              break;
            }
            case arrow::flight::sql::SqlInfoOptions::SQL_MAX_CATALOG_NAME_LENGTH:
            {
              info_[SQL_MAX_CATALOG_NAME_LEN] = static_cast<uint16_t>(reinterpret_cast<arrow::Int64Scalar*>(scalar)->value);
              break;
            }
            case ARROW_SQL_MAX_ROW_SIZE:
            {
              info_[SQL_MAX_ROW_SIZE] = static_cast<uint32_t>(reinterpret_cast<arrow::Int64Scalar*>(scalar)->value);
              break;
            }
            case arrow::flight::sql::SqlInfoOptions::SQL_MAX_STATEMENT_LENGTH:
            {
              info_[SQL_MAX_STATEMENT_LEN] = static_cast<uint32_t>(reinterpret_cast<arrow::Int64Scalar*>(scalar)->value);
              break;
            }
            case arrow::flight::sql::SqlInfoOptions::SQL_MAX_STATEMENTS:
            {
              info_[SQL_MAX_CONCURRENT_ACTIVITIES] = static_cast<uint16_t>(reinterpret_cast<arrow::Int64Scalar*>(scalar)->value);
              break;
            }
            case arrow::flight::sql::SqlInfoOptions::SQL_MAX_TABLE_NAME_LENGTH:
            {
              info_[SQL_MAX_TABLE_NAME_LEN] = static_cast<uint16_t>(reinterpret_cast<arrow::Int64Scalar*>(scalar)->value);
              break;
            }
            case ARROW_SQL_MAX_TABLES_IN_SELECT:
            {
              info_[SQL_MAX_TABLES_IN_SELECT] = static_cast<uint16_t>(reinterpret_cast<arrow::Int64Scalar*>(scalar)->value);
              break;
            }
            case arrow::flight::sql::SqlInfoOptions::SQL_MAX_USERNAME_LENGTH:
            {
              info_[SQL_MAX_USER_NAME_LEN] = static_cast<uint16_t>(reinterpret_cast<arrow::Int64Scalar*>(scalar)->value);
              break;
            }
            case arrow::flight::sql::SqlInfoOptions::SQL_DEFAULT_TRANSACTION_ISOLATION:
            {
              constexpr int32_t NONE = 0;
              constexpr int32_t READ_UNCOMMITTED = 1;
              constexpr int32_t READ_COMMITTED = 2;
              constexpr int32_t REPEATABLE_READ = 3;
              constexpr int32_t SERIALIZABLE = 4;
              int64_t scalar_value = static_cast<uint64_t>(reinterpret_cast<arrow::Int64Scalar*>(scalar)->value);
              uint32_t result = 0;
              if ((scalar_value & (1 << READ_UNCOMMITTED)) != 0) {
                result = SQL_TXN_READ_UNCOMMITTED;
              } else if ((scalar_value & (1 << READ_COMMITTED)) != 0) {
                result = SQL_TXN_READ_COMMITTED;
              } else if ((scalar_value & (1 << REPEATABLE_READ)) != 0) {
                result = SQL_TXN_REPEATABLE_READ;
              } else if ((scalar_value & (1 << SERIALIZABLE)) != 0) {
                result = SQL_TXN_SERIALIZABLE;
              }
              info_[SQL_DEFAULT_TXN_ISOLATION] = result;
              break;
            }

            // Int32 properties
            case arrow::flight::sql::SqlInfoOptions::SQL_SUPPORTED_GROUP_BY:
            {
              // Note: SqlGroupBy enum is missing in C++. Using Java values.
              constexpr int32_t UNRELATED = 0;
              constexpr int32_t BEYOND_SELECT = 1;
              int32_t scalar_value = static_cast<int32_t>(reinterpret_cast<arrow::Int32Scalar*>(scalar)->value);
              uint16_t result = SQL_GB_NOT_SUPPORTED;
              if ((scalar_value & (1 << UNRELATED)) != 0) {
                result = SQL_GB_NO_RELATION;
              } else if ((scalar_value & (1 << BEYOND_SELECT)) != 0) {
                result = SQL_GB_GROUP_BY_CONTAINS_SELECT;
              }
              // Note GROUP_BY_EQUALS_SELECT and COLLATE cannot be described.
              info_[SQL_GROUP_BY] = result;
              break;            
            }
            case arrow::flight::sql::SqlInfoOptions::SQL_SUPPORTED_GRAMMAR:
            {
              // Note: SupportedSqlGrammar enum is missing in C++. Using Java values.
              constexpr int32_t MINIMUM = 0;
              constexpr int32_t CORE = 1;
              constexpr int32_t EXTENDED = 2;
              int32_t scalar_value = static_cast<int32_t>(reinterpret_cast<arrow::Int32Scalar*>(scalar)->value);
              uint32_t result = SQL_OIC_CORE;
              if ((scalar_value & (1 << MINIMUM)) != 0) {
                result = SQL_OIC_CORE;
              } else if ((scalar_value & (1 << CORE)) != 0) {
                result = SQL_OIC_LEVEL1;
              } else if ((scalar_value & (1 << EXTENDED)) != 0) {
                result = SQL_OIC_LEVEL2;
              }
              info_[SQL_ODBC_API_CONFORMANCE] = result;
              break;
            }
            case arrow::flight::sql::SqlInfoOptions::SQL_ANSI92_SUPPORTED_LEVEL:
            {
              // Note: SupportedAnsi92SqlGrammarLevel enum is missing in C++. Using Java values.
              constexpr int32_t ENTRY = 0;
              constexpr int32_t INTERMEDIATE = 1;
              constexpr int32_t FULL = 2;
              int32_t scalar_value = static_cast<int32_t>(reinterpret_cast<arrow::Int32Scalar*>(scalar)->value);
              uint32_t result = SQL_SC_SQL92_ENTRY;
              uint16_t odbc_sql_conformance = SQL_OSC_MINIMUM;
              if ((scalar_value & (1 << ENTRY)) != 0) {
                result = SQL_SC_SQL92_ENTRY;
              } else if ((scalar_value & (1 << INTERMEDIATE)) != 0) {
                result = SQL_SC_SQL92_INTERMEDIATE;
                odbc_sql_conformance = SQL_OSC_CORE;
              } else if ((scalar_value & (1 << FULL)) != 0) {
                result = SQL_SC_SQL92_FULL;
                odbc_sql_conformance = SQL_OSC_EXTENDED;
              }
              info_[SQL_SQL_CONFORMANCE] = result;
              info_[SQL_ODBC_SQL_CONFORMANCE] = odbc_sql_conformance;
              break;
            }
            case arrow::flight::sql::SqlInfoOptions::SQL_OUTER_JOINS_SUPPORT_LEVEL:
            {
              int32_t scalar_value = static_cast<int32_t>(reinterpret_cast<arrow::Int32Scalar*>(scalar)->value);

              // If limited outer joins is supported, we can't tell which joins are supported so just report none.
              // If full outer joins is supported, nested joins are supported and full outer joins are supported,
              // so all joins + nested are supported.
              constexpr int32_t UNSUPPORTED = 0;
              constexpr int32_t LIMITED = 1;
              constexpr int32_t FULL = 2;
              uint32_t result = 0;
              // Assume inner and cross joins are supported. Flight SQL can't report this currently.
              uint32_t relational_operators = SQL_SRJO_CROSS_JOIN | SQL_SRJO_INNER_JOIN;
              if ((scalar_value & (1 << FULL)) != 0) {
                result = SQL_OJ_LEFT | SQL_OJ_RIGHT | SQL_OJ_FULL | SQL_OJ_NESTED;
                relational_operators |= SQL_SRJO_FULL_OUTER_JOIN | SQL_SRJO_LEFT_OUTER_JOIN | SQL_SRJO_RIGHT_OUTER_JOIN;
              } else if ((scalar_value & (1 << LIMITED)) != 0) {
                result = SQL_SC_SQL92_INTERMEDIATE;
              } else if ((scalar_value & (1 << UNSUPPORTED)) != 0) {
                result = 0;
              } 
              info_[SQL_OJ_CAPABILITIES] = result;
              info_[SQL_OUTER_JOINS] = result != 0 ? "Y" : "N";
              info_[SQL_SQL92_RELATIONAL_JOIN_OPERATORS] = relational_operators;
              break;
            }
            case arrow::flight::sql::SqlInfoOptions::SQL_SCHEMAS_SUPPORTED_ACTIONS:
            {
              int32_t scalar_value = static_cast<int32_t>(reinterpret_cast<arrow::Int32Scalar*>(scalar)->value);

              // Missing SqlSupportedElementActions enum in C++. Values taken from java.
              constexpr int32_t PROCEDURE = 0;
              constexpr int32_t INDEX = 1;
              constexpr int32_t PRIVILEGE = 2;
              // Assume schemas are supported in DML and Table manipulation.
              uint32_t result = SQL_SU_DML_STATEMENTS | SQL_SU_TABLE_DEFINITION;
              if ((scalar_value & (1 << PROCEDURE)) != 0) {
                result |= SQL_SU_PROCEDURE_INVOCATION;
              }
              if ((scalar_value & (1 << INDEX)) != 0) {
                result |= SQL_SU_INDEX_DEFINITION;
              }
              if ((scalar_value & (1 << PRIVILEGE)) != 0) {
                result |= SQL_SU_PRIVILEGE_DEFINITION;
              }
              info_[SQL_SCHEMA_USAGE] = result;
              break;
            }
            case arrow::flight::sql::SqlInfoOptions::SQL_CATALOGS_SUPPORTED_ACTIONS:
            {
              int32_t scalar_value = static_cast<int32_t>(reinterpret_cast<arrow::Int32Scalar*>(scalar)->value);

              // Missing SqlSupportedElementActions enum in C++. Values taken from java.
              constexpr int32_t PROCEDURE = 0;
              constexpr int32_t INDEX = 1;
              constexpr int32_t PRIVILEGE = 2;
              // Assume catalogs are supported in DML and Table manipulation.
              uint32_t result = SQL_CU_DML_STATEMENTS | SQL_CU_TABLE_DEFINITION;
              if ((scalar_value & (1 << PROCEDURE)) != 0) {
                result |= SQL_CU_PROCEDURE_INVOCATION;
              }
              if ((scalar_value & (1 << INDEX)) != 0) {
                result |= SQL_CU_INDEX_DEFINITION;
              }
              if ((scalar_value & (1 << PRIVILEGE)) != 0) {
                result |= SQL_CU_PRIVILEGE_DEFINITION;
              }
              info_[SQL_CATALOG_USAGE] = result;
              break;
            }
            case arrow::flight::sql::SqlInfoOptions::SQL_SUPPORTED_POSITIONED_COMMANDS:
            {
              // Ignore, positioned updates/deletes unsupported.
              break;
            }
            case arrow::flight::sql::SqlInfoOptions::SQL_SUPPORTED_SUBQUERIES:
            {
              int32_t scalar_value = static_cast<int32_t>(reinterpret_cast<arrow::Int32Scalar*>(scalar)->value);

              // Missing SqlSupportedElementActions enum in C++. Values taken from java.
              constexpr int32_t COMPARISONS = 0;
              constexpr int32_t EXISTS = 1;
              constexpr int32_t IN = 2;
              constexpr int32_t QUANTIFIEDS = 3;
              uint32_t result = 0;
              if ((scalar_value & (1 << COMPARISONS)) != 0) {
                result |= SQL_SQ_COMPARISON;
              }
              if ((scalar_value & (1 << EXISTS)) != 0) {
                result |= SQL_SQ_EXISTS;
              }
              if ((scalar_value & (1 << IN)) != 0) {
                result |= SQL_SQ_IN;
              }
              if ((scalar_value & (1 << QUANTIFIEDS)) != 0) {
                result |= SQL_SQ_QUANTIFIED;
              }
              info_[SQL_SUBQUERIES] = result;
              break;
            }
            case arrow::flight::sql::SqlInfoOptions::SQL_SUPPORTED_UNIONS:
            {
              int32_t scalar_value = static_cast<int32_t>(reinterpret_cast<arrow::Int32Scalar*>(scalar)->value);

              // Missing enum in C++. Values taken from java.
              constexpr int32_t UNION = 0;
              constexpr int32_t UNION_ALL = 1;
              uint32_t result = 0;
              if ((scalar_value & (1 << UNION)) != 0) {
                result |= SQL_U_UNION;
              }
              if ((scalar_value & (1 << UNION_ALL)) != 0) {
                result |= SQL_U_UNION_ALL;
              }
              info_[SQL_UNION] = result;
              break;
            }
            case arrow::flight::sql::SqlInfoOptions::SQL_SUPPORTED_TRANSACTIONS_ISOLATION_LEVELS:
            {
              int32_t scalar_value = static_cast<int32_t>(reinterpret_cast<arrow::Int32Scalar*>(scalar)->value);

              // Missing enum in C++. Values taken from java.
              constexpr int32_t NONE = 0;
              constexpr int32_t READ_UNCOMMITTED = 1;
              constexpr int32_t READ_COMMITTED = 2;
              constexpr int32_t REPEATABLE_READ = 3;
              constexpr int32_t SERIALIZABLE = 4;
              uint32_t result = 0;
              if ((scalar_value & (1 << NONE)) != 0) {
                result = 0;
              }
              if ((scalar_value & (1 << READ_UNCOMMITTED)) != 0) {
                result |= SQL_TXN_READ_UNCOMMITTED;
              }
              if ((scalar_value & (1 << READ_COMMITTED)) != 0) {
                result |= SQL_TXN_READ_COMMITTED;
              }
              if ((scalar_value & (1 << REPEATABLE_READ)) != 0) {
                result |= SQL_TXN_REPEATABLE_READ;
              }
              if ((scalar_value & (1 << SERIALIZABLE)) != 0) {
                result |= SQL_TXN_SERIALIZABLE;
              }
              info_[SQL_TXN_ISOLATION_OPTION] = result;
              break;
            }
            case arrow::flight::sql::SqlInfoOptions::SQL_SUPPORTED_RESULT_SET_TYPES:
              // Ignored. Warpdrive supports forward-only only.
              break;
            case arrow::flight::sql::SqlInfoOptions::SQL_SUPPORTED_CONCURRENCIES_FOR_RESULT_SET_UNSPECIFIED:
              // Ignored. Warpdrive supports forward-only only.
              break;
            case arrow::flight::sql::SqlInfoOptions::SQL_SUPPORTED_CONCURRENCIES_FOR_RESULT_SET_FORWARD_ONLY:
              // Ignored. Warpdrive supports forward-only only.
              break;
            case arrow::flight::sql::SqlInfoOptions::SQL_SUPPORTED_CONCURRENCIES_FOR_RESULT_SET_SCROLL_SENSITIVE:
              // Ignored. Warpdrive supports forward-only only.
              break;
            case arrow::flight::sql::SqlInfoOptions::SQL_SUPPORTED_CONCURRENCIES_FOR_RESULT_SET_SCROLL_INSENSITIVE:
              // Ignored. Warpdrive supports forward-only only.
              break;

            // List<string> properties
            case ARROW_SQL_NUMERIC_FUNCTIONS:
            {
              std::shared_ptr<arrow::Array> list_value = reinterpret_cast<arrow::BaseListScalar*>(scalar)->value;
              uint32_t result = 0;
              for (int64_t list_index = 0; list_index < list_value->length(); ++list_index) {
                if (!list_value->IsNull(list_index)) {
                  ReportNumericFunction(reinterpret_cast<arrow::StringArray*>(list_value.get())->GetString(list_index),
                    result);
                }
              }
              info_[SQL_NUMERIC_FUNCTIONS] = result;
              break;
            }

            case ARROW_SQL_STRING_FUNCTIONS:
            {
              std::shared_ptr<arrow::Array> list_value = reinterpret_cast<arrow::BaseListScalar*>(scalar)->value;
              uint32_t result = 0;
              for (int64_t list_index = 0; list_index < list_value->length(); ++list_index) {
                if (!list_value->IsNull(list_index)) {
                  ReportStringFunction(reinterpret_cast<arrow::StringArray*>(list_value.get())->GetString(list_index),
                    result);
                }
              }
              info_[SQL_STRING_FUNCTIONS] = result;
              break;
            }
            case ARROW_SQL_SYSTEM_FUNCTIONS:
            {
              std::shared_ptr<arrow::Array> list_value = reinterpret_cast<arrow::BaseListScalar*>(scalar)->value;
              uint32_t sys_result = 0;
              uint32_t convert_result = 0;
              for (int64_t list_index = 0; list_index < list_value->length(); ++list_index) {
                if (!list_value->IsNull(list_index)) {
                  ReportSystemFunction(reinterpret_cast<arrow::StringArray*>(list_value.get())->GetString(list_index),
                    sys_result, convert_result);
                }
              }
              info_[SQL_CONVERT_FUNCTIONS] = convert_result;
              info_[SQL_SYSTEM_FUNCTIONS] = sys_result;
              break;
            }
            case arrow::flight::sql::SqlInfoOptions::SQL_DATETIME_FUNCTIONS:
            {
              std::shared_ptr<arrow::Array> list_value = reinterpret_cast<arrow::BaseListScalar*>(scalar)->value;
              uint32_t result = 0;
              for (int64_t list_index = 0; list_index < list_value->length(); ++list_index) {
                if (!list_value->IsNull(list_index)) {
                  ReportDatetimeFunction(reinterpret_cast<arrow::StringArray*>(list_value.get())->GetString(list_index),
                    result);
                }
              }
              info_[SQL_TIMEDATE_FUNCTIONS] = result;
              break;
            }

            // Map<int32, list<int32> properties
            case arrow::flight::sql::SqlInfoOptions::SQL_SUPPORTS_CONVERT:
            {
              arrow::MapScalar* map_scalar = reinterpret_cast<arrow::MapScalar*>(scalar);
              auto data_array = map_scalar->value;
              arrow::StructArray* map_contents = reinterpret_cast<arrow::StructArray*>(data_array.get());
              auto map_keys = map_contents->field(0);
              auto map_values = map_contents->field(1); 
              for (int64_t map_index = 0; map_index < map_contents->length(); ++map_index) {
                if (!map_values->IsNull(map_index)) {
                  auto map_key_scalar_ptr = map_keys->GetScalar(map_index).ValueOrDie();
                  auto map_value_scalar_ptr = map_values->GetScalar(map_index).ValueOrDie();
                  int32_t map_key_scalar = reinterpret_cast<arrow::Int32Scalar*>(map_key_scalar_ptr.get())->value;
                  auto map_value_scalar = reinterpret_cast<arrow::BaseListScalar*>(map_value_scalar_ptr.get())->value;

                  int32_t info_type = GetInfoTypeForArrowConvertEntry(map_key_scalar);
                  if (info_type < 0) {
                    continue;
                  }
                  uint32_t info_to_write = 0;
                  for (int64_t map_value_array_index = 0; map_value_array_index < map_value_scalar->length(); ++ map_value_array_index) {
                    if (!map_value_scalar->IsNull(map_value_array_index)) {
                      auto list_entry_scalar = map_value_scalar->GetScalar(map_value_array_index).ValueOrDie();
                      info_to_write |= GetCvtBitForArrowConvertEntry(reinterpret_cast<arrow::Int32Scalar*>(list_entry_scalar.get())->value);
                    }
                  }
                  info_[info_type] = info_to_write;
                }
              }
              break;
            }

            default:
             // Ignore unrecognized.
             break;
          }
        }
      }

      if (transactions_supported) {
        if (transaction_ddl_commit) {
          info_[SQL_TXN_CAPABLE] = static_cast<uint16_t>(SQL_TC_DDL_COMMIT);
        } else if (transaction_ddl_ignore) {
          info_[SQL_TXN_CAPABLE] = static_cast<uint16_t>(SQL_TC_DDL_IGNORE);
        } else {
          // Ambiguous if this means transactions on DDL is supported or not. Assume not
          info_[SQL_TXN_CAPABLE] = SQL_TC_DML;
        }
      } else {
        info_[SQL_TXN_CAPABLE] = static_cast<uint16_t>(SQL_TC_NONE);
      }

      if (supports_correlation_name) {
        if (requires_different_correlation_name) {
          info_[SQL_CORRELATION_NAME] = static_cast<uint16_t>(SQL_CN_DIFFERENT);
        } else {
          info_[SQL_CORRELATION_NAME] = static_cast<uint16_t>(SQL_CN_ANY);
        }
      } else {
        info_[SQL_CORRELATION_NAME] = static_cast<uint16_t>(SQL_CN_NONE);
      }
    }
  }

  return false;
}

void FlightSqlConnection::Connect(const ConnPropertyMap &properties,
                                  std::vector<std::string> &missing_attr) {
  try {
    Location location = BuildLocation(properties, missing_attr);
    FlightClientOptions client_options =
        BuildFlightClientOptions(properties, missing_attr);

    const std::shared_ptr<arrow::flight::ClientMiddlewareFactory>
        &cookie_factory = arrow::flight::GetCookieFactory();
    client_options.middleware.push_back(cookie_factory);

    std::unique_ptr<FlightClient> flight_client;
    ThrowIfNotOK(
        FlightClient::Connect(location, client_options, &flight_client));

    std::unique_ptr<FlightSqlAuthMethod> auth_method =
        FlightSqlAuthMethod::FromProperties(flight_client, properties);
    auth_method->Authenticate(*this, call_options_);

    sql_client_.reset(new FlightSqlClient(std::move(flight_client)));

    // Note: This should likely come from Flight instead of being from the connection
    // properties to allow reporting a user for other auth mechanisms and also
    // decouple the database user from user credentials.
    info_[SQL_USER_NAME] = auth_method->GetUser();
    SetAttribute(CONNECTION_DEAD, false);

    PopulateCallOptionsFromAttributes();
  } catch (...) {
    SetAttribute(CONNECTION_DEAD, true);
    sql_client_.reset();

    throw;
  }
}

const FlightCallOptions &
FlightSqlConnection::PopulateCallOptionsFromAttributes() {
  // Set CONNECTION_TIMEOUT attribute
  const boost::optional<Connection::Attribute> &connection_timeout =
      GetAttribute(CONNECTION_TIMEOUT);
  if (connection_timeout.has_value()) {
    call_options_.timeout =
        TimeoutDuration{boost::get<double>(connection_timeout.value())};
  }

  return call_options_;
}

FlightClientOptions FlightSqlConnection::BuildFlightClientOptions(
    const ConnPropertyMap &properties, std::vector<std::string> &missing_attr) {
  FlightClientOptions options;
  // Persist state information using cookies if the FlightProducer supports it.
  options.middleware.push_back(arrow::flight::GetCookieFactory());

  // TODO: Set up TLS  properties
  return std::move(options);
}

Location
FlightSqlConnection::BuildLocation(const ConnPropertyMap &properties,
                                   std::vector<std::string> &missing_attr) {
  const auto &host_iter =
      TrackMissingRequiredProperty(HOST, properties, missing_attr);

  const auto &port_iter =
      TrackMissingRequiredProperty(PORT, properties, missing_attr);

  if (!missing_attr.empty()) {
    std::string missing_attr_str =
        std::string("Missing required properties: ") +
        boost::algorithm::join(missing_attr, ", ");
    throw DriverException(missing_attr_str);
  }

  const std::string &host = host_iter->second;
  const int &port = boost::lexical_cast<int>(port_iter->second);

  Location location;
  const auto &it_use_tls = properties.find(USE_TLS);
  if (it_use_tls != properties.end() &&
      boost::lexical_cast<bool>(it_use_tls->second)) {
    ThrowIfNotOK(Location::ForGrpcTls(host, port, &location));
  } else {
    ThrowIfNotOK(Location::ForGrpcTcp(host, port, &location));
  }
  return location;
}

void FlightSqlConnection::Close() {
  if (closed_) {
    throw DriverException("Connection already closed.");
  }

  sql_client_.reset();
  closed_ = true;
}

std::shared_ptr<Statement> FlightSqlConnection::CreateStatement() {
  return std::shared_ptr<Statement>(
      new FlightSqlStatement(*sql_client_, call_options_));
}

void FlightSqlConnection::SetAttribute(Connection::AttributeId attribute,
                                       const Connection::Attribute &value) {
  attribute_[attribute] = value;
}

boost::optional<Connection::Attribute>
FlightSqlConnection::GetAttribute(Connection::AttributeId attribute) {
  const auto &it = attribute_.find(attribute);
  return boost::make_optional(it != attribute_.end(), it->second);
}

Connection::Info FlightSqlConnection::GetInfo(uint16_t info_type) {
  auto it = info_.find(info_type);

  if (info_.end() == it) {
    if (LoadInfoFromServer()) {
      it = info_.find(info_type);
    }
    if (info_.end() == it) {
      throw DriverException("Unknown GetInfo type: " + std::to_string(info_type));
    }
  }
  return it->second;
}

FlightSqlConnection::FlightSqlConnection(OdbcVersion odbc_version)
    : odbc_version_(odbc_version),
      has_server_info_(false),
      closed_(false) {
    info_[SQL_DRIVER_NAME] = "Arrow Flight ODBC Driver";
    info_[SQL_DRIVER_VER] = "1.0.0";

    info_[SQL_GETDATA_EXTENSIONS] = static_cast<uint32_t>(SQL_GD_ANY_COLUMN | SQL_GD_ANY_ORDER);
    info_[SQL_CURSOR_SENSITIVITY] = static_cast<uint32_t>(SQL_UNSPECIFIED);

    // Properties which don't currently have SqlGetInfo fields but probably should.
    info_[SQL_ACCESSIBLE_TABLES] = "N";
    info_[SQL_COLLATION_SEQ] = "";
    info_[SQL_ALTER_TABLE] = static_cast<uint32_t>(0);
    info_[SQL_DATETIME_LITERALS] = static_cast<uint32_t>(SQL_DL_SQL92_DATE | SQL_DL_SQL92_TIME | SQL_DL_SQL92_TIMESTAMP);
    info_[SQL_CREATE_ASSERTION] = static_cast<uint32_t>(0);
    info_[SQL_CREATE_CHARACTER_SET] = static_cast<uint32_t>(0);
    info_[SQL_CREATE_COLLATION] = static_cast<uint32_t>(0);
    info_[SQL_CREATE_DOMAIN] = static_cast<uint32_t>(0);
    info_[SQL_INDEX_KEYWORDS] = static_cast<uint32_t>(SQL_IK_NONE);
    info_[SQL_TIMEDATE_ADD_INTERVALS] = static_cast<uint32_t>(
      SQL_FN_TSI_FRAC_SECOND | SQL_FN_TSI_SECOND |SQL_FN_TSI_MINUTE | SQL_FN_TSI_HOUR |
      SQL_FN_TSI_DAY | SQL_FN_TSI_WEEK | SQL_FN_TSI_MONTH | SQL_FN_TSI_QUARTER | SQL_FN_TSI_YEAR);
    info_[SQL_TIMEDATE_DIFF_INTERVALS] = static_cast<uint32_t>(
      SQL_FN_TSI_FRAC_SECOND | SQL_FN_TSI_SECOND |SQL_FN_TSI_MINUTE | SQL_FN_TSI_HOUR |
      SQL_FN_TSI_DAY | SQL_FN_TSI_WEEK | SQL_FN_TSI_MONTH | SQL_FN_TSI_QUARTER | SQL_FN_TSI_YEAR);
    info_[SQL_CURSOR_COMMIT_BEHAVIOR] = static_cast<uint16_t>(SQL_CB_CLOSE);
    info_[SQL_CURSOR_ROLLBACK_BEHAVIOR] = static_cast<uint16_t>(SQL_CB_CLOSE);
    info_[SQL_CREATE_TRANSLATION] = static_cast<uint32_t>(0);
    info_[SQL_DDL_INDEX] = static_cast<uint32_t>(0);
    info_[SQL_DROP_ASSERTION] = static_cast<uint32_t>(0);
    info_[SQL_DROP_CHARACTER_SET] = static_cast<uint32_t>(0);
    info_[SQL_DROP_COLLATION] = static_cast<uint32_t>(0);
    info_[SQL_DROP_DOMAIN] = static_cast<uint32_t>(0);
    info_[SQL_DROP_TRANSLATION] = static_cast<uint32_t>(0);
    info_[SQL_DROP_VIEW] = static_cast<uint32_t>(0);
    info_[SQL_MAX_IDENTIFIER_LEN] = static_cast<uint16_t>(65535); // arbitrary
    
    // Assume all aggregate functions reported in ODBC are supported.
    info_[SQL_AGGREGATE_FUNCTIONS] = static_cast<uint32_t>(SQL_AF_ALL | SQL_AF_AVG | SQL_AF_COUNT |
      SQL_AF_DISTINCT | SQL_AF_MAX | SQL_AF_MIN | SQL_AF_SUM);
  }

} // namespace flight_sql
} // namespace driver

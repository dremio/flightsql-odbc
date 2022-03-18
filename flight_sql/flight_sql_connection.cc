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
#include <arrow/flight/client_cookie_middleware.h>
#include <arrow/array.h>
#include <arrow/scalar.h>
#include <boost/algorithm/string/join.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/optional.hpp>
#include <iostream>
#include <mutex>
#include <odbcabstraction/exceptions.h>
#include <sqlext.h>

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

} // namespace

bool FlightSqlConnection::LoadInfoFromServer() {
  if (has_server_info_.exchange(true)) {
    std::unique_lock<std::mutex> lock(mutex_);
    auto result = sql_client_->GetSqlInfo(call_options_, {});
    ThrowIfNotOK(result.status());
    FlightStreamChunkIterator chunk_iter(*sql_client_, call_options_, result.ValueOrDie());

    FlightStreamChunk chunk;
    bool supportsCorrelationName = false;
    bool requiresDifferentCorrelationName = false;
    while (chunk_iter.GetNext(&chunk)) {
      auto name_array = chunk.data->GetColumnByName("info_name");
      auto value_array = chunk.data->GetColumnByName("value");

      arrow::UInt32Array* info_type_array = static_cast<arrow::UInt32Array*>(name_array.get());
      arrow::UnionArray* value_union_array = static_cast<arrow::UnionArray*>(value_array.get());
      for (int64_t i = 0; i < chunk.data->num_rows(); ++i) {
        if (!value_array->IsNull(i)) {
          auto info_type = static_cast<arrow::flight::sql::SqlInfoOptions::SqlInfo>(info_type_array->value(i));
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
            case ARROW_SQL_NUMERIC_FUNCTIONS:
            case ARROW_SQL_STRING_FUNCTIONS:
            case ARROW_SQL_SYSTEM_FUNCTIONS:
            case arrow::flight::sql::SqlInfoOptions::SQL_DATETIME_FUNCTIONS:
            case arrow::flight::sql::SqlInfoOptions::SQL_SEARCH_STRING_ESCAPE:
            {
              info_[SQL_SEARCH_PATTERN_ESCAPE] = scalar->ToString();
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
              break;
            }

            // Bool properties
            case arrow::flight::sql::SqlInfoOptions::FLIGHT_SQL_SERVER_READ_ONLY:
            {
              info_[SQL_DATA_SOURCE_READ_ONLY] = reinterpret_cast<arrow::BooleanScalar*>(scalar)->value ? "Y" : "N";
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
              supportsCorrelationName = reinterpret_cast<arrow::BooleanScalar*>(scalar)->value;
              break;
            }
            case arrow::flight::sql::SqlInfoOptions::SQL_SUPPORTS_DIFFERENT_TABLE_CORRELATION_NAMES:
            {
              // Simply cache SQL_SUPPORTS_TABLE_CORRELATION_NAMES and SQL_SUPPORTS_DIFFERENT_TABLE_CORRELATION_NAMES
              // since we need both properties to determine the value for SQL_CORRELATION_NAME.
              requiresDifferentCorrelationName = reinterpret_cast<arrow::BooleanScalar*>(scalar)->value;
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
              break;
            case arrow::flight::sql::SqlInfoOptions::SQL_SELECT_FOR_UPDATE_SUPPORTED:
              break;
            case arrow::flight::sql::SqlInfoOptions::SQL_STORED_PROCEDURES_SUPPORTED:
              break;
            case arrow::flight::sql::SqlInfoOptions::SQL_MAX_ROW_SIZE_INCLUDES_BLOBS:
              break;
            case arrow::flight::sql::SqlInfoOptions::SQL_TRANSACTIONS_SUPPORTED:
              break;
            case arrow::flight::sql::SqlInfoOptions::SQL_DATA_DEFINITION_CAUSES_TRANSACTION_COMMIT:
              break;
            case arrow::flight::sql::SqlInfoOptions::SQL_DATA_DEFINITIONS_IN_TRANSACTIONS_IGNORED:
              break;
            case arrow::flight::sql::SqlInfoOptions::SQL_BATCH_UPDATES_SUPPORTED:
              break;
            case arrow::flight::sql::SqlInfoOptions::SQL_SAVEPOINTS_SUPPORTED:
              break;
            case arrow::flight::sql::SqlInfoOptions::SQL_NAMED_PARAMETERS_SUPPORTED:
              break;
            case arrow::flight::sql::SqlInfoOptions::SQL_LOCATORS_UPDATE_COPY:
              break;
            case arrow::flight::sql::SqlInfoOptions::SQL_STORED_FUNCTIONS_USING_CALL_SYNTAX_SUPPORTED:
              break;

            // Int64 properties
            case ARROW_SQL_IDENTIFIER_CASE:
              break;
            case arrow::flight::sql::SqlInfoOptions::SQL_NULL_ORDERING:
              break;
            case ARROW_SQL_QUOTED_IDENTIFIER_CASE:
              break;
            case arrow::flight::sql::SqlInfoOptions::SQL_MAX_BINARY_LITERAL_LENGTH:
              break;
            case arrow::flight::sql::SqlInfoOptions::SQL_MAX_CHAR_LITERAL_LENGTH:
              break;
            case arrow::flight::sql::SqlInfoOptions::SQL_MAX_COLUMN_NAME_LENGTH:
              break;
            case ARROW_SQL_MAX_COLUMNS_IN_GROUP_BY:
              break;
            case ARROW_SQL_MAX_COLUMNS_IN_INDEX:
              break;
            case ARROW_SQL_MAX_COLUMNS_IN_ORDER_BY:
              break;
            case ARROW_SQL_MAX_COLUMNS_IN_SELECT:
              break;
            case ARROW_SQL_MAX_COLUMNS_IN_TABLE:
              break;
            case arrow::flight::sql::SqlInfoOptions::SQL_MAX_CONNECTIONS:
              break;
            case arrow::flight::sql::SqlInfoOptions::SQL_MAX_CURSOR_NAME_LENGTH:
              break;
            case arrow::flight::sql::SqlInfoOptions::SQL_MAX_INDEX_LENGTH:
              break;
            case arrow::flight::sql::SqlInfoOptions::SQL_SCHEMA_NAME_LENGTH:
              break;
            case arrow::flight::sql::SqlInfoOptions::SQL_MAX_PROCEDURE_NAME_LENGTH:
              break;
            case arrow::flight::sql::SqlInfoOptions::SQL_MAX_CATALOG_NAME_LENGTH:
              break;
            case ARROW_SQL_MAX_ROW_SIZE:
              break;
            case arrow::flight::sql::SqlInfoOptions::SQL_MAX_STATEMENT_LENGTH:
              break;
            case arrow::flight::sql::SqlInfoOptions::SQL_MAX_STATEMENTS:
              break;
            case arrow::flight::sql::SqlInfoOptions::SQL_MAX_TABLE_NAME_LENGTH:
              break;
            case ARROW_SQL_MAX_TABLES_IN_SELECT:
              break;
            case arrow::flight::sql::SqlInfoOptions::SQL_MAX_USERNAME_LENGTH:
              break;
            case arrow::flight::sql::SqlInfoOptions::SQL_DEFAULT_TRANSACTION_ISOLATION:
              break;

            // Int32 properties
            case arrow::flight::sql::SqlInfoOptions::SQL_SUPPORTED_GROUP_BY:
              break;            
            case arrow::flight::sql::SqlInfoOptions::SQL_SUPPORTED_GRAMMAR:
              break;
            case arrow::flight::sql::SqlInfoOptions::SQL_ANSI92_SUPPORTED_LEVEL:
              break;
            case arrow::flight::sql::SqlInfoOptions::SQL_OUTER_JOINS_SUPPORT_LEVEL:
              break;
            case arrow::flight::sql::SqlInfoOptions::SQL_SCHEMAS_SUPPORTED_ACTIONS:
              break;
            case arrow::flight::sql::SqlInfoOptions::SQL_CATALOGS_SUPPORTED_ACTIONS:
              break;
            case arrow::flight::sql::SqlInfoOptions::SQL_SUPPORTED_POSITIONED_COMMANDS:
              break;
            case arrow::flight::sql::SqlInfoOptions::SQL_SUPPORTED_SUBQUERIES:
              break;
            case arrow::flight::sql::SqlInfoOptions::SQL_CORRELATED_SUBQUERIES_SUPPORTED:
              break;
            case arrow::flight::sql::SqlInfoOptions::SQL_SUPPORTED_UNIONS:
              break;
            case arrow::flight::sql::SqlInfoOptions::SQL_SUPPORTED_TRANSACTIONS_ISOLATION_LEVELS:
              break;
            case arrow::flight::sql::SqlInfoOptions::SQL_SUPPORTED_RESULT_SET_TYPES:
              break;
            case arrow::flight::sql::SqlInfoOptions::SQL_SUPPORTED_CONCURRENCIES_FOR_RESULT_SET_UNSPECIFIED:
              break;
            case arrow::flight::sql::SqlInfoOptions::SQL_SUPPORTED_CONCURRENCIES_FOR_RESULT_SET_FORWARD_ONLY:
              break;
            case arrow::flight::sql::SqlInfoOptions::SQL_SUPPORTED_CONCURRENCIES_FOR_RESULT_SET_SCROLL_SENSITIVE:
              break;
            case arrow::flight::sql::SqlInfoOptions::SQL_SUPPORTED_CONCURRENCIES_FOR_RESULT_SET_SCROLL_INSENSITIVE:
              break;

            // Map<int32, list<int32> properties
            case arrow::flight::sql::SqlInfoOptions::SQL_SUPPORTS_CONVERT:
              break;

            default:
             // Ignore unrecognized.
             break;
          }
        }
      }
    }

    // TODO:
    // ODBC => GetSqlInfo
    // SQL_MAX_ACTIVE_STATEMENTS -> SQL_MAX_STATEMENTS
    // SQL_BATCH_ROW_COUNT -> SQL_BATCH_UPDATES_SUPPORTED
    // SQL_MAX_DRIVER_CONNECTIONS -> SQL_MAX_CONNECTIONS

    // Driver-level string properties.
    case SQL_USER_NAME:
    case SQL_SEARCH_PATTERN_ESCAPE:
    case SQL_SERVER_NAME:
    case SQL_DATA_SOURCE_READ_ONLY:
    case SQL_ACCESSIBLE_TABLES:
    case SQL_ACCESSIBLE_PROCEDURES:
    case SQL_CATALOG_TERM:
    case SQL_COLLATION_SEQ:
    case SQL_SCHEMA_TERM:
    case SQL_CATALOG_NAME:
    case SQL_CATALOG_NAME_SEPARATOR:
    case SQL_EXPRESSIONS_IN_ORDERBY:
    case SQL_IDENTIFIER_QUOTE_CHAR:
    case SQL_INTEGRITY:
    case SQL_KEYWORDS:
    case SQL_OUTER_JOINS: // Not documented in SQLGetInfo, but other drivers return Y/N strings
    case SQL_PROCEDURES:
    case SQL_SPECIAL_CHARACTERS:
    case SQL_MAX_ROW_SIZE_INCLUDES_LONG:

    // Driver-level 32-bit integer propreties.
    case SQL_INFO_SCHEMA_VIEWS:
    case SQL_AGGREGATE_FUNCTIONS:
    case SQL_ALTER_DOMAIN:
//    case SQL_ALTER_SCHEMA:
    case SQL_ALTER_TABLE:
    case SQL_DATETIME_LITERALS:
    case SQL_CATALOG_USAGE:
    case SQL_CREATE_ASSERTION:
    case SQL_CREATE_CHARACTER_SET:
    case SQL_CREATE_COLLATION:
    case SQL_CREATE_DOMAIN:
    case SQL_CREATE_SCHEMA:
    case SQL_CREATE_TABLE:
    case SQL_INDEX_KEYWORDS:
    case SQL_INSERT_STATEMENT:
    case SQL_LIKE_ESCAPE_CLAUSE:
    case SQL_OJ_CAPABILITIES:
    case SQL_ORDER_BY_COLUMNS_IN_SELECT:
    case SQL_SCHEMA_USAGE:
    case SQL_SQL_CONFORMANCE:
    case SQL_SUBQUERIES:
    case SQL_UNION:
    case SQL_MAX_BINARY_LITERAL_LEN:
    case SQL_MAX_CHAR_LITERAL_LEN:
    case SQL_MAX_ROW_SIZE:
    case SQL_MAX_STATEMENT_LEN:
    case SQL_CONVERT_FUNCTIONS:
    case SQL_NUMERIC_FUNCTIONS:
    case SQL_STRING_FUNCTIONS:
    case SQL_SYSTEM_FUNCTIONS:
    case SQL_TIMEDATE_ADD_INTERVALS:
    case SQL_TIMEDATE_DIFF_INTERVALS:
    case SQL_TIMEDATE_FUNCTIONS:
    case SQL_CONVERT_BIGINT:
    case SQL_CONVERT_BINARY:
    case SQL_CONVERT_BIT:
    case SQL_CONVERT_CHAR:
    case SQL_CONVERT_DATE:
    case SQL_CONVERT_DECIMAL:
    case SQL_CONVERT_DOUBLE:
    case SQL_CONVERT_FLOAT:
    case SQL_CONVERT_INTEGER:
    case SQL_CONVERT_INTERVAL_DAY_TIME:
    case SQL_CONVERT_INTERVAL_YEAR_MONTH:
    case SQL_CONVERT_LONGVARBINARY:
    case SQL_CONVERT_LONGVARCHAR:
    case SQL_CONVERT_NUMERIC:
    case SQL_CONVERT_REAL:
    case SQL_CONVERT_SMALLINT:
    case SQL_CONVERT_TIME:
    case SQL_CONVERT_TIMESTAMP:
    case SQL_CONVERT_TINYINT:
    case SQL_CONVERT_VARBINARY:
    case SQL_CONVERT_VARCHAR:

    // Driver-level 16-bit integer properties.
    case SQL_MAX_CONCURRENT_ACTIVITIES:
    case SQL_CONCAT_NULL_BEHAVIOR:
    case SQL_CURSOR_COMMIT_BEHAVIOR:
    case SQL_CURSOR_ROLLBACK_BEHAVIOR:
    case SQL_NULL_COLLATION:
    case SQL_CATALOG_LOCATION:
    case SQL_CORRELATION_NAME:
    case SQL_CREATE_TRANSLATION:
    case SQL_DDL_INDEX:
    case SQL_DROP_ASSERTION:
    case SQL_DROP_CHARACTER_SET:
    case SQL_DROP_COLLATION:
    case SQL_DROP_DOMAIN:
    case SQL_DROP_SCHEMA:
    case SQL_DROP_TABLE:
    case SQL_DROP_TRANSLATION:
    case SQL_DROP_VIEW:
    case SQL_GROUP_BY:
    case SQL_IDENTIFIER_CASE:
    case SQL_NON_NULLABLE_COLUMNS:
    case SQL_QUOTED_IDENTIFIER_CASE:
    case SQL_MAX_CATALOG_NAME_LEN:
    case SQL_MAX_COLUMN_NAME_LEN:
    case SQL_MAX_COLUMNS_IN_GROUP_BY:
    case SQL_MAX_COLUMNS_IN_INDEX:
    case SQL_MAX_COLUMNS_IN_ORDER_BY:
    case SQL_MAX_COLUMNS_IN_SELECT:
    case SQL_MAX_COLUMNS_IN_TABLE:
    case SQL_MAX_CURSOR_NAME_LEN:
    case SQL_MAX_IDENTIFIER_LEN:
    case SQL_MAX_SCHEMA_NAME_LEN:
    case SQL_MAX_TABLE_NAME_LEN:
    case SQL_MAX_TABLES_IN_SELECT:
    case SQL_MAX_USER_NAME_LEN:
    return true;
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

#include "sql.h"
#include "sqlext.h"
FlightSqlConnection::FlightSqlConnection(OdbcVersion odbc_version)
    : odbc_version_(odbc_version),
      has_server_info_(false),
      closed_(false) {
    info_[SQL_DRIVER_NAME] = "Arrow Flight ODBC Driver";
    info_[SQL_DRIVER_VER] = "1.0.0";

    info_[SQL_GETDATA_EXTENSIONS] = static_cast<uint32_t>(SQL_GD_ANY_COLUMN | SQL_GD_ANY_ORDER);
    info_[SQL_CURSOR_SENSITIVITY] = static_cast<uint32_t>(SQL_UNSPECIFIED);
    info_[SQL_DEFAULT_TXN_ISOLATION] = static_cast<uint32_t>(0);

    info_[SQL_MAX_DRIVER_CONNECTIONS] = static_cast<uint16_t>(0);
  }

} // namespace flight_sql
} // namespace driver

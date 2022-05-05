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

#include "utils.h"
#include <odbcabstraction/platform.h> 
#include <arrow/type.h>
#include <odbcabstraction/types.h>

namespace driver {
namespace flight_sql {

using namespace odbcabstraction;
using arrow::util::make_optional;
using arrow::util::nullopt;

SqlDataType
GetDataTypeFromArrowField_V3(const std::shared_ptr<arrow::Field> &field) {
  const std::shared_ptr<arrow::DataType> &type = field->type();

  switch (type->id()) {
  case arrow::Type::BOOL:
    return odbcabstraction::SqlDataType_BIT;
  case arrow::Type::UINT8:
  case arrow::Type::INT8:
    return odbcabstraction::SqlDataType_TINYINT;
  case arrow::Type::UINT16:
  case arrow::Type::INT16:
    return odbcabstraction::SqlDataType_SMALLINT;
  case arrow::Type::UINT32:
  case arrow::Type::INT32:
    return odbcabstraction::SqlDataType_INTEGER;
  case arrow::Type::UINT64:
  case arrow::Type::INT64:
    return odbcabstraction::SqlDataType_BIGINT;
  case arrow::Type::HALF_FLOAT:
  case arrow::Type::FLOAT:
    return odbcabstraction::SqlDataType_FLOAT;
  case arrow::Type::DOUBLE:
    return odbcabstraction::SqlDataType_DOUBLE;
  case arrow::Type::BINARY:
  case arrow::Type::FIXED_SIZE_BINARY:
  case arrow::Type::LARGE_BINARY:
    return odbcabstraction::SqlDataType_BINARY;
  case arrow::Type::STRING:
  case arrow::Type::LARGE_STRING:
    return odbcabstraction::SqlDataType_VARCHAR;
  case arrow::Type::DATE32:
  case arrow::Type::DATE64:
    return odbcabstraction::SqlDataType_TYPE_DATE;
  case arrow::Type::TIMESTAMP:
    return odbcabstraction::SqlDataType_TYPE_TIMESTAMP;
  case arrow::Type::DECIMAL128:
  case arrow::Type::DECIMAL256:
    return odbcabstraction::SqlDataType_DECIMAL;
  case arrow::Type::TIME32:
  case arrow::Type::TIME64:
    return odbcabstraction::SqlDataType_TYPE_TIME;

    // TODO: Handle remaining types.
  case arrow::Type::INTERVAL_MONTHS:
  case arrow::Type::INTERVAL_DAY_TIME:
  case arrow::Type::INTERVAL_MONTH_DAY_NANO:
  case arrow::Type::LIST:
  case arrow::Type::STRUCT:
  case arrow::Type::SPARSE_UNION:
  case arrow::Type::DENSE_UNION:
  case arrow::Type::DICTIONARY:
  case arrow::Type::MAP:
  case arrow::Type::EXTENSION:
  case arrow::Type::FIXED_SIZE_LIST:
  case arrow::Type::DURATION:
  case arrow::Type::LARGE_LIST:
  case arrow::Type::MAX_ID:
  case arrow::Type::NA:
    break;
  }

  return odbcabstraction::SqlDataType_VARCHAR;
}

int16_t ConvertSqlDataTypeFromV3ToV2(int16_t data_type_v3) {
  switch (data_type_v3) {
    case SqlDataType_TYPE_DATE:
      return 9; // Same as SQL_DATE from sqlext.h
    case SqlDataType_TYPE_TIME:
      return 10; // Same as SQL_TIME from sqlext.h
    case SqlDataType_TYPE_TIMESTAMP:
      return 11; // Same as SQL_TIMESTAMP from sqlext.h
    default:
      return data_type_v3;
  }
}

CDataType ConvertCDataTypeFromV2ToV3(int16_t data_type_v2) {
  switch (data_type_v2) {
    case -6: // Same as SQL_C_TINYINT from sqlext.h
      return CDataType_STINYINT;
    case 4: // Same as SQL_C_LONG from sqlext.h
      return CDataType_SLONG;
    case 5: // Same as SQL_C_SHORT from sqlext.h
      return CDataType_SSHORT;
    case 7: // Same as SQL_C_FLOAT from sqlext.h
      return CDataType_FLOAT;
    case 8: // Same as SQL_C_DOUBLE from sqlext.h
      return CDataType_DOUBLE;
    case 9: // Same as SQL_C_DATE from sqlext.h
      return CDataType_DATE;
    case 10: // Same as SQL_C_TIME from sqlext.h
      return CDataType_TIME;
    case 11: // Same as SQL_C_TIMESTAMP from sqlext.h
      return CDataType_TIMESTAMP;
    default:
      return static_cast<CDataType>(data_type_v2);
  }
}

std::string GetTypeNameFromSqlDataType(int16_t data_type) {
  switch (data_type) {
  case SqlDataType_CHAR:
    return "CHAR";
  case SqlDataType_VARCHAR:
    return "VARCHAR";
  case SqlDataType_LONGVARCHAR:
    return "LONGVARCHAR";
  case SqlDataType_WCHAR:
    return "WCHAR";
  case SqlDataType_WVARCHAR:
    return "WVARCHAR";
  case SqlDataType_WLONGVARCHAR:
    return "WLONGVARCHAR";
  case SqlDataType_DECIMAL:
    return "DECIMAL";
  case SqlDataType_NUMERIC:
    return "NUMERIC";
  case SqlDataType_SMALLINT:
    return "SMALLINT";
  case SqlDataType_INTEGER:
    return "INTEGER";
  case SqlDataType_REAL:
    return "REAL";
  case SqlDataType_FLOAT:
    return "FLOAT";
  case SqlDataType_DOUBLE:
    return "DOUBLE";
  case SqlDataType_BIT:
    return "BIT";
  case SqlDataType_TINYINT:
    return "TINYINT";
  case SqlDataType_BIGINT:
    return "BIGINT";
  case SqlDataType_BINARY:
    return "BINARY";
  case SqlDataType_VARBINARY:
    return "VARBINARY";
  case SqlDataType_LONGVARBINARY:
    return "LONGVARBINARY";
  case SqlDataType_TYPE_DATE:
  case 9:
    return "DATE";
  case SqlDataType_TYPE_TIME:
  case 10:
    return "TIME";
  case SqlDataType_TYPE_TIMESTAMP:
  case 11:
    return "TIMESTAMP";
  case SqlDataType_INTERVAL_MONTH:
    return "INTERVAL_MONTH";
  case SqlDataType_INTERVAL_YEAR:
    return "INTERVAL_YEAR";
  case SqlDataType_INTERVAL_YEAR_TO_MONTH:
    return "INTERVAL_YEAR_TO_MONTH";
  case SqlDataType_INTERVAL_DAY:
    return "INTERVAL_DAY";
  case SqlDataType_INTERVAL_HOUR:
    return "INTERVAL_HOUR";
  case SqlDataType_INTERVAL_MINUTE:
    return "INTERVAL_MINUTE";
  case SqlDataType_INTERVAL_SECOND:
    return "INTERVAL_SECOND";
  case SqlDataType_INTERVAL_DAY_TO_HOUR:
    return "INTERVAL_DAY_TO_HOUR";
  case SqlDataType_INTERVAL_DAY_TO_MINUTE:
    return "INTERVAL_DAY_TO_MINUTE";
  case SqlDataType_INTERVAL_DAY_TO_SECOND:
    return "INTERVAL_DAY_TO_SECOND";
  case SqlDataType_INTERVAL_HOUR_TO_MINUTE:
    return "INTERVAL_HOUR_TO_MINUTE";
  case SqlDataType_INTERVAL_HOUR_TO_SECOND:
    return "INTERVAL_HOUR_TO_SECOND";
  case SqlDataType_INTERVAL_MINUTE_TO_SECOND:
    return "INTERVAL_MINUTE_TO_SECOND";
  case SqlDataType_GUID:
    return "GUID";
  }

  throw driver::odbcabstraction::DriverException("Unsupported data type: " +
                                                 std::to_string(data_type));
}

optional<int16_t>
GetRadixFromSqlDataType(odbcabstraction::SqlDataType data_type) {
  switch (data_type) {
  case SqlDataType_DECIMAL:
  case SqlDataType_NUMERIC:
  case SqlDataType_SMALLINT:
  case SqlDataType_TINYINT:
  case SqlDataType_INTEGER:
  case SqlDataType_BIGINT:
    return 10;
  case SqlDataType_REAL:
  case SqlDataType_FLOAT:
  case SqlDataType_DOUBLE:
    return 2;
  default:
    return arrow::util::nullopt;
  }
}

int16_t GetNonConciseDataType(odbcabstraction::SqlDataType data_type) {
  switch (data_type) {
  case SqlDataType_TYPE_DATE:
  case SqlDataType_TYPE_TIME:
  case SqlDataType_TYPE_TIMESTAMP:
    return 9; // Same as SQL_DATETIME on sql.h
  case SqlDataType_INTERVAL_YEAR:
  case SqlDataType_INTERVAL_MONTH:
  case SqlDataType_INTERVAL_DAY:
  case SqlDataType_INTERVAL_HOUR:
  case SqlDataType_INTERVAL_MINUTE:
  case SqlDataType_INTERVAL_SECOND:
  case SqlDataType_INTERVAL_YEAR_TO_MONTH:
  case SqlDataType_INTERVAL_DAY_TO_HOUR:
  case SqlDataType_INTERVAL_DAY_TO_MINUTE:
  case SqlDataType_INTERVAL_DAY_TO_SECOND:
  case SqlDataType_INTERVAL_HOUR_TO_MINUTE:
  case SqlDataType_INTERVAL_HOUR_TO_SECOND:
  case SqlDataType_INTERVAL_MINUTE_TO_SECOND:
    return 10; // Same as SQL_INTERVAL on sqlext.h
  default:
    return data_type;
  }
}

optional<int16_t> GetSqlDateTimeSubCode(SqlDataType data_type) {
  switch (data_type) {
  case SqlDataType_TYPE_DATE:
    return SqlDateTimeSubCode_DATE;
  case SqlDataType_TYPE_TIME:
    return SqlDateTimeSubCode_TIME;
  case SqlDataType_TYPE_TIMESTAMP:
    return SqlDateTimeSubCode_TIMESTAMP;
  case SqlDataType_INTERVAL_YEAR:
    return SqlDateTimeSubCode_YEAR;
  case SqlDataType_INTERVAL_MONTH:
    return SqlDateTimeSubCode_MONTH;
  case SqlDataType_INTERVAL_DAY:
    return SqlDateTimeSubCode_DAY;
  case SqlDataType_INTERVAL_HOUR:
    return SqlDateTimeSubCode_HOUR;
  case SqlDataType_INTERVAL_MINUTE:
    return SqlDateTimeSubCode_MINUTE;
  case SqlDataType_INTERVAL_SECOND:
    return SqlDateTimeSubCode_SECOND;
  case SqlDataType_INTERVAL_YEAR_TO_MONTH:
    return SqlDateTimeSubCode_YEAR_TO_MONTH;
  case SqlDataType_INTERVAL_DAY_TO_HOUR:
    return SqlDateTimeSubCode_DAY_TO_HOUR;
  case SqlDataType_INTERVAL_DAY_TO_MINUTE:
    return SqlDateTimeSubCode_DAY_TO_MINUTE;
  case SqlDataType_INTERVAL_DAY_TO_SECOND:
    return SqlDateTimeSubCode_DAY_TO_SECOND;
  case SqlDataType_INTERVAL_HOUR_TO_MINUTE:
    return SqlDateTimeSubCode_HOUR_TO_MINUTE;
  case SqlDataType_INTERVAL_HOUR_TO_SECOND:
    return SqlDateTimeSubCode_HOUR_TO_SECOND;
  case SqlDataType_INTERVAL_MINUTE_TO_SECOND:
    return SqlDateTimeSubCode_MINUTE_TO_SECOND;
  default:
    return arrow::util::nullopt;
  }
}

optional<int32_t> GetCharOctetLength(SqlDataType data_type,
                                     const optional<int32_t>& column_size) {
  switch (data_type) {
  case SqlDataType_CHAR:
  case SqlDataType_VARCHAR:
  case SqlDataType_LONGVARCHAR:
    return column_size.has_value() ? column_size.value() : NO_TOTAL;
  case SqlDataType_WCHAR:
  case SqlDataType_WVARCHAR:
  case SqlDataType_WLONGVARCHAR:
    return column_size.has_value() ? (column_size.value() * sizeof(SqlWChar))
                                   : NO_TOTAL;
  case SqlDataType_BINARY:
  case SqlDataType_VARBINARY:
  case SqlDataType_LONGVARBINARY:
    return column_size.has_value() ? column_size.value() : NO_TOTAL;
  default:
    return arrow::util::nullopt;
  }
}

optional<int32_t> GetBufferLength(SqlDataType data_type,
                                  const optional<int32_t>& column_size) {
  switch (data_type) {
  case SqlDataType_CHAR:
  case SqlDataType_VARCHAR:
  case SqlDataType_LONGVARCHAR:
    return column_size;
  case SqlDataType_WCHAR:
  case SqlDataType_WVARCHAR:
  case SqlDataType_WLONGVARCHAR:
    return column_size.has_value() ? arrow::util::make_optional(column_size.value() * sizeof(SqlWChar))
                                   : arrow::util::nullopt;
  case SqlDataType_BINARY:
  case SqlDataType_VARBINARY:
  case SqlDataType_LONGVARBINARY:
    return column_size;
  case SqlDataType_DECIMAL:
    return 19; // The same as sizeof(SQL_NUMERIC_STRUCT)
  case SqlDataType_NUMERIC:
    return 19; // The same as sizeof(SQL_NUMERIC_STRUCT)
  case SqlDataType_BIT:
  case SqlDataType_TINYINT:
    return 1;
  case SqlDataType_SMALLINT:
    return 2;
  case SqlDataType_INTEGER:
    return 4;
  case SqlDataType_BIGINT:
    return 8;
  case SqlDataType_REAL:
    return 4;
  case SqlDataType_FLOAT:
  case SqlDataType_DOUBLE:
    return 8;
  case SqlDataType_TYPE_DATE:
    return 6; // The same as sizeof(SQL_DATE_STRUCT)
  case SqlDataType_TYPE_TIME:
    return 6; // The same as sizeof(SQL_TIME_STRUCT)
  case SqlDataType_TYPE_TIMESTAMP:
    return 16; // The same as sizeof(SQL_TIME_STRUCT)
  case SqlDataType_INTERVAL_MONTH:
  case SqlDataType_INTERVAL_YEAR:
  case SqlDataType_INTERVAL_YEAR_TO_MONTH:
  case SqlDataType_INTERVAL_DAY:
  case SqlDataType_INTERVAL_HOUR:
  case SqlDataType_INTERVAL_MINUTE:
  case SqlDataType_INTERVAL_SECOND:
  case SqlDataType_INTERVAL_DAY_TO_HOUR:
  case SqlDataType_INTERVAL_DAY_TO_MINUTE:
  case SqlDataType_INTERVAL_DAY_TO_SECOND:
  case SqlDataType_INTERVAL_HOUR_TO_MINUTE:
  case SqlDataType_INTERVAL_HOUR_TO_SECOND:
  case SqlDataType_INTERVAL_MINUTE_TO_SECOND:
    return 28; // The same as sizeof(SQL_INTERVAL_STRUCT)
  case SqlDataType_GUID:
    return 16;
  default:
    return arrow::util::nullopt;
  }
}

optional<int32_t> GetDisplaySize(SqlDataType data_type,
                                 const optional<int32_t>& column_size) {
  switch (data_type) {
    case SqlDataType_CHAR:
    case SqlDataType_VARCHAR:
    case SqlDataType_LONGVARCHAR:
    case SqlDataType_WCHAR:
    case SqlDataType_WVARCHAR:
    case SqlDataType_WLONGVARCHAR:
      return column_size;
    case SqlDataType_BINARY:
    case SqlDataType_VARBINARY:
    case SqlDataType_LONGVARBINARY:
      return column_size ? make_optional(*column_size * 2) : nullopt;
    case SqlDataType_DECIMAL:
    case SqlDataType_NUMERIC:
      return column_size ? make_optional(*column_size + 2) : nullopt;
    case SqlDataType_BIT:
      return 1;
    case SqlDataType_TINYINT:
      return 4;
    case SqlDataType_SMALLINT:
      return 6;
    case SqlDataType_INTEGER:
      return 11;
    case SqlDataType_BIGINT:
      return 20;
    case SqlDataType_REAL:
      return 14;
    case SqlDataType_FLOAT:
    case SqlDataType_DOUBLE:
      return 24;
    case SqlDataType_TYPE_DATE:
      return 10;
    case SqlDataType_TYPE_TIME:
      return 12; // Assuming format "hh:mm:ss.fff"
    case SqlDataType_TYPE_TIMESTAMP:
      return 23; // Assuming format "yyyy-mm-dd hh:mm:ss.fff"
    case SqlDataType_INTERVAL_MONTH:
    case SqlDataType_INTERVAL_YEAR:
    case SqlDataType_INTERVAL_YEAR_TO_MONTH:
    case SqlDataType_INTERVAL_DAY:
    case SqlDataType_INTERVAL_HOUR:
    case SqlDataType_INTERVAL_MINUTE:
    case SqlDataType_INTERVAL_SECOND:
    case SqlDataType_INTERVAL_DAY_TO_HOUR:
    case SqlDataType_INTERVAL_DAY_TO_MINUTE:
    case SqlDataType_INTERVAL_DAY_TO_SECOND:
    case SqlDataType_INTERVAL_HOUR_TO_MINUTE:
    case SqlDataType_INTERVAL_HOUR_TO_SECOND:
    case SqlDataType_INTERVAL_MINUTE_TO_SECOND:
      return nullopt; // TODO: Implement for INTERVAL types
    case SqlDataType_GUID:
      return 36;
    default:
      return nullopt;
  }
}

std::string ConvertSqlPatternToRegexString(const std::string &pattern) {
  static const std::string specials = "[]()|^-+*?{}$\\.";

  std::string regex_str;
  bool escape = false;
  for (const auto &c : pattern) {
    if (escape) {
      regex_str += c;
      escape = false;
      continue;
    }

    switch (c) {
    case '\\':
      escape = true;
      break;
    case '_':
      regex_str += '.';
      break;
    case '%':
      regex_str += ".*";
      break;
    default:
      if (specials.find(c) != std::string::npos) {
        regex_str += '\\';
      }
      regex_str += c;
      break;
    }
  }
  return regex_str;
}

boost::xpressive::sregex ConvertSqlPatternToRegex(const std::string &pattern) {
  const std::string &regex_str = ConvertSqlPatternToRegexString(pattern);
  return boost::xpressive::sregex(boost::xpressive::sregex::compile(regex_str));
}

} // namespace flight_sql
} // namespace driver

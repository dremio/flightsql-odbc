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
#include "arrow/builder.h"
#include "arrow/type_fwd.h"
#include <odbcabstraction/platform.h>
#include <arrow/type.h>
#include <arrow/compute/api.h>
#include <odbcabstraction/types.h>
#include <json_converter.h>

namespace driver {
namespace flight_sql {

namespace {
bool IsComplexType(arrow::Type::type type_id) {
  switch (type_id) {
    case arrow::Type::LIST:
    case arrow::Type::LARGE_LIST:
    case arrow::Type::FIXED_SIZE_LIST:
    case arrow::Type::MAP:
    case arrow::Type::STRUCT:
      return true;
    default:
      return false;
  }
}
}

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

bool NeedArrayConversion(arrow::Type::type original_type_id, odbcabstraction::CDataType data_type) {
  switch (original_type_id) {
    case arrow::Type::DATE32:
    case arrow::Type::DATE64:
      return data_type != odbcabstraction::CDataType_DATE;
    case arrow::Type::TIME32:
    case arrow::Type::TIME64:
      return data_type != odbcabstraction::CDataType_TIME;
    case arrow::Type::TIMESTAMP:
      return data_type != odbcabstraction::CDataType_TIMESTAMP;
    case arrow::Type::STRING:
      return data_type != odbcabstraction::CDataType_CHAR &&
             data_type != odbcabstraction::CDataType_WCHAR;
    case arrow::Type::INT16:
      return data_type != odbcabstraction::CDataType_SSHORT;
    case arrow::Type::UINT16:
      return data_type != odbcabstraction::CDataType_USHORT;
    case arrow::Type::INT32:
      return data_type != odbcabstraction::CDataType_SLONG;
    case arrow::Type::UINT32:
      return data_type != odbcabstraction::CDataType_ULONG;
    case arrow::Type::FLOAT:
      return data_type != odbcabstraction::CDataType_FLOAT;
    case arrow::Type::DOUBLE:
      return data_type != odbcabstraction::CDataType_DOUBLE;
    case arrow::Type::BOOL:
      return data_type != odbcabstraction::CDataType_BIT;
    case arrow::Type::INT8:
      return data_type != odbcabstraction::CDataType_STINYINT;
    case arrow::Type::UINT8:
      return data_type != odbcabstraction::CDataType_UTINYINT;
    case arrow::Type::INT64:
      return data_type != odbcabstraction::CDataType_SBIGINT;
    case arrow::Type::UINT64:
      return data_type != odbcabstraction::CDataType_UBIGINT;
    case arrow::Type::BINARY:
      return data_type != odbcabstraction::CDataType_BINARY;
    case arrow::Type::DECIMAL128:
      return data_type != odbcabstraction::CDataType_NUMERIC;
    case arrow::Type::LIST:
    case arrow::Type::LARGE_LIST:
    case arrow::Type::FIXED_SIZE_LIST:
    case arrow::Type::MAP:
    case arrow::Type::STRUCT:
      return data_type == odbcabstraction::CDataType_CHAR || data_type == odbcabstraction::CDataType_WCHAR;
    default:
      throw odbcabstraction::DriverException(std::string("Invalid conversion"));
  }
}

std::shared_ptr<arrow::DataType>
GetDefaultDataTypeForTypeId(arrow::Type::type type_id) {
  switch (type_id) {
    case arrow::Type::STRING:
      return arrow::utf8();
    case arrow::Type::INT16:
      return arrow::int16();
    case arrow::Type::UINT16:
      return arrow::uint16();
    case arrow::Type::INT32:
      return arrow::int32();
    case arrow::Type::UINT32:
      return arrow::uint32();
    case arrow::Type::FLOAT:
      return arrow::float32();
    case arrow::Type::DOUBLE:
      return arrow::float64();
    case arrow::Type::BOOL:
      return arrow::boolean();
    case arrow::Type::INT8:
      return arrow::int8();
    case arrow::Type::UINT8:
      return arrow::uint8();
    case arrow::Type::INT64:
      return arrow::int64();
    case arrow::Type::UINT64:
      return arrow::uint64();
    case arrow::Type::BINARY:
      return arrow::binary();
    case arrow::Type::DATE64:
      return arrow::date64();
    case arrow::Type::TIME64:
      return arrow::time64(arrow::TimeUnit::MICRO);
    case arrow::Type::TIMESTAMP:
      return arrow::timestamp(arrow::TimeUnit::SECOND);
  }

  throw odbcabstraction::DriverException(std::string("Invalid type id: ") + std::to_string(type_id));
}

arrow::Type::type
ConvertCToArrowType(odbcabstraction::CDataType data_type) {
  switch (data_type) {
    case odbcabstraction::CDataType_CHAR:
    case odbcabstraction::CDataType_WCHAR:
      return arrow::Type::STRING;
    case odbcabstraction::CDataType_SSHORT:
      return arrow::Type::INT16;
    case odbcabstraction::CDataType_USHORT:
      return arrow::Type::UINT16;
    case odbcabstraction::CDataType_SLONG:
      return arrow::Type::INT32;
    case odbcabstraction::CDataType_ULONG:
      return arrow::Type::UINT32;
    case odbcabstraction::CDataType_FLOAT:
      return arrow::Type::FLOAT;
    case odbcabstraction::CDataType_DOUBLE:
      return arrow::Type::DOUBLE;
    case odbcabstraction::CDataType_BIT:
      return arrow::Type::BOOL;
    case odbcabstraction::CDataType_STINYINT:
      return arrow::Type::INT8;
    case odbcabstraction::CDataType_UTINYINT:
      return arrow::Type::UINT8;
    case odbcabstraction::CDataType_SBIGINT:
      return arrow::Type::INT64;
    case odbcabstraction::CDataType_UBIGINT:
      return arrow::Type::UINT64;
    case odbcabstraction::CDataType_BINARY:
      return arrow::Type::BINARY;
    case odbcabstraction::CDataType_NUMERIC:
      return arrow::Type::DECIMAL128;
    case odbcabstraction::CDataType_TIMESTAMP:
      return arrow::Type::TIMESTAMP;
    case odbcabstraction::CDataType_TIME:
      return arrow::Type::TIME64;
    case odbcabstraction::CDataType_DATE:
      return arrow::Type::DATE64;
    default:
      throw odbcabstraction::DriverException(std::string("Invalid target type: ") + std::to_string(data_type));
  }
}

odbcabstraction::CDataType ConvertArrowTypeToC(arrow::Type::type type_id) {
  switch (type_id) {
    case arrow::Type::STRING:
      return odbcabstraction::CDataType_CHAR;
    case arrow::Type::INT16:
      return odbcabstraction::CDataType_SSHORT;
    case arrow::Type::UINT16:
      return odbcabstraction::CDataType_USHORT;
    case arrow::Type::INT32:
      return odbcabstraction::CDataType_SLONG;
    case arrow::Type::UINT32:
      return odbcabstraction::CDataType_ULONG;
    case arrow::Type::FLOAT:
      return odbcabstraction::CDataType_FLOAT;
    case arrow::Type::DOUBLE:
      return odbcabstraction::CDataType_DOUBLE;
    case arrow::Type::BOOL:
      return odbcabstraction::CDataType_BIT;
    case arrow::Type::INT8:
      return odbcabstraction::CDataType_STINYINT;
    case arrow::Type::UINT8:
      return odbcabstraction::CDataType_UTINYINT;
    case arrow::Type::INT64:
      return odbcabstraction::CDataType_SBIGINT;
    case arrow::Type::UINT64:
      return odbcabstraction::CDataType_UBIGINT;
    case arrow::Type::BINARY:
      return odbcabstraction::CDataType_BINARY;
    case arrow::Type::DATE64:
    case arrow::Type::DATE32:
      return odbcabstraction::CDataType_DATE;
    case arrow::Type::TIME64:
    case arrow::Type::TIME32:
      return odbcabstraction::CDataType_TIME;
    case arrow::Type::TIMESTAMP:
      return odbcabstraction::CDataType_TIMESTAMP;
    default:
      throw odbcabstraction::DriverException(std::string("Invalid type id: ") + std::to_string(type_id));
  }
}

std::shared_ptr<arrow::Array>
CheckConversion(const arrow::Result<arrow::Datum> &result) {
  if (result.ok()) {
    const arrow::Datum &datum = result.ValueOrDie();
    return datum.make_array();
  } else {
    throw odbcabstraction::DriverException(result.status().message());
  }
}

ArrayConvertTask GetConverter(arrow::Type::type original_type_id,
                              odbcabstraction::CDataType target_type) {
  // The else statement has a convert the works for the most case of array
  // conversion. In case, we find conversion that the default one can't handle
  // we can include some additional if-else statement with the logic to handle
  // it
  if (original_type_id == arrow::Type::STRING &&
      target_type == odbcabstraction::CDataType_TIME) {
    return [=](const std::shared_ptr<arrow::Array> &original_array) {
      arrow::compute::StrptimeOptions options("%H:%M", arrow::TimeUnit::MICRO, false);

      auto converted_result =
        arrow::compute::Strptime({original_array}, options);
      auto first_converted_array = CheckConversion(converted_result);

      arrow::compute::CastOptions cast_options;
      cast_options.to_type = time64(arrow::TimeUnit::MICRO);
      return CheckConversion(arrow::compute::CallFunction(
        "cast", {first_converted_array}, &cast_options));
    };
  } else if (original_type_id == arrow::Type::STRING &&
             target_type == odbcabstraction::CDataType_DATE) {
    return [=](const std::shared_ptr<arrow::Array> &original_array) {
      // The Strptime requires a date format. Using the ISO 8601 format
      arrow::compute::StrptimeOptions options("%Y-%m-%d", arrow::TimeUnit::SECOND,
                                              false);

      auto converted_result =
        arrow::compute::Strptime({original_array}, options);

      auto first_converted_array = CheckConversion(converted_result);
      arrow::compute::CastOptions cast_options;
      cast_options.to_type = arrow::date64();
      return CheckConversion(arrow::compute::CallFunction(
        "cast", {first_converted_array}, &cast_options));
    };
  } else if (original_type_id == arrow::Type::DECIMAL128 &&
                 target_type == odbcabstraction::CDataType_CHAR ||
             target_type == odbcabstraction::CDataType_WCHAR) {
    return [=](const std::shared_ptr<arrow::Array> &original_array) {
      arrow::StringBuilder builder;
      int64_t length = original_array->length();
      ThrowIfNotOK(builder.ReserveData(length));

      for (int i = 0; i < length; ++i) {
        auto result = original_array->GetScalar(i);
        auto scalar = result.ValueOrDie();

        ThrowIfNotOK(builder.Append(scalar->ToString()));
      }

      auto finish = builder.Finish();

      return finish.ValueOrDie();
    };
  } else if (IsComplexType(original_type_id)  &&
             (target_type == odbcabstraction::CDataType_CHAR ||
              target_type == odbcabstraction::CDataType_WCHAR)) {
    return [=](const std::shared_ptr<arrow::Array> &original_array) {
      const auto &json_conversion_result = ConvertToJson(original_array);
      ThrowIfNotOK(json_conversion_result.status());
      return json_conversion_result.ValueOrDie();
    };
  } else {
    // Default converter
    return [=](const std::shared_ptr<arrow::Array> &original_array) {
      const arrow::Type::type &target_arrow_type_id =
          ConvertCToArrowType(target_type);
      arrow::compute::CastOptions cast_options;
      cast_options.to_type = GetDefaultDataTypeForTypeId(target_arrow_type_id);

      return CheckConversion(arrow::compute::CallFunction(
          "cast", {original_array}, &cast_options));
    };
  }
}
} // namespace flight_sql
} // namespace driver

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

#include "flight_sql_result_set_column.h"
#include <odbcabstraction/platform.h>
#include "flight_sql_result_set.h"
#include "json_converter.h"
#include <accessors/types.h>
#include <arrow/compute/api.h>
#include <memory>
#include <odbcabstraction/types.h>

namespace driver {
namespace flight_sql {

typedef std::function<
  std::shared_ptr<arrow::Array>(std::shared_ptr<arrow::Array>)>
  ArrayConvertTask;

namespace {
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
      return arrow::time64(TimeUnit::MICRO);
    case arrow::Type::TIMESTAMP:
      return arrow::timestamp(TimeUnit::SECOND);
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
  }

  throw odbcabstraction::DriverException(std::string("Invalid target type: ") + std::to_string(data_type));
}

bool NeedConversion(arrow::Type::type original_type_id, odbcabstraction::CDataType data_type) {
  if (original_type_id == arrow::Type::DATE64 || original_type_id == arrow::Type::DATE32) {
    return data_type != odbcabstraction::CDataType_DATE;
  } else if (original_type_id == arrow::Type::TIME64 || original_type_id == arrow::Type::TIME32) {
    return data_type != odbcabstraction::CDataType_TIME;
  }
  else if (original_type_id == arrow::Type::TIMESTAMP) {
    return data_type != odbcabstraction::CDataType_TIMESTAMP;
  }
  else if (original_type_id == arrow::Type::STRING) {
    return (data_type != odbcabstraction::CDataType_CHAR && data_type != odbcabstraction::CDataType_WCHAR);
  }
  else if (original_type_id == arrow::Type::INT16) {
    return data_type != odbcabstraction::CDataType_SSHORT;
  }
  else if (original_type_id == arrow::Type::UINT16) {
    return data_type != odbcabstraction::CDataType_USHORT;
  }
  else if (original_type_id == arrow::Type::INT32) {
    return data_type != odbcabstraction::CDataType_SLONG;
  }
  else if (original_type_id == arrow::Type::UINT32) {
    return data_type != odbcabstraction::CDataType_ULONG;
  }
  else if (original_type_id == arrow::Type::FLOAT) {
    return data_type != odbcabstraction::CDataType_FLOAT;
  }
  else if (original_type_id == arrow::Type::DOUBLE) {
    return data_type != odbcabstraction::CDataType_DOUBLE;
  }
  else if (original_type_id == arrow::Type::BOOL) {
    return data_type != odbcabstraction::CDataType_BIT;
  }
  else if (original_type_id == arrow::Type::INT8) {
    return data_type != odbcabstraction::CDataType_STINYINT;
  }
  else if (original_type_id == arrow::Type::UINT8) {
    return data_type != odbcabstraction::CDataType_UTINYINT;
  }
  else if (original_type_id == arrow::Type::INT64) {
    return data_type != odbcabstraction::CDataType_SBIGINT;
  }
  else if (original_type_id == arrow::Type::UINT64) {
    return data_type != odbcabstraction::CDataType_UBIGINT;
  }
  else if (original_type_id == arrow::Type::BINARY) {
    return data_type != odbcabstraction::CDataType_BINARY;
  }
  else if (original_type_id == arrow::Type::DECIMAL128) {
    return data_type != odbcabstraction::CDataType_NUMERIC;
  } else {
    throw odbcabstraction::DriverException(std::string("Invalid conversion"));
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
    return [=](const std::shared_ptr<Array> &original_array) {
      arrow::compute::StrptimeOptions options("%H:%M", TimeUnit::MICRO, false);

      auto converted_result =
          arrow::compute::Strptime({original_array}, options);
      auto first_converted_array = CheckConversion(converted_result);

      arrow::compute::CastOptions cast_options;
      cast_options.to_type = time64(TimeUnit::MICRO);
      return CheckConversion(arrow::compute::CallFunction(
          "cast", {first_converted_array}, &cast_options));
    };
  } else if (original_type_id == arrow::Type::STRING &&
             target_type == odbcabstraction::CDataType_DATE) {
    return [=](const std::shared_ptr<Array> &original_array) {
      // The Strptime requires a date format. Using the ISO 8601 format
      arrow::compute::StrptimeOptions options("%Y-%m-%d", TimeUnit::SECOND,
                                              false);

      auto converted_result =
          arrow::compute::Strptime({original_array}, options);

      auto first_converted_array = CheckConversion(converted_result);
      arrow::compute::CastOptions cast_options;
      cast_options.to_type = date64();
      return CheckConversion(arrow::compute::CallFunction(
          "cast", {first_converted_array}, &cast_options));
    };
  } else {
    // Default converter
    return [=](const std::shared_ptr<Array> &original_array) {
      const arrow::Type::type &target_arrow_type_id =
          ConvertCToArrowType(target_type);
      arrow::compute::CastOptions cast_options;
      cast_options.to_type = GetDefaultDataTypeForTypeId(target_arrow_type_id);

      return CheckConversion(arrow::compute::CallFunction(
          "cast", {original_array}, &cast_options));
    };
  }
}

std::shared_ptr<Array>
CastArray(const std::shared_ptr<arrow::Array> &original_array,
          CDataType target_type) {
  bool conversion = NeedConversion(original_array->type()->id(), target_type);

  if (conversion) {
    auto converter = GetConverter(original_array->type_id(), target_type);
    return converter(original_array);
  } else {
    return original_array;
  }
}
} // namespace

std::unique_ptr<Accessor>
FlightSqlResultSetColumn::CreateAccessor(CDataType target_type) {
  const std::shared_ptr<arrow::Array> &original_array =
      result_set_->GetArrayForColumn(column_n_);
  cached_original_array_ = original_array.get();
  cached_casted_array_ = CastArray(original_array, target_type);

  return flight_sql::CreateAccessor(cached_casted_array_.get(), target_type);
}

Accessor *
FlightSqlResultSetColumn::GetAccessorForTargetType(CDataType target_type) {
  const std::shared_ptr<arrow::Array> &original_array =
      result_set_->GetArrayForColumn(column_n_);

  // TODO: Figure out if that's the best way of caching
  if (cached_accessor_ && original_array.get() == cached_original_array_ &&
      cached_accessor_->target_type_ == target_type) {
    return cached_accessor_.get();
  }

  cached_accessor_ = CreateAccessor(target_type);
  return cached_accessor_.get();
}

FlightSqlResultSetColumn::FlightSqlResultSetColumn()
    : FlightSqlResultSetColumn(nullptr, 0) {}

FlightSqlResultSetColumn::FlightSqlResultSetColumn(
    FlightSqlResultSet *result_set, int column_n)
    : result_set_(result_set), column_n_(column_n),
      cached_original_array_(nullptr), is_bound(false) {}

Accessor *FlightSqlResultSetColumn::GetAccessorForBinding() {
  return GetAccessorForTargetType(binding.target_type);
}

Accessor *
FlightSqlResultSetColumn::GetAccessorForGetData(CDataType target_type) {
  return GetAccessorForTargetType(target_type);
}

void FlightSqlResultSetColumn::SetBinding(ColumnBinding new_binding) {
  binding = new_binding;
  is_bound = true;

  ResetAccessor();
}

void FlightSqlResultSetColumn::ResetBinding() {
  is_bound = false;
  ResetAccessor();
}

void FlightSqlResultSetColumn::ResetAccessor() {
  if (is_bound) {
    cached_accessor_.reset();
  }
}

} // namespace flight_sql
} // namespace driver

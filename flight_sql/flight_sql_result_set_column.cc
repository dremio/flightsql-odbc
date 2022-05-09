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
  }

  throw odbcabstraction::DriverException(std::string("Invalid target type: ") + std::to_string(data_type));
}

std::shared_ptr<Array>
CastArray(const std::shared_ptr<arrow::Array> &original_array,
          CDataType target_type) {
  const arrow::Type::type &target_arrow_type_id = ConvertCToArrowType(target_type);
  if (original_array->type()->id() == target_arrow_type_id) {
    // Avoid casting if target type is the same as original
    return original_array;
  }

  arrow::compute::CastOptions cast_options;
  cast_options.to_type = GetDefaultDataTypeForTypeId(target_arrow_type_id);

  const arrow::Result<arrow::Datum> &result =
      arrow::compute::CallFunction("cast", {original_array}, &cast_options);
  if (result.ok()) {
    arrow::Datum datum = result.ValueOrDie();
    return std::move(datum).make_array();
  } else if (target_type == odbcabstraction::CDataType_CHAR || target_type == odbcabstraction::CDataType_WCHAR) {
    // Fallback to JSON conversion if target type is CHAR/WCHAR
    const auto &json_conversion_result = ConvertToJson(original_array);
    ThrowIfNotOK(json_conversion_result.status());

    return json_conversion_result.ValueOrDie();
  } else {
    throw odbcabstraction::DriverException(result.status().message());
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

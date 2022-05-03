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
ConvertCToArrowDataType(odbcabstraction::CDataType data_type) {
  switch (data_type) {
  case odbcabstraction::CDataType_CHAR:
  case odbcabstraction::CDataType_WCHAR:
    return arrow::utf8();
  case odbcabstraction::CDataType_SSHORT:
    return arrow::int16();
  case odbcabstraction::CDataType_USHORT:
    return arrow::uint16();
  case odbcabstraction::CDataType_SLONG:
    return arrow::int32();
  case odbcabstraction::CDataType_ULONG:
    return arrow::uint32();
  case odbcabstraction::CDataType_FLOAT:
    return arrow::float32();
  case odbcabstraction::CDataType_DOUBLE:
    return arrow::float64();
  case odbcabstraction::CDataType_BIT:
    return arrow::boolean();
  case odbcabstraction::CDataType_STINYINT:
    return arrow::int8();
  case odbcabstraction::CDataType_UTINYINT:
    return arrow::uint8();
  case odbcabstraction::CDataType_SBIGINT:
    return arrow::int64();
  case odbcabstraction::CDataType_UBIGINT:
    return arrow::uint64();
  case odbcabstraction::CDataType_BINARY:
    return arrow::binary();
  }

  throw odbcabstraction::DriverException(std::string("Invalid target type: ") + std::to_string(data_type));
}

std::shared_ptr<Array>
CastArray(const std::shared_ptr<arrow::Array> &original_array,
          CDataType target_type) {
  Type::type type = original_array->type_id();
  auto is_complex = type == Type::LIST || type == Type::FIXED_SIZE_LIST || type == Type::LARGE_LIST
                    || type == Type::MAP || type == Type::STRUCT;
  if (is_complex &&
      (target_type == odbcabstraction::CDataType_CHAR || target_type == odbcabstraction::CDataType_WCHAR)) {
    const auto &result = ConvertToJson(original_array);
    ThrowIfNotOK(result.status());

    return result.ValueOrDie();
  }

  const std::shared_ptr<arrow::DataType> &target_arrow_type =
      ConvertCToArrowDataType(target_type);
  if (original_array->type()->Equals(target_arrow_type)) {
    // Avoid casting if target type is the same as original
    return original_array;
  }

  arrow::compute::CastOptions cast_options;
  cast_options.to_type = target_arrow_type;

  const arrow::Result<arrow::Datum> &result =
      arrow::compute::CallFunction("cast", {original_array}, &cast_options);
  ThrowIfNotOK(result.status());
  arrow::Datum datum = result.ValueOrDie();
  return std::move(datum).make_array();
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

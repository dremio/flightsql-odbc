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
#include "flight_sql_result_set.h"
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
}
} // namespace

std::unique_ptr<Accessor>
FlightSqlResultSetColumn::CreateAccessorX(CDataType target_type) {
  const std::shared_ptr<arrow::Array> &original_array =
      result_set_->GetArrayForColumn(column_n_);

  arrow::compute::CastOptions cast_options;
  cast_options.to_type = ConvertCToArrowDataType(target_type);

  const arrow::Result<arrow::Datum> &result =
      arrow::compute::CallFunction("cast", {original_array}, &cast_options);
  ThrowIfNotOK(result.status());
  arrow::Datum datum = result.ValueOrDie();
  casted_array_cache_ = std::move(datum).make_array();

  return CreateAccessor(casted_array_cache_.get(), target_type);
}

std::unique_ptr<Accessor>
FlightSqlResultSetColumn::GetAccessorForTargetType(CDataType target_type) {
  const std::shared_ptr<arrow::Array> &original_array =
      result_set_->GetArrayForColumn(column_n_);

  return CreateAccessorX(target_type);
}

FlightSqlResultSetColumn::FlightSqlResultSetColumn(
    FlightSqlResultSet *result_set, int column_n)
    : result_set_(result_set), column_n_(column_n), get_data_offset(0) {}

Accessor *FlightSqlResultSetColumn::GetAccessorForBinding() {
  if (cached_accessor_ == nullptr) {
    cached_accessor_ = GetAccessorForTargetType(binding->target_type);
  }

  return cached_accessor_.get();
}

Accessor *
FlightSqlResultSetColumn::GetAccessorForGetData(CDataType target_type) {
  if (cached_accessor_ == nullptr ||
      cached_accessor_->GetTargetType() != target_type) {
    cached_accessor_ = GetAccessorForTargetType(target_type);
  }

  return cached_accessor_.get();
}

void FlightSqlResultSetColumn::SetBinding(ColumnBinding *new_binding) {
  binding.reset(new_binding);
  ResetAccessor();
}

void FlightSqlResultSetColumn::ResetAccessor() { cached_accessor_.reset(); }

} // namespace flight_sql
} // namespace driver
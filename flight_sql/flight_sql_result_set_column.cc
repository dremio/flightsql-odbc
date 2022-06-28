/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include "flight_sql_result_set_column.h"
#include <odbcabstraction/platform.h>
#include "flight_sql_result_set.h"
#include "utils.h"
#include <accessors/types.h>
#include <memory>
#include <odbcabstraction/types.h>

namespace driver {
namespace flight_sql {

namespace {
std::shared_ptr<Array>
CastArray(const std::shared_ptr<arrow::Array> &original_array,
          CDataType target_type) {
  bool conversion = NeedArrayConversion(original_array->type()->id(), target_type);

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

  if (target_type == odbcabstraction::CDataType_DEFAULT) {
    target_type = ConvertArrowTypeToC(original_array->type_id(), result_set_->UseWideChar());
  }

  // TODO: Figure out if that's the best way of caching
  if (cached_accessor_ && original_array.get() == cached_original_array_ &&
      cached_accessor_->target_type_ == target_type) {
    return cached_accessor_.get();
  }

  cached_accessor_ = CreateAccessor(target_type);
  cached_original_array_ = original_array.get();
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
  // An improvement would be to have ResetAccessor take in a newly acquired array.
  // Then the accessor itself should switch to the new array and reset internal
  // state, rather than deleting the accessor.
  cached_accessor_.reset();
}

} // namespace flight_sql
} // namespace driver

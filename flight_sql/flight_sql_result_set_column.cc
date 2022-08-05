/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include "flight_sql_result_set_column.h"
#include <odbcabstraction/platform.h>
#include "flight_sql_result_set_accessors.h"
#include "utils.h"
#include "odbcabstraction/logger.h"
#include <accessors/types.h>
#include <memory>
#include <odbcabstraction/types.h>

namespace driver {
namespace flight_sql {

namespace {
std::shared_ptr<Array>
CastArray(const std::shared_ptr<arrow::Array> &original_array,
          CDataType target_type) {
  LOG_TRACE("[{}] Entry with parameters: target_type '{}'", __FUNCTION__, target_type)
  bool conversion = NeedArrayConversion(original_array->type()->id(), target_type);

  if (conversion) {
    auto converter = GetConverter(original_array->type_id(), target_type);
    LOG_TRACE("[{}] Exiting successfully with converted array", __FUNCTION__)
    return converter(original_array);
  } else {
    LOG_TRACE("[{}] Exiting successfully with original array", __FUNCTION__)
    return original_array;
  }
}
} // namespace

std::unique_ptr<Accessor>
FlightSqlResultSetColumn::CreateAccessor(CDataType target_type) {
  LOG_TRACE("[{}] Entry with parameters: target_type '{}'", __FUNCTION__, target_type)

  cached_casted_array_ = CastArray(original_array_, target_type);

  auto return_ptr = flight_sql::CreateAccessor(cached_casted_array_.get(), target_type);

  LOG_TRACE("[{}] Exiting successfully with Accessor", __FUNCTION__)
  return return_ptr;
}

Accessor *
FlightSqlResultSetColumn::GetAccessorForTargetType(CDataType target_type) {
  LOG_TRACE("[{}] Entry with parameters: target_type '{}'", __FUNCTION__, target_type)

  // Cast the original array to a type matching the target_type.
  if (target_type == odbcabstraction::CDataType_DEFAULT) {
    target_type = ConvertArrowTypeToC(original_array_->type_id(), use_wide_char_);
  }

  cached_accessor_ = CreateAccessor(target_type);

  LOG_TRACE("[{}] Exiting successfully with Accessor", __FUNCTION__)
  return cached_accessor_.get();
}

FlightSqlResultSetColumn::FlightSqlResultSetColumn(bool use_wide_char)
    : use_wide_char_(use_wide_char),
      is_bound_(false) {}

void FlightSqlResultSetColumn::SetBinding(const ColumnBinding& new_binding, arrow::Type::type arrow_type) {
  LOG_TRACE("[{}] Entry with parameters: target_type '{}'", __FUNCTION__, arrow_type)

  binding_ = new_binding;
  is_bound_ = true;

  if (binding_.target_type == odbcabstraction::CDataType_DEFAULT) {
    binding_.target_type = ConvertArrowTypeToC(arrow_type, use_wide_char_);
  }

  // Overwrite the binding if the caller is using SQL_C_NUMERIC and has used zero
  // precision if it is zero (this is precision unset and will always fail).
  if (binding_.precision == 0 &&
      binding_.target_type == odbcabstraction::CDataType_NUMERIC) {
    binding_.precision = arrow::Decimal128Type::kMaxPrecision;
  }

  // Rebuild the accessor and casted array if the target type changed.
  if (original_array_ && (!cached_casted_array_ || cached_accessor_->target_type_ != binding_.target_type)) {
    cached_accessor_ = CreateAccessor(binding_.target_type);
  }
}

void FlightSqlResultSetColumn::ResetBinding() {
  is_bound_ = false;
  cached_casted_array_.reset();
  cached_accessor_.reset();
}

} // namespace flight_sql
} // namespace driver

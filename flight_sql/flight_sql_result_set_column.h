/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#pragma once

#include <accessors/types.h>
#include <arrow/array.h>
#include "utils.h"

namespace driver {
namespace flight_sql {

using arrow::Array;

class FlightSqlResultSetColumn {
private:
  std::shared_ptr<Array> original_array_;
  std::shared_ptr<Array> cached_casted_array_;
  std::unique_ptr<Accessor> cached_accessor_;

  std::unique_ptr<Accessor> CreateAccessor(CDataType target_type);

  Accessor *GetAccessorForTargetType(CDataType target_type);

public:
  FlightSqlResultSetColumn() = default;
  explicit FlightSqlResultSetColumn(bool use_wide_char);

  ColumnBinding binding_;
  bool use_wide_char_;
  bool is_bound_;

  inline Accessor *GetAccessorForBinding() {
    return cached_accessor_.get();
  }

  inline Accessor *GetAccessorForGetData(CDataType target_type) {
    if (target_type == odbcabstraction::CDataType_DEFAULT) {
      target_type = ConvertArrowTypeToC(original_array_->type_id(), use_wide_char_);
    }

    if (cached_accessor_ && cached_accessor_->target_type_ == target_type) {
      return cached_accessor_.get();
    }
    return GetAccessorForTargetType(target_type);
  }

  void SetBinding(const ColumnBinding& new_binding);

  void ResetBinding();

  inline void ResetAccessor() {
    if (is_bound_) {
      cached_accessor_ = CreateAccessor(cached_accessor_->target_type_);
    } else if (cached_accessor_) {
      cached_accessor_ = CreateAccessor(cached_accessor_->target_type_);
    } else {
      cached_casted_array_.reset();
      cached_accessor_.reset();
    }
  }

  inline void SetArrowArray(std::shared_ptr<Array> array) {
    original_array_ = std::move(array);
  }
};
} // namespace flight_sql
} // namespace driver

/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#pragma once

#include <odbcabstraction/platform.h>
#include <arrow/array.h>
#include <cstddef>
#include <cstdint>
#include <odbcabstraction/exceptions.h>
#include <odbcabstraction/types.h>
#include <odbcabstraction/diagnostics.h>
#include <sstream>
#include "odbcabstraction/types.h"

namespace driver {
namespace flight_sql {

using arrow::Array;
using odbcabstraction::CDataType;

class FlightSqlResultSet;

struct ColumnBinding {
  void *buffer;
  ssize_t *strlen_buffer;
  size_t buffer_length;
  CDataType target_type;
  int precision;
  int scale;

  ColumnBinding() = default;

  ColumnBinding(CDataType target_type, int precision, int scale, void *buffer,
                size_t buffer_length, ssize_t *strlen_buffer)
      : target_type(target_type), precision(precision), scale(scale),
        buffer(buffer), buffer_length(buffer_length),
        strlen_buffer(strlen_buffer) {}
};

/// \brief Accessor interface meant to provide a way of populating data of a
/// single column to buffers bound by `ColumnarResultSet::BindColumn`.
class Accessor {
public:
  const CDataType target_type_;

  Accessor(CDataType target_type) : target_type_(target_type) {}

  virtual ~Accessor() = default;

  /// \brief Populates next cells
  virtual size_t GetColumnarData(ColumnBinding *binding, int64_t starting_row,
                                 size_t cells, int64_t &value_offset, bool update_value_offset,
                                 odbcabstraction::Diagnostics &diagnostics, uint16_t* row_status_array) = 0;

  virtual size_t GetCellLength(ColumnBinding *binding) const = 0;
};

template <typename ARROW_ARRAY, CDataType TARGET_TYPE, typename DERIVED>
class FlightSqlAccessor : public Accessor {
public:
  explicit FlightSqlAccessor(Array *array)
      : Accessor(TARGET_TYPE),
        array_(arrow::internal::checked_cast<ARROW_ARRAY *>(array)) {}

  size_t GetColumnarData(ColumnBinding *binding, int64_t starting_row,
                         size_t cells, int64_t &value_offset, bool update_value_offset,
                         odbcabstraction::Diagnostics &diagnostics, uint16_t* row_status_array) override {
    const std::shared_ptr<Array> &array =
        array_->Slice(starting_row, static_cast<int64_t>(cells));

    return GetColumnarData(
        arrow::internal::checked_pointer_cast<ARROW_ARRAY>(array), binding,
        value_offset, update_value_offset, diagnostics, row_status_array);
  }

  size_t GetCellLength(ColumnBinding *binding) const override {
    return static_cast<const DERIVED *>(this)->GetCellLength_impl(binding);
  }

private:
  ARROW_ARRAY *array_;

  size_t GetColumnarData(const std::shared_ptr<ARROW_ARRAY> &sliced_array,
                         ColumnBinding *binding, int64_t &value_offset, bool update_value_offset,
                         odbcabstraction::Diagnostics &diagnostics, uint16_t* row_status_array) {
    return static_cast<DERIVED *>(this)->GetColumnarData_impl(
        sliced_array, binding, value_offset, update_value_offset, diagnostics, row_status_array);
  }

  size_t GetColumnarData_impl(const std::shared_ptr<ARROW_ARRAY> &sliced_array,
                              ColumnBinding *binding, int64_t &value_offset, bool update_value_offset,
                              odbcabstraction::Diagnostics &diagnostics, uint16_t* row_status_array) {
    int64_t length = sliced_array->length();
    for (int64_t i = 0; i < length; ++i) {
      if (sliced_array->IsNull(i)) {
        if (binding->strlen_buffer) {
          binding->strlen_buffer[i] = odbcabstraction::NULL_DATA;
        } else {
          throw odbcabstraction::NullWithoutIndicatorException();
        }
      } else {
        // TODO: Optimize this by creating different versions of MoveSingleCell
        // depending on if strlen_buffer is null.
        auto row_status = MoveSingleCell(binding, sliced_array.get(), i, value_offset, update_value_offset,
                                         diagnostics);
        if (row_status_array) {
          row_status_array[i] = row_status;
        }
      }
    }

    return length;
  }

  odbcabstraction::RowStatus MoveSingleCell(ColumnBinding *binding, ARROW_ARRAY *array, int64_t i,
                                            int64_t &value_offset, bool update_value_offset,
                                            odbcabstraction::Diagnostics &diagnostics) {
    return static_cast<DERIVED *>(this)->MoveSingleCell_impl(binding, array, i,
                                                             value_offset, update_value_offset, diagnostics);
  }

  odbcabstraction::RowStatus MoveSingleCell_impl(ColumnBinding *binding, ARROW_ARRAY *array,
                           int64_t i, int64_t &value_offset, bool update_value_offset, odbcabstraction::Diagnostics &diagnostics) {
    std::stringstream ss;
    ss << "Unknown type conversion from StringArray to target C type "
       << TARGET_TYPE;
    throw odbcabstraction::DriverException(ss.str());
  }
};

} // namespace flight_sql
} // namespace driver

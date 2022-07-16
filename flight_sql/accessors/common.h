/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#pragma once

#include "types.h"
#include <arrow/array.h>
#include <arrow/scalar.h>
#include <odbcabstraction/types.h>
#include <odbcabstraction/diagnostics.h>
#include <algorithm>
#include <cstdint>

namespace driver {
namespace flight_sql {

using namespace arrow;
using namespace odbcabstraction;

template <typename ARRAY_TYPE>
inline size_t CopyFromArrayValuesToBinding(ARRAY_TYPE* array,
                                           ColumnBinding *binding,
                                           int64_t starting_row, int64_t cells) {
  constexpr ssize_t element_size = sizeof(typename ARRAY_TYPE::value_type);

  if (binding->strlen_buffer) {
    for (int64_t i = 0; i < cells; ++i) {
      int64_t current_row = starting_row + i;
      if (array->IsNull(current_row)) {
        binding->strlen_buffer[i] = NULL_DATA;
      } else {
        binding->strlen_buffer[i] = element_size;
      }
    }
  } else {
    // Duplicate this loop to avoid null checks within the loop.
    for (int64_t i = starting_row; i < starting_row + cells; ++i) {
      if (array->IsNull(i)) {
        throw odbcabstraction::NullWithoutIndicatorException();
      }
    }
  }

  // Copy the entire array to the bound ODBC buffers.
  // Note that the array should already have been sliced down to the same number
  // of elements in the ODBC data array by the point in which this function is called.
  const auto *values = array->raw_values();
  memcpy(binding->buffer, &values[starting_row], element_size * cells);

  return cells;
}

} // namespace flight_sql
} // namespace driver

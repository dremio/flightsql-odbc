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
inline size_t CopyFromArrayValuesToBinding(const std::shared_ptr<Array> &array,
                                           ColumnBinding *binding) {
  auto *typed_array = reinterpret_cast<ARRAY_TYPE *>(array.get());
  constexpr ssize_t element_size = sizeof(typename ARRAY_TYPE::value_type);

  int64_t length = array->length();
  if (binding->strlen_buffer) {
    for (int64_t i = 0; i < length; ++i) {
      if (array->IsNull(i)) {
        binding->strlen_buffer[i] = NULL_DATA;
      } else {
        binding->strlen_buffer[i] = element_size;
      }
    }
  } else {
    // Duplicate this loop to avoid null checks within the loop.
    for (int64_t i = 0; i < length; ++i) {
      if (array->IsNull(i)) {
        throw odbcabstraction::NullWithoutIndicatorException();
      }
    }
  }

  const auto *values = typed_array->raw_values();
  size_t value_length =
      std::min(static_cast<size_t>(length), binding->buffer_length);
  memcpy(binding->buffer, values, element_size * value_length);

  return length;
}

} // namespace flight_sql
} // namespace driver

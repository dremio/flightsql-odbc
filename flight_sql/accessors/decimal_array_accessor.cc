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

#include "decimal_array_accessor.h"

#include <arrow/array.h>

namespace driver {
namespace flight_sql {

using namespace arrow;
using namespace odbcabstraction;

namespace {
void Negate(uint8_t* values) {
  uint8_t carry = 1;
  for (size_t i = 0; i < 16; ++i) {
    uint8_t& elem = values[i];
    elem = ~elem + carry;
    carry &= (elem == 0);
  }
}
} // namespace

template <typename ARROW_ARRAY, CDataType TARGET_TYPE>
DecimalArrayFlightSqlAccessor<ARROW_ARRAY, TARGET_TYPE>::DecimalArrayFlightSqlAccessor(
    Array *array)
    : FlightSqlAccessor<ARROW_ARRAY, TARGET_TYPE,
                        DecimalArrayFlightSqlAccessor<ARROW_ARRAY, TARGET_TYPE>>(array) {}

template <>
void DecimalArrayFlightSqlAccessor<Decimal128Array, CDataType_NUMERIC>::MoveSingleCell_impl(
    ColumnBinding *binding, Decimal128Array *array, int64_t i,
    int64_t value_offset, odbcabstraction::Diagnostics &diagnostics) {
  auto result = &(static_cast<NUMERIC_STRUCT *>(binding->buffer)[i]);
  const std::shared_ptr<Decimal128Type> &data_type = std::static_pointer_cast<Decimal128Type>(array->type());
  memcpy(&result->val, array->Value(i), 16);
  result->precision = static_cast<uint8_t>(data_type->precision());
  result->scale = static_cast<int8_t>(data_type->scale());

  // If the most significant bit is set this number is negative (sign = 0).
  result->sign = (static_cast<int8_t>(result->val[15]) >> 7) == 0;
  result->precision = data_type->precision();
  if (result->sign == 0) {
    Negate(result->val);
  }

  if (binding->strlen_buffer) {
    binding->strlen_buffer[i] = static_cast<ssize_t>(sizeof(NUMERIC_STRUCT));
  }
}

template class DecimalArrayFlightSqlAccessor<Decimal128Array, odbcabstraction::CDataType_NUMERIC>;

} // namespace flight_sql
} // namespace driver

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

#include "int8_array_accessor.h"

namespace driver {
namespace flight_sql {

using namespace arrow;
using namespace odbcabstraction;

template <CDataType TARGET_TYPE>
Int8ArrayFlightSqlAccessor<TARGET_TYPE>::Int8ArrayFlightSqlAccessor(
    Array *array)
    : FlightSqlAccessor<Int8Array, TARGET_TYPE,
                        Int8ArrayFlightSqlAccessor<TARGET_TYPE>>(array) {}

template <>
size_t Int8ArrayFlightSqlAccessor<CDataType_STINYINT>::GetColumnarData_impl(
    const std::shared_ptr<Int8Array> &sliced_array, ColumnBinding *binding,
    int64_t value_offset) {
  return CopyFromArrayValuesToBinding<Int8Array>(sliced_array, binding);
}

template <>
void Int8ArrayFlightSqlAccessor<CDataType_CHAR>::MoveSingleCell_impl(
    ColumnBinding *binding, Int8Array *array, int64_t i, int64_t value_offset) {
  MoveToCharBuffer(binding, array, i, value_offset);
}

template class Int8ArrayFlightSqlAccessor<odbcabstraction::CDataType_STINYINT>;

} // namespace flight_sql
} // namespace driver

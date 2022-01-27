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

#pragma once

#include "common.h"
#include "types.h"
#include <arrow/array.h>
#include <arrow/scalar.h>
#include <odbcabstraction/types.h>

namespace driver {
namespace flight_sql {

using namespace arrow;
using namespace odbcabstraction;

/*
 * Int64Array
 */

template <>
inline size_t FlightSqlAccessor<Int64Array, CDataType_SBIGINT>::GetColumnarData(
    FlightSqlResultSet *result_set, ColumnBinding *binding,
    int64_t starting_row, size_t cells, int64_t value_offset) {
  return CopyFromArrayValuesToBinding<Int64Array>(result_set, binding,
                                                  starting_row, cells);
}

template <>
inline void FlightSqlAccessor<Int64Array, CDataType_CHAR>::MoveSingleCell(
    ColumnBinding *binding, Int64Array *array, int64_t i,
    int64_t value_offset) {
  MoveToCharBuffer(binding, array, i, value_offset);
}

/*
 * UInt64Array
 */

template <>
inline size_t
FlightSqlAccessor<UInt64Array, CDataType_UBIGINT>::GetColumnarData(
    FlightSqlResultSet *result_set, ColumnBinding *binding,
    int64_t starting_row, size_t cells, int64_t value_offset) {
  return CopyFromArrayValuesToBinding<UInt64Array>(result_set, binding,
                                                   starting_row, cells);
}

template <>
inline void FlightSqlAccessor<UInt64Array, CDataType_CHAR>::MoveSingleCell(
    ColumnBinding *binding, UInt64Array *array, int64_t i,
    int64_t value_offset) {
  MoveToCharBuffer(binding, array, i, value_offset);
}

/*
 * Int32Array
 */

template <>
inline size_t FlightSqlAccessor<Int32Array, CDataType_SLONG>::GetColumnarData(
    FlightSqlResultSet *result_set, ColumnBinding *binding,
    int64_t starting_row, size_t cells, int64_t value_offset) {
  return CopyFromArrayValuesToBinding<Int32Array>(result_set, binding,
                                                  starting_row, cells);
}

template <>
inline void FlightSqlAccessor<Int32Array, CDataType_CHAR>::MoveSingleCell(
    ColumnBinding *binding, Int32Array *array, int64_t i,
    int64_t value_offset) {
  MoveToCharBuffer(binding, array, i, value_offset);
}

/*
 * UInt32Array
 */

template <>
inline size_t FlightSqlAccessor<UInt32Array, CDataType_ULONG>::GetColumnarData(
    FlightSqlResultSet *result_set, ColumnBinding *binding,
    int64_t starting_row, size_t cells, int64_t value_offset) {
  return CopyFromArrayValuesToBinding<UInt32Array>(result_set, binding,
                                                   starting_row, cells);
}

template <>
inline void FlightSqlAccessor<UInt32Array, CDataType_CHAR>::MoveSingleCell(
    ColumnBinding *binding, UInt32Array *array, int64_t i,
    int64_t value_offset) {
  MoveToCharBuffer(binding, array, i, value_offset);
}

/*
 * Int16Array
 */

template <>
inline size_t FlightSqlAccessor<Int16Array, CDataType_SSHORT>::GetColumnarData(
    FlightSqlResultSet *result_set, ColumnBinding *binding,
    int64_t starting_row, size_t cells, int64_t value_offset) {
  return CopyFromArrayValuesToBinding<Int16Array>(result_set, binding,
                                                  starting_row, cells);
}

template <>
inline void FlightSqlAccessor<Int16Array, CDataType_CHAR>::MoveSingleCell(
    ColumnBinding *binding, Int16Array *array, int64_t i,
    int64_t value_offset) {
  MoveToCharBuffer(binding, array, i, value_offset);
}

/*
 * UInt16Array
 */

template <>
inline size_t FlightSqlAccessor<UInt16Array, CDataType_USHORT>::GetColumnarData(
    FlightSqlResultSet *result_set, ColumnBinding *binding,
    int64_t starting_row, size_t cells, int64_t value_offset) {
  return CopyFromArrayValuesToBinding<UInt16Array>(result_set, binding,
                                                   starting_row, cells);
}

template <>
inline void FlightSqlAccessor<UInt16Array, CDataType_CHAR>::MoveSingleCell(
    ColumnBinding *binding, UInt16Array *array, int64_t i,
    int64_t value_offset) {
  MoveToCharBuffer(binding, array, i, value_offset);
}

/*
 * Int8Array
 */

template <>
inline size_t FlightSqlAccessor<Int8Array, CDataType_STINYINT>::GetColumnarData(
    FlightSqlResultSet *result_set, ColumnBinding *binding,
    int64_t starting_row, size_t cells, int64_t value_offset) {
  return CopyFromArrayValuesToBinding<Int8Array>(result_set, binding,
                                                 starting_row, cells);
}

template <>
inline void FlightSqlAccessor<Int8Array, CDataType_CHAR>::MoveSingleCell(
    ColumnBinding *binding, Int8Array *array, int64_t i, int64_t value_offset) {
  MoveToCharBuffer(binding, array, i, value_offset);
}

/*
 * UInt8Array
 */

template <>
inline size_t
FlightSqlAccessor<UInt8Array, CDataType_UTINYINT>::GetColumnarData(
    FlightSqlResultSet *result_set, ColumnBinding *binding,
    int64_t starting_row, size_t cells, int64_t value_offset) {
  return CopyFromArrayValuesToBinding<UInt8Array>(result_set, binding,
                                                  starting_row, cells);
}

template <>
inline void FlightSqlAccessor<UInt8Array, CDataType_CHAR>::MoveSingleCell(
    ColumnBinding *binding, UInt8Array *array, int64_t i,
    int64_t value_offset) {
  MoveToCharBuffer(binding, array, i, value_offset);
}

/*
 * FloatArray
 */

template <>
inline size_t FlightSqlAccessor<FloatArray, CDataType_FLOAT>::GetColumnarData(
    FlightSqlResultSet *result_set, ColumnBinding *binding,
    int64_t starting_row, size_t cells, int64_t value_offset) {
  return CopyFromArrayValuesToBinding<FloatArray>(result_set, binding,
                                                  starting_row, cells);
}

template <>
inline void FlightSqlAccessor<FloatArray, CDataType_CHAR>::MoveSingleCell(
    ColumnBinding *binding, FloatArray *array, int64_t i,
    int64_t value_offset) {
  MoveToCharBuffer(binding, array, i, value_offset);
}

/*
 * DoubleArray
 */

template <>
inline size_t FlightSqlAccessor<DoubleArray, CDataType_DOUBLE>::GetColumnarData(
    FlightSqlResultSet *result_set, ColumnBinding *binding,
    int64_t starting_row, size_t cells, int64_t value_offset) {
  return CopyFromArrayValuesToBinding<DoubleArray>(result_set, binding,
                                                   starting_row, cells);
}

template <>
inline void FlightSqlAccessor<DoubleArray, CDataType_CHAR>::MoveSingleCell(
    ColumnBinding *binding, DoubleArray *array, int64_t i,
    int64_t value_offset) {
  MoveToCharBuffer(binding, array, i, value_offset);
}

} // namespace flight_sql
} // namespace driver

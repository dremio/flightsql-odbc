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

#include "flight_sql_result_set_accessors.h"

namespace driver {
namespace flight_sql {

template <DataType TARGET_TYPE>
inline size_t GetColumnarData(FlightSqlResultSet *result_set,
                              const arrow::DataType &source_type,
                              ColumnBinding *binding, size_t cells,
                              int64_t value_offset) {
#define ONE_CASE(SOURCE_TYPE)                                                  \
  case SOURCE_TYPE:                                                            \
    return GetColumnarData<SOURCE_TYPE, TARGET_TYPE>(result_set, binding,      \
                                                     cells, value_offset);

  switch (source_type.id()) {
    ONE_CASE(arrow::Type::NA)
    ONE_CASE(arrow::Type::BOOL)
    ONE_CASE(arrow::Type::UINT8)
    ONE_CASE(arrow::Type::INT8)
    ONE_CASE(arrow::Type::UINT16)
    ONE_CASE(arrow::Type::INT16)
    ONE_CASE(arrow::Type::UINT32)
    ONE_CASE(arrow::Type::INT32)
    ONE_CASE(arrow::Type::UINT64)
    ONE_CASE(arrow::Type::INT64)
    ONE_CASE(arrow::Type::HALF_FLOAT)
    ONE_CASE(arrow::Type::FLOAT)
    ONE_CASE(arrow::Type::DOUBLE)
    ONE_CASE(arrow::Type::STRING)
    ONE_CASE(arrow::Type::BINARY)
    ONE_CASE(arrow::Type::FIXED_SIZE_BINARY)
    ONE_CASE(arrow::Type::DATE32)
    ONE_CASE(arrow::Type::DATE64)
    ONE_CASE(arrow::Type::TIMESTAMP)
    ONE_CASE(arrow::Type::TIME32)
    ONE_CASE(arrow::Type::TIME64)
    ONE_CASE(arrow::Type::INTERVAL_MONTHS)
    ONE_CASE(arrow::Type::INTERVAL_DAY_TIME)
    ONE_CASE(arrow::Type::DECIMAL128)
    ONE_CASE(arrow::Type::DECIMAL256)
    ONE_CASE(arrow::Type::LIST)
    ONE_CASE(arrow::Type::STRUCT)
    ONE_CASE(arrow::Type::SPARSE_UNION)
    ONE_CASE(arrow::Type::DENSE_UNION)
    ONE_CASE(arrow::Type::DICTIONARY)
    ONE_CASE(arrow::Type::MAP)
    ONE_CASE(arrow::Type::EXTENSION)
    ONE_CASE(arrow::Type::FIXED_SIZE_LIST)
    ONE_CASE(arrow::Type::DURATION)
    ONE_CASE(arrow::Type::LARGE_STRING)
    ONE_CASE(arrow::Type::LARGE_BINARY)
    ONE_CASE(arrow::Type::LARGE_LIST)
    ONE_CASE(arrow::Type::INTERVAL_MONTH_DAY_NANO)
    ONE_CASE(arrow::Type::MAX_ID)
  }
#undef ONE_CASE
}

size_t GetColumnarData(FlightSqlResultSet *result_set,
                       const arrow::DataType &source_type,
                       ColumnBinding *binding, size_t cells,
                       int64_t value_offset) {

#define CASE_FOR_TYPE(TARGET_TYPE)                                             \
  case TARGET_TYPE:                                                            \
    return GetColumnarData<TARGET_TYPE>(result_set, source_type, binding,      \
                                        cells, value_offset);

  // TODO: Maybe use something like BOOST_PP_SEQ_ENUM? Would like not to have
  // all types mentioned one by one.
  switch (binding->target_type) {
    CASE_FOR_TYPE(odbcabstraction::UNKNOWN_TYPE)
    CASE_FOR_TYPE(odbcabstraction::CHAR)
    CASE_FOR_TYPE(odbcabstraction::NUMERIC)
    CASE_FOR_TYPE(odbcabstraction::DECIMAL)
    CASE_FOR_TYPE(odbcabstraction::INTEGER)
    CASE_FOR_TYPE(odbcabstraction::SMALLINT)
    CASE_FOR_TYPE(odbcabstraction::FLOAT)
    CASE_FOR_TYPE(odbcabstraction::REAL)
    CASE_FOR_TYPE(odbcabstraction::DOUBLE)
    CASE_FOR_TYPE(odbcabstraction::DATETIME)
    CASE_FOR_TYPE(odbcabstraction::DATE)
    CASE_FOR_TYPE(odbcabstraction::TIME)
    CASE_FOR_TYPE(odbcabstraction::TIMESTAMP)
    CASE_FOR_TYPE(odbcabstraction::VARCHAR)
    CASE_FOR_TYPE(odbcabstraction::LONGVARCHAR)
    CASE_FOR_TYPE(odbcabstraction::BINARY)
    CASE_FOR_TYPE(odbcabstraction::VARBINARY)
    CASE_FOR_TYPE(odbcabstraction::LONGVARBINARY)
    CASE_FOR_TYPE(odbcabstraction::BIGINT)
    CASE_FOR_TYPE(odbcabstraction::TINYINT)
    CASE_FOR_TYPE(odbcabstraction::BIT)
  }

#undef CASE_FOR_TYPE
}

} // namespace flight_sql
} // namespace driver
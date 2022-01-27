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
inline std::unique_ptr<Accessor>
CreateAccessor(FlightSqlResultSet *result_set,
               const arrow::DataType &source_type) {

#define ONE_CASE(ARROW_TYPE, ACCESSOR_CLASS)                                   \
  case ARROW_TYPE:                                                             \
    return std::unique_ptr<Accessor>(                                          \
        new ACCESSOR_CLASS<TARGET_TYPE>(result_set));
  switch (source_type.id()) {
    ONE_CASE(arrow::Type::NA, StringArrayAccessor)
    ONE_CASE(arrow::Type::BOOL, StringArrayAccessor)
    ONE_CASE(arrow::Type::UINT8, StringArrayAccessor)
    ONE_CASE(arrow::Type::INT8, StringArrayAccessor)
    ONE_CASE(arrow::Type::UINT16, StringArrayAccessor)
    ONE_CASE(arrow::Type::INT16, StringArrayAccessor)
    ONE_CASE(arrow::Type::UINT32, StringArrayAccessor)
    ONE_CASE(arrow::Type::INT32, StringArrayAccessor)
    ONE_CASE(arrow::Type::UINT64, StringArrayAccessor)
    ONE_CASE(arrow::Type::INT64, StringArrayAccessor)
    ONE_CASE(arrow::Type::HALF_FLOAT, StringArrayAccessor)
    ONE_CASE(arrow::Type::FLOAT, StringArrayAccessor)
    ONE_CASE(arrow::Type::DOUBLE, StringArrayAccessor)
    ONE_CASE(arrow::Type::STRING, StringArrayAccessor)
    ONE_CASE(arrow::Type::BINARY, StringArrayAccessor)
    ONE_CASE(arrow::Type::FIXED_SIZE_BINARY, StringArrayAccessor)
    ONE_CASE(arrow::Type::DATE32, StringArrayAccessor)
    ONE_CASE(arrow::Type::DATE64, StringArrayAccessor)
    ONE_CASE(arrow::Type::TIMESTAMP, StringArrayAccessor)
    ONE_CASE(arrow::Type::TIME32, StringArrayAccessor)
    ONE_CASE(arrow::Type::TIME64, StringArrayAccessor)
    ONE_CASE(arrow::Type::INTERVAL_MONTHS, StringArrayAccessor)
    ONE_CASE(arrow::Type::INTERVAL_DAY_TIME, StringArrayAccessor)
    ONE_CASE(arrow::Type::DECIMAL128, StringArrayAccessor)
    ONE_CASE(arrow::Type::DECIMAL256, StringArrayAccessor)
    ONE_CASE(arrow::Type::LIST, StringArrayAccessor)
    ONE_CASE(arrow::Type::STRUCT, StringArrayAccessor)
    ONE_CASE(arrow::Type::SPARSE_UNION, StringArrayAccessor)
    ONE_CASE(arrow::Type::DENSE_UNION, StringArrayAccessor)
    ONE_CASE(arrow::Type::DICTIONARY, StringArrayAccessor)
    ONE_CASE(arrow::Type::MAP, StringArrayAccessor)
    ONE_CASE(arrow::Type::EXTENSION, StringArrayAccessor)
    ONE_CASE(arrow::Type::FIXED_SIZE_LIST, StringArrayAccessor)
    ONE_CASE(arrow::Type::DURATION, StringArrayAccessor)
    ONE_CASE(arrow::Type::LARGE_STRING, StringArrayAccessor)
    ONE_CASE(arrow::Type::LARGE_BINARY, StringArrayAccessor)
    ONE_CASE(arrow::Type::LARGE_LIST, StringArrayAccessor)
    ONE_CASE(arrow::Type::INTERVAL_MONTH_DAY_NANO, StringArrayAccessor)
    ONE_CASE(arrow::Type::MAX_ID, StringArrayAccessor)
  }
#undef ONE_CASE
}

std::unique_ptr<Accessor> CreateAccessor(FlightSqlResultSet *result_set,
                                         const arrow::DataType &source_type,
                                         DataType target_type) {

#define CASE_FOR_TYPE(TYPE)                                                    \
  case TYPE:                                                                   \
    return CreateAccessor<TYPE>(result_set, source_type);

  // TODO: Maybe use something like BOOST_PP_SEQ_ENUM? Would like not to have
  // all types mentioned one by one.
  switch (target_type) {
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
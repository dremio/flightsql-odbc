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

template <CDataType TARGET_TYPE>
inline std::unique_ptr<Accessor>
CreateAccessor(const arrow::DataType &source_type) {
#define ONE_CASE(ARROW_TYPE)                                                   \
  case arrow::ARROW_TYPE##Type::type_id:                                       \
    return std::unique_ptr<Accessor>(                                          \
        new FlightSqlAccessor<arrow::ARROW_TYPE##Array, TARGET_TYPE>());

  switch (source_type.id()) {
    ONE_CASE(Boolean)
    ONE_CASE(UInt8)
    ONE_CASE(Int8)
    ONE_CASE(UInt16)
    ONE_CASE(Int16)
    ONE_CASE(UInt32)
    ONE_CASE(Int32)
    ONE_CASE(UInt64)
    ONE_CASE(Int64)
    ONE_CASE(HalfFloat)
    ONE_CASE(Float)
    ONE_CASE(Double)
    ONE_CASE(String)
    ONE_CASE(Binary)
    ONE_CASE(FixedSizeBinary)
    ONE_CASE(Date32)
    ONE_CASE(Date64)
    ONE_CASE(Timestamp)
    ONE_CASE(Time32)
    ONE_CASE(Time64)
    //    ONE_CASE(IntervalMonths)
    //    ONE_CASE(IntervalDayTime)
    ONE_CASE(Decimal128)
    ONE_CASE(Decimal256)
    ONE_CASE(List)
    ONE_CASE(Struct)
    ONE_CASE(SparseUnion)
    ONE_CASE(DenseUnion)
    ONE_CASE(Dictionary)
    ONE_CASE(Map)
    //    ONE_CASE(Extension)
    ONE_CASE(FixedSizeList)
    ONE_CASE(Duration)
    ONE_CASE(LargeString)
    ONE_CASE(LargeBinary)
    ONE_CASE(LargeList)
    //    ONE_CASE(IntervalMonthDayNano)
    //    ONE_CASE(MaxId)
  default:
    break;
  }
#undef ONE_CASE

  throw odbcabstraction::DriverException("Unreachable");
}

std::unique_ptr<Accessor>
CreateAccessor(const arrow::DataType &source_type,
               odbcabstraction::CDataType target_type) {

#define CASE_FOR_TYPE(TARGET_TYPE)                                             \
  case TARGET_TYPE:                                                            \
    return CreateAccessor<TARGET_TYPE>(source_type);

  // TODO: Maybe use something like BOOST_PP_SEQ_ENUM? Would like not to have
  // all types mentioned one by one.
  switch (target_type) {
    CASE_FOR_TYPE(CDataType_CHAR)
    CASE_FOR_TYPE(CDataType_WCHAR)
    CASE_FOR_TYPE(CDataType_SSHORT)
    CASE_FOR_TYPE(CDataType_USHORT)
    CASE_FOR_TYPE(CDataType_SLONG)
    CASE_FOR_TYPE(CDataType_ULONG)
    CASE_FOR_TYPE(CDataType_FLOAT)
    CASE_FOR_TYPE(CDataType_DOUBLE)
    CASE_FOR_TYPE(CDataType_BIT)
    CASE_FOR_TYPE(CDataType_STINYINT)
    CASE_FOR_TYPE(CDataType_UTINYINT)
    CASE_FOR_TYPE(CDataType_SBIGINT)
    CASE_FOR_TYPE(CDataType_UBIGINT)
    CASE_FOR_TYPE(CDataType_BINARY)
  }
#undef CASE_FOR_TYPE

  throw odbcabstraction::DriverException("Unreachable");
}

} // namespace flight_sql
} // namespace driver

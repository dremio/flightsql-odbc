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

#include "accessors/main.h"
#include "boost/functional/hash.hpp"

namespace driver {
namespace flight_sql {

using odbcabstraction::CDataType;

typedef std::pair<arrow::Type::type, CDataType> SourceAndTargetPair;
typedef std::function<Accessor *(arrow::Array *)> AccessorConstructor;

std::unordered_map<SourceAndTargetPair, AccessorConstructor,
                   boost::hash<SourceAndTargetPair>>
    ACCESSORS_CONSTRUCTORS = {
        {SourceAndTargetPair(arrow::Type::type::STRING, CDataType_CHAR),
         [](arrow::Array *source_array) {
           return new StringArrayFlightSqlAccessor<CDataType_CHAR>(
               source_array);
         }},
        {SourceAndTargetPair(arrow::Type::type::STRING, CDataType_WCHAR),
         [](arrow::Array *source_array) {
           return new StringArrayFlightSqlAccessor<CDataType_CHAR>(
               source_array);
         }},
        {SourceAndTargetPair(arrow::Type::type::DOUBLE, CDataType_DOUBLE),
         [](arrow::Array *source_array) {
           return new DoubleArrayFlightSqlAccessor<CDataType_DOUBLE>(
               source_array);
         }},
        {SourceAndTargetPair(arrow::Type::type::FLOAT, CDataType_FLOAT),
         [](arrow::Array *source_array) {
           return new FloatArrayFlightSqlAccessor<CDataType_FLOAT>(
               source_array);
         }},
        {SourceAndTargetPair(arrow::Type::type::INT64, CDataType_SBIGINT),
         [](arrow::Array *source_array) {
           return new Int64ArrayFlightSqlAccessor<CDataType_SBIGINT>(
               source_array);
         }},
        {SourceAndTargetPair(arrow::Type::type::UINT64, CDataType_UBIGINT),
         [](arrow::Array *source_array) {
           return new UInt64ArrayFlightSqlAccessor<CDataType_UBIGINT>(
               source_array);
         }},
        {SourceAndTargetPair(arrow::Type::type::INT32, CDataType_SLONG),
         [](arrow::Array *source_array) {
           return new Int32ArrayFlightSqlAccessor<CDataType_SLONG>(
               source_array);
         }},
        {SourceAndTargetPair(arrow::Type::type::UINT32, CDataType_ULONG),
         [](arrow::Array *source_array) {
           return new UInt32ArrayFlightSqlAccessor<CDataType_ULONG>(
               source_array);
         }},
        {SourceAndTargetPair(arrow::Type::type::INT16, CDataType_SSHORT),
         [](arrow::Array *source_array) {
           return new Int16ArrayFlightSqlAccessor<CDataType_SSHORT>(
               source_array);
         }},
        {SourceAndTargetPair(arrow::Type::type::UINT16, CDataType_USHORT),
         [](arrow::Array *source_array) {
           return new UInt16ArrayFlightSqlAccessor<CDataType_USHORT>(
               source_array);
         }},
        {SourceAndTargetPair(arrow::Type::type::INT8, CDataType_STINYINT),
         [](arrow::Array *source_array) {
           return new Int8ArrayFlightSqlAccessor<CDataType_STINYINT>(
               source_array);
         }},
        {SourceAndTargetPair(arrow::Type::type::UINT8, CDataType_UTINYINT),
         [](arrow::Array *source_array) {
           return new UInt8ArrayFlightSqlAccessor<CDataType_UTINYINT>(
               source_array);
         }}};

std::unique_ptr<Accessor> CreateAccessor(arrow::Array *source_array,
                                         CDataType target_type) {
  auto it = ACCESSORS_CONSTRUCTORS.find(
      SourceAndTargetPair(source_array->type_id(), target_type));
  if (it != ACCESSORS_CONSTRUCTORS.end()) {
    auto accessor = it->second(source_array);
    return std::unique_ptr<Accessor>(accessor);
  }

  std::stringstream ss;
  ss << "Unsupported type conversion! Tried to convert '"
     << source_array->type()->ToString() << "' to C type '" << target_type
     << "'";
  throw odbcabstraction::DriverException(ss.str());
}

} // namespace flight_sql
} // namespace driver

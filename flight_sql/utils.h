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

#include <arrow/flight/types.h>
#include <arrow/util/optional.h>
#include <boost/xpressive/xpressive.hpp>
#include <codecvt>
#include <odbcabstraction/exceptions.h>
#include <odbcabstraction/types.h>

namespace driver {
namespace flight_sql {

using arrow::util::optional;

#ifdef WITH_IODBC
using SqlWChar = char32_t;
using SqlWString = std::u32string;
#else
using SqlWChar = char16_t;
using SqlWString = std::u16string;
#endif
using CharToWStrConverter =
    std::wstring_convert<std::codecvt_utf8<SqlWChar>, SqlWChar>;

inline void ThrowIfNotOK(const arrow::Status &status) {
  if (!status.ok()) {
    throw odbcabstraction::DriverException(status.message());
  }
}

template <typename T, typename AttributeTypeT>
inline bool CheckIfSetToOnlyValidValue(const AttributeTypeT &value, T allowed_value) {
  return boost::get<T>(value) == allowed_value;
}

template <typename BUILDER, typename T>
arrow::Status AppendToBuilder(BUILDER &builder, optional<T> opt_value) {
  if (opt_value) {
    return builder.Append(*opt_value);
  } else {
    return builder.AppendNull();
  }
}

template <typename BUILDER, typename T>
arrow::Status AppendToBuilder(BUILDER &builder, T value) {
  return builder.Append(value);
}

odbcabstraction::SqlDataType
GetDataTypeFromArrowField_V3(const std::shared_ptr<arrow::Field> &field);

int16_t ConvertSqlDataTypeFromV3ToV2(int16_t data_type_v3);

odbcabstraction::CDataType ConvertCDataTypeFromV2ToV3(int16_t data_type_v2);

std::string GetTypeNameFromSqlDataType(int16_t data_type);

optional<int16_t>
GetRadixFromSqlDataType(odbcabstraction::SqlDataType data_type);

int16_t GetNonConciseDataType(odbcabstraction::SqlDataType data_type);

optional<int16_t> GetSqlDateTimeSubCode(odbcabstraction::SqlDataType data_type);

optional<int32_t> GetCharOctetLength(odbcabstraction::SqlDataType data_type,
                                     const optional<int32_t>& column_size);

optional<int32_t> GetBufferLength(odbcabstraction::SqlDataType data_type,
                                  const optional<int32_t>& column_size);

optional<int32_t> GetDisplaySize(odbcabstraction::SqlDataType data_type,
                                 const optional<int32_t>& column_size);

std::string ConvertSqlPatternToRegexString(const std::string &pattern);

boost::xpressive::sregex ConvertSqlPatternToRegex(const std::string &pattern);

} // namespace flight_sql
} // namespace driver

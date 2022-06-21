/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#pragma once

#include <arrow/flight/types.h>
#include <arrow/util/optional.h>
#include <boost/xpressive/xpressive.hpp>
#include <codecvt>
#include <odbcabstraction/exceptions.h>
#include <odbcabstraction/types.h>

namespace driver {
namespace flight_sql {

typedef std::function<
  std::shared_ptr<arrow::Array>(std::shared_ptr<arrow::Array>)>
  ArrayConvertTask;

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
                                     const arrow::Result<int32_t>& column_size,
                                     const int32_t decimal_precison=0);

optional<int32_t> GetBufferLength(odbcabstraction::SqlDataType data_type,
                                  const optional<int32_t>& column_size);

optional<int32_t> GetTypeScale(odbcabstraction::SqlDataType data_type,
                                  const optional<int32_t>& type_scale);

optional<int32_t> GetColumnSize(odbcabstraction::SqlDataType data_type,
                                  const optional<int32_t>& column_size);

optional<int32_t> GetDisplaySize(odbcabstraction::SqlDataType data_type,
                                 const optional<int32_t>& column_size);

std::string ConvertSqlPatternToRegexString(const std::string &pattern);

boost::xpressive::sregex ConvertSqlPatternToRegex(const std::string &pattern);

bool NeedArrayConversion(arrow::Type::type original_type_id,
                         odbcabstraction::CDataType data_type);

std::shared_ptr<arrow::DataType> GetDefaultDataTypeForTypeId(arrow::Type::type type_id);

arrow::Type::type ConvertCToArrowType(odbcabstraction::CDataType data_type);

odbcabstraction::CDataType ConvertArrowTypeToC(arrow::Type::type type_id);

std::shared_ptr<arrow::Array> CheckConversion(const arrow::Result<arrow::Datum> &result);

ArrayConvertTask GetConverter(arrow::Type::type original_type_id,
                              odbcabstraction::CDataType target_type);

std::string ConvertToDBMSVer(const std::string& str);

int32_t GetDecimalTypeScale(const std::shared_ptr<arrow::DataType>& decimalType);

int32_t GetDecimalTypePrecision(const std::shared_ptr<arrow::DataType>& decimalType);

} // namespace flight_sql
} // namespace driver

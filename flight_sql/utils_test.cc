/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include "utils.h"
#include "arrow/testing/builder.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"
#include <odbcabstraction/platform.h> 
#include "gtest/gtest.h"

namespace driver {
namespace flight_sql {

void AssertConvertedArray(const std::shared_ptr<arrow::Array>& original_array,
                          const std::shared_ptr<arrow::Array>& converted_array,
                          uint64_t size,
                          arrow::Type::type arrow_type) {

  ASSERT_EQ(converted_array->type_id(), arrow_type);
  ASSERT_EQ(converted_array->length(),size);
  AssertArraysEqual(original_array, converted_array);
}

std::shared_ptr<arrow::Array> convertArray(
  const std::shared_ptr<arrow::Array>& original_array,
  odbcabstraction::CDataType c_type) {
  auto converter = GetConverter(original_array->type_id(),
                                c_type);
  return converter(original_array);
}

void TestArrayConversion(const std::vector<std::string>& input,
                           odbcabstraction::CDataType c_type,
                           arrow::Type::type arrow_type) {
  std::shared_ptr<arrow::Array> array;
  arrow::ArrayFromVector<arrow::StringType, std::string>(input, &array);

  auto converted_array = convertArray(array, c_type);

  AssertConvertedArray(array, converted_array, input.size(), arrow_type);
}

void TestTime32ArrayConversion(const std::vector<int32_t>& input,
                         odbcabstraction::CDataType c_type,
                         arrow::Type::type arrow_type) {
  std::shared_ptr<arrow::Array> array;
  arrow::ArrayFromVector<arrow::Time32Type, int32_t>(time32(arrow::TimeUnit::MILLI),
                                                     input, &array);

  auto converted_array = convertArray(array, c_type);

  AssertConvertedArray(array, converted_array, input.size(), arrow_type);
}

void TestTime64ArrayConversion(const std::vector<int64_t>& input,
                               odbcabstraction::CDataType c_type,
                               arrow::Type::type arrow_type) {
  std::shared_ptr<arrow::Array> array;
  arrow::ArrayFromVector<arrow::Time64Type, int64_t>(time64(arrow::TimeUnit::NANO),
                                                     input, &array);

  auto converted_array = convertArray(array, c_type);

  AssertConvertedArray(array, converted_array, input.size(), arrow_type);;
}


TEST(Utils, Time32ToTimeStampArray) {
    TestTime32ArrayConversion({14896, 17820},
                        odbcabstraction::CDataType_TIMESTAMP,
                        arrow::Type::TIMESTAMP);
}

TEST(Utils, Time64ToTimeStampArray) {
    TestTime64ArrayConversion({1489272000000, 1789270000000},
                          odbcabstraction::CDataType_TIMESTAMP,
                          arrow::Type::TIMESTAMP);
}

TEST(Utils, StringToDateArray) {
  TestArrayConversion({"2020-01-20", "2022-03-10"},
                      odbcabstraction::CDataType_DATE, arrow::Type::DATE64);
}

TEST(Utils, StringToTimeArray) {
  TestArrayConversion({"10:00", "12:00"},
                      odbcabstraction::CDataType_TIME, arrow::Type::TIME64);
}

TEST(Utils, StringToTimeStampArray) {
  TestArrayConversion({"2013-05-27 19:15:00", "2013-05-31 16:40:00"},
                      odbcabstraction::CDataType_TIMESTAMP, arrow::Type::TIMESTAMP);
}

TEST(Utils, ConvertSqlPatternToRegexString) {
  ASSERT_EQ(std::string("XY"), ConvertSqlPatternToRegexString("XY"));
  ASSERT_EQ(std::string("X.Y"), ConvertSqlPatternToRegexString("X_Y"));
  ASSERT_EQ(std::string("X.*Y"), ConvertSqlPatternToRegexString("X%Y"));
  ASSERT_EQ(std::string("X%Y"), ConvertSqlPatternToRegexString("X\\%Y"));
  ASSERT_EQ(std::string("X_Y"), ConvertSqlPatternToRegexString("X\\_Y"));
}

TEST(Utils, ConvertToDBMSVer) {
  ASSERT_EQ(std::string("01.02.0003"), ConvertToDBMSVer("1.2.3"));
  ASSERT_EQ(std::string("01.02.0003.0"), ConvertToDBMSVer("1.2.3.0"));
  ASSERT_EQ(std::string("01.02.0000"), ConvertToDBMSVer("1.2"));
  ASSERT_EQ(std::string("01.00.0000"), ConvertToDBMSVer("1"));
  ASSERT_EQ(std::string("01.02.0000-foo"), ConvertToDBMSVer("1.2-foo"));
  ASSERT_EQ(std::string("01.00.0000-foo"), ConvertToDBMSVer("1-foo"));
  ASSERT_EQ(std::string("10.11.0001-foo"), ConvertToDBMSVer("10.11.1-foo"));
}

} // namespace flight_sql
} // namespace driver

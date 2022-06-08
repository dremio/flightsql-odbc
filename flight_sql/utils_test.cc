/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include "utils.h"

#include "odbcabstraction/calendar_utils.h"

#include "arrow/testing/builder.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"
#include "gtest/gtest.h"

namespace driver {
namespace flight_sql {

void AssertConvertedArray(const std::shared_ptr<arrow::Array>& expected_array,
                          const std::shared_ptr<arrow::Array>& converted_array,
                          uint64_t size,
                          arrow::Type::type arrow_type) {
  ASSERT_EQ(converted_array->type_id(), arrow_type);
  ASSERT_EQ(converted_array->length(),size);
  ASSERT_EQ(expected_array->ToString(), converted_array->ToString());
}

std::shared_ptr<arrow::Array> convertArray(
  const std::shared_ptr<arrow::Array>& original_array,
  odbcabstraction::CDataType c_type) {
  auto converter = GetConverter(original_array->type_id(),
                                c_type);
  return converter(original_array);
}

void TestArrayConversion(const std::vector<std::string>& input,
                         const std::shared_ptr<arrow::Array>& expected_array,
                         odbcabstraction::CDataType c_type,
                         arrow::Type::type arrow_type) {
  std::shared_ptr<arrow::Array> original_array;
  arrow::ArrayFromVector<arrow::StringType, std::string>(input, &original_array);

  auto converted_array = convertArray(original_array, c_type);

  AssertConvertedArray(expected_array, converted_array, input.size(), arrow_type);
}

void TestTime32ArrayConversion(const std::vector<int32_t>& input,
                               const std::shared_ptr<arrow::Array>& expected_array,
                               odbcabstraction::CDataType c_type,
                               arrow::Type::type arrow_type) {
  std::shared_ptr<arrow::Array> original_array;
  arrow::ArrayFromVector<arrow::Time32Type, int32_t>(time32(arrow::TimeUnit::MILLI),
                                                     input, &original_array);

  auto converted_array = convertArray(original_array, c_type);

  AssertConvertedArray(expected_array, converted_array, input.size(), arrow_type);
}

void TestTime64ArrayConversion(const std::vector<int64_t>& input,
                               const std::shared_ptr<arrow::Array>& expected_array,
                               odbcabstraction::CDataType c_type,
                               arrow::Type::type arrow_type) {
  std::shared_ptr<arrow::Array> original_array;
  arrow::ArrayFromVector<arrow::Time64Type, int64_t>(time64(arrow::TimeUnit::NANO),
                                                     input, &original_array);

  auto converted_array = convertArray(original_array, c_type);

  AssertConvertedArray(expected_array, converted_array, input.size(), arrow_type);
}

TEST(Utils, Time32ToTimeStampArray) {
  std::vector<int32_t> input_data = {14896, 17820};

  const auto seconds_from_epoch = odbcabstraction::GetTodayTimeFromEpoch();
  std::vector<int64_t> expected_data;
  expected_data.reserve(2);

  for (const auto &item : input_data) {
    expected_data.emplace_back(item + seconds_from_epoch * 1000);
  }

  std::shared_ptr<arrow::Array> expected;
  auto timestamp_field = field("timestamp_field", timestamp(arrow::TimeUnit::MILLI));
  arrow::ArrayFromVector<arrow::TimestampType, int64_t>(timestamp_field->type(),
                                                        expected_data, &expected);

  TestTime32ArrayConversion(input_data, expected,
                            odbcabstraction::CDataType_TIMESTAMP,
                            arrow::Type::TIMESTAMP);
}

TEST(Utils, Time64ToTimeStampArray) {
  std::vector<int64_t> input_data = {1579489200000, 1646881200000};

  const auto seconds_from_epoch = odbcabstraction::GetTodayTimeFromEpoch();
  std::vector<int64_t> expected_data;
  expected_data.reserve(2);

  for (const auto &item : input_data) {
    expected_data.emplace_back(item + seconds_from_epoch * 1000000000);
  }

  std::shared_ptr<arrow::Array> expected;
  auto timestamp_field = field("timestamp_field", timestamp(arrow::TimeUnit::NANO));
  arrow::ArrayFromVector<arrow::TimestampType, int64_t>(timestamp_field->type(),
                                                        expected_data, &expected);

  TestTime64ArrayConversion(input_data, expected,
                            odbcabstraction::CDataType_TIMESTAMP,
                            arrow::Type::TIMESTAMP);
}

TEST(Utils, StringToDateArray) {
  std::shared_ptr<arrow::Array> expected;
  arrow::ArrayFromVector<arrow::Date64Type, int64_t>(
    {1579489200000, 1646881200000}, &expected);

  TestArrayConversion({"2020-01-20", "2022-03-10"}, expected,
                      odbcabstraction::CDataType_DATE,
                      arrow::Type::DATE64);
}

TEST(Utils, StringToTimeArray) {
  std::shared_ptr<arrow::Array> expected;
  arrow::ArrayFromVector<arrow::Time64Type, int64_t>(time64(arrow::TimeUnit::MICRO),
                                                     {36000000000, 43200000000}, &expected);

  TestArrayConversion({"10:00", "12:00"}, expected,
                      odbcabstraction::CDataType_TIME, arrow::Type::TIME64);
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

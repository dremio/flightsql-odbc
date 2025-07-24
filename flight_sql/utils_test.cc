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
#include "arrow/util/decimal.h"
#include "arrow/array/builder_decimal.h"
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

// Tests for FormatDecimalWithoutScientificNotation function
TEST(Utils, FormatDecimalWithoutScientificNotation_ZeroScale) {
  // Test integers (scale = 0)
  auto result1 = arrow::Decimal128::FromString("123");
  ASSERT_TRUE(result1.ok());
  arrow::Decimal128 value1 = result1.ValueOrDie();
  ASSERT_EQ("123", FormatDecimalWithoutScientificNotation(value1, 0));

  auto result2 = arrow::Decimal128::FromString("-456");
  ASSERT_TRUE(result2.ok());
  arrow::Decimal128 value2 = result2.ValueOrDie();
  ASSERT_EQ("-456", FormatDecimalWithoutScientificNotation(value2, 0));

  auto result_zero = arrow::Decimal128::FromString("0");
  ASSERT_TRUE(result_zero.ok());
  arrow::Decimal128 zero = result_zero.ValueOrDie();
  ASSERT_EQ("0", FormatDecimalWithoutScientificNotation(zero, 0));
}

TEST(Utils, FormatDecimalWithoutScientificNotation_PositiveScale) {
  // Test normal decimal formatting
  auto result1 = arrow::Decimal128::FromString("12345");
  ASSERT_TRUE(result1.ok());
  arrow::Decimal128 value1 = result1.ValueOrDie();
  ASSERT_EQ("123.45", FormatDecimalWithoutScientificNotation(value1, 2));

  auto result2 = arrow::Decimal128::FromString("-12345");
  ASSERT_TRUE(result2.ok());
  arrow::Decimal128 value2 = result2.ValueOrDie();
  ASSERT_EQ("-123.45", FormatDecimalWithoutScientificNotation(value2, 2));

  // Test with more digits than scale
  auto result3 = arrow::Decimal128::FromString("1234567");
  ASSERT_TRUE(result3.ok());
  arrow::Decimal128 value3 = result3.ValueOrDie();
  ASSERT_EQ("1234.567", FormatDecimalWithoutScientificNotation(value3, 3));

  // Test single digit with scale
  auto result4 = arrow::Decimal128::FromString("5");
  ASSERT_TRUE(result4.ok());
  arrow::Decimal128 value4 = result4.ValueOrDie();
  ASSERT_EQ("0.05", FormatDecimalWithoutScientificNotation(value4, 2));
}

TEST(Utils, FormatDecimalWithoutScientificNotation_LeadingZeros) {
  // Test cases where we need leading zeros
  auto result1 = arrow::Decimal128::FromString("123");
  ASSERT_TRUE(result1.ok());
  arrow::Decimal128 value1 = result1.ValueOrDie();
  ASSERT_EQ("0.00123", FormatDecimalWithoutScientificNotation(value1, 5));

  auto result2 = arrow::Decimal128::FromString("-123");
  ASSERT_TRUE(result2.ok());
  arrow::Decimal128 value2 = result2.ValueOrDie();
  ASSERT_EQ("-0.00123", FormatDecimalWithoutScientificNotation(value2, 5));

  // Test with scale equal to number of digits
  auto result3 = arrow::Decimal128::FromString("123");
  ASSERT_TRUE(result3.ok());
  arrow::Decimal128 value3 = result3.ValueOrDie();
  ASSERT_EQ("0.123", FormatDecimalWithoutScientificNotation(value3, 3));

  // Test single digit with large scale
  auto result4 = arrow::Decimal128::FromString("1");
  ASSERT_TRUE(result4.ok());
  arrow::Decimal128 value4 = result4.ValueOrDie();
  ASSERT_EQ("0.0001", FormatDecimalWithoutScientificNotation(value4, 4));
}

TEST(Utils, FormatDecimalWithoutScientificNotation_NegativeScale) {
  // Test negative scale (adds trailing zeros)
  auto result1 = arrow::Decimal128::FromString("123");
  ASSERT_TRUE(result1.ok());
  arrow::Decimal128 value1 = result1.ValueOrDie();
  ASSERT_EQ("12300", FormatDecimalWithoutScientificNotation(value1, -2));

  auto result2 = arrow::Decimal128::FromString("-456");
  ASSERT_TRUE(result2.ok());
  arrow::Decimal128 value2 = result2.ValueOrDie();
  ASSERT_EQ("-45600", FormatDecimalWithoutScientificNotation(value2, -2));

  auto result_zero = arrow::Decimal128::FromString("0");
  ASSERT_TRUE(result_zero.ok());
  arrow::Decimal128 zero = result_zero.ValueOrDie();
  ASSERT_EQ("0000", FormatDecimalWithoutScientificNotation(zero, -3));
}

TEST(Utils, FormatDecimalWithoutScientificNotation_LargeNumbers) {
  // Test very large numbers
  auto result_large_pos = arrow::Decimal128::FromString("123456789012345678901234567890");
  ASSERT_TRUE(result_large_pos.ok());
  arrow::Decimal128 large_positive = result_large_pos.ValueOrDie();
  ASSERT_EQ("1234567890123456789012345678.90", FormatDecimalWithoutScientificNotation(large_positive, 2));

  auto result_large_neg = arrow::Decimal128::FromString("-123456789012345678901234567890");
  ASSERT_TRUE(result_large_neg.ok());
  arrow::Decimal128 large_negative = result_large_neg.ValueOrDie();
  ASSERT_EQ("-1234567890123456789012345678.90", FormatDecimalWithoutScientificNotation(large_negative, 2));

  // Test maximum precision decimal
  auto result_max = arrow::Decimal128::FromString("99999999999999999999999999999999999999");
  ASSERT_TRUE(result_max.ok());
  arrow::Decimal128 max_decimal = result_max.ValueOrDie();
  ASSERT_EQ("999999999999999999999999999999999999.99", FormatDecimalWithoutScientificNotation(max_decimal, 2));
}

TEST(Utils, FormatDecimalWithoutScientificNotation_EdgeCases) {
  // Test minimum negative value
  auto result_min = arrow::Decimal128::FromString("-99999999999999999999999999999999999999");
  ASSERT_TRUE(result_min.ok());
  arrow::Decimal128 min_value = result_min.ValueOrDie();
  ASSERT_EQ("-999999999999999999999999999999999999.99", FormatDecimalWithoutScientificNotation(min_value, 2));

  // Test very small positive number
  auto result_tiny_pos = arrow::Decimal128::FromString("1");
  ASSERT_TRUE(result_tiny_pos.ok());
  arrow::Decimal128 tiny_positive = result_tiny_pos.ValueOrDie();
  ASSERT_EQ("0.000000000000000000000000000000000001", FormatDecimalWithoutScientificNotation(tiny_positive, 36));

  // Test very small negative number
  auto result_tiny_neg = arrow::Decimal128::FromString("-1");
  ASSERT_TRUE(result_tiny_neg.ok());
  arrow::Decimal128 tiny_negative = result_tiny_neg.ValueOrDie();
  ASSERT_EQ("-0.000000000000000000000000000000000001", FormatDecimalWithoutScientificNotation(tiny_negative, 36));

  // Test scale of 1
  auto result_scale_one = arrow::Decimal128::FromString("123");
  ASSERT_TRUE(result_scale_one.ok());
  arrow::Decimal128 scale_one = result_scale_one.ValueOrDie();
  ASSERT_EQ("12.3", FormatDecimalWithoutScientificNotation(scale_one, 1));
}

TEST(Utils, FormatDecimalWithoutScientificNotation_FinancialNumbers) {
  // Test common financial numbers
  auto result_price1 = arrow::Decimal128::FromString("199999");
  ASSERT_TRUE(result_price1.ok());
  arrow::Decimal128 price1 = result_price1.ValueOrDie();
  ASSERT_EQ("1999.99", FormatDecimalWithoutScientificNotation(price1, 2));

  auto result_price2 = arrow::Decimal128::FromString("50");
  ASSERT_TRUE(result_price2.ok());
  arrow::Decimal128 price2 = result_price2.ValueOrDie();
  ASSERT_EQ("0.50", FormatDecimalWithoutScientificNotation(price2, 2));

  auto result_price3 = arrow::Decimal128::FromString("1");
  ASSERT_TRUE(result_price3.ok());
  arrow::Decimal128 price3 = result_price3.ValueOrDie();
  ASSERT_EQ("0.01", FormatDecimalWithoutScientificNotation(price3, 2));

  // Test negative financial numbers
  auto result_loss = arrow::Decimal128::FromString("-123456");
  ASSERT_TRUE(result_loss.ok());
  arrow::Decimal128 loss = result_loss.ValueOrDie();
  ASSERT_EQ("-1234.56", FormatDecimalWithoutScientificNotation(loss, 2));
}

TEST(Utils, FormatDecimalWithoutScientificNotation_ScientificAvoidance) {
  // Test numbers that would normally be in scientific notation
  auto result_very_large = arrow::Decimal128::FromString("1000000000000000000");
  ASSERT_TRUE(result_very_large.ok());
  arrow::Decimal128 very_large = result_very_large.ValueOrDie();
  ASSERT_EQ("10000000000000000.00", FormatDecimalWithoutScientificNotation(very_large, 2));

  auto result_very_small = arrow::Decimal128::FromString("1");
  ASSERT_TRUE(result_very_small.ok());
  arrow::Decimal128 very_small = result_very_small.ValueOrDie();
  ASSERT_EQ("0.00000000000000000001", FormatDecimalWithoutScientificNotation(very_small, 20));

  // Test numbers with many trailing zeros when converted
  auto result_trailing = arrow::Decimal128::FromString("123000");
  ASSERT_TRUE(result_trailing.ok());
  arrow::Decimal128 trailing_zeros = result_trailing.ValueOrDie();
  ASSERT_EQ("123.000", FormatDecimalWithoutScientificNotation(trailing_zeros, 3));
}

// Test decimal to string array conversion
TEST(Utils, DecimalToStringArrayConversion) {
  // Create decimal array with various values
  auto decimal_type = std::make_shared<arrow::Decimal128Type>(10, 2);

  std::vector<arrow::Decimal128> decimal_values;

  auto result_val1 = arrow::Decimal128::FromString("12345");
  ASSERT_TRUE(result_val1.ok());
  arrow::Decimal128 val1 = result_val1.ValueOrDie();  // 123.45

  auto result_val2 = arrow::Decimal128::FromString("-6789");
  ASSERT_TRUE(result_val2.ok());
  arrow::Decimal128 val2 = result_val2.ValueOrDie();  // -67.89

  auto result_val3 = arrow::Decimal128::FromString("0");
  ASSERT_TRUE(result_val3.ok());
  arrow::Decimal128 val3 = result_val3.ValueOrDie();      // 0.00

  auto result_val4 = arrow::Decimal128::FromString("1");
  ASSERT_TRUE(result_val4.ok());
  arrow::Decimal128 val4 = result_val4.ValueOrDie();      // 0.01

  decimal_values = {val1, val2, val3, val4};

  // Create decimal array manually using builder
  arrow::Decimal128Builder builder(decimal_type);
  for (const auto& value : decimal_values) {
    ASSERT_TRUE(builder.Append(value).ok());
  }

  auto result = builder.Finish();
  ASSERT_TRUE(result.ok());
  std::shared_ptr<arrow::Array> decimal_array = result.ValueOrDie();

  // Convert to string array
  auto converted_array = convertArray(decimal_array, odbcabstraction::CDataType_CHAR);

  // Verify conversion
  ASSERT_EQ(converted_array->type_id(), arrow::Type::STRING);
  ASSERT_EQ(converted_array->length(), 4);

  auto string_array = std::static_pointer_cast<arrow::StringArray>(converted_array);
  ASSERT_EQ(string_array->GetString(0), "123.45");
  ASSERT_EQ(string_array->GetString(1), "-67.89");
  ASSERT_EQ(string_array->GetString(2), "0.00");
  ASSERT_EQ(string_array->GetString(3), "0.01");
}

} // namespace flight_sql
} // namespace driver

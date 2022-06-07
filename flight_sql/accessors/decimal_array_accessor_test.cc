/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include "arrow/util/decimal.h"
#include "arrow/builder.h"
#include "arrow/testing/builder.h"
#include "decimal_array_accessor.h"
#include "gtest/gtest.h"

namespace {

using namespace arrow;
using namespace driver::odbcabstraction;
using driver::flight_sql::ThrowIfNotOK;

std::vector <Decimal128> MakeDecimalVector(const std::vector <std::string> &values,
                                           int32_t scale) {
  std::vector <arrow::Decimal128> ret;
  for (const auto &str: values) {
    Decimal128 str_value;
    int32_t str_precision;
    int32_t str_scale;

    ThrowIfNotOK(Decimal128::FromString(str, &str_value, &str_precision, &str_scale));

    Decimal128 scaled_value;
    if (str_scale == scale) {
      scaled_value = str_value;
    } else {
      scaled_value = str_value.Rescale(str_scale, scale).ValueOrDie();
    }
    ret.push_back(scaled_value);
  }
  return ret;
}

std::string ConvertNumericToString(NUMERIC_STRUCT &numeric) {
  auto v = reinterpret_cast<int64_t *>(numeric.val);
  auto decimal = Decimal128(v[1], v[0]);
  const std::string &string = decimal.ToString(numeric.scale);

  return (numeric.sign ? "" : "-") + string;
}
}

namespace driver {
namespace flight_sql {

void AssertNumericOutput(int input_precision, int input_scale, const std::vector <std::string> &values_str,
                         int output_precision, int output_scale, const std::vector <std::string> &expected_values_str) {
  auto decimal_type = std::make_shared<arrow::Decimal128Type>(input_precision, input_scale);
  const std::vector <Decimal128> &values = MakeDecimalVector(values_str, decimal_type->scale());

  std::shared_ptr <Array> array;
  ArrayFromVector<Decimal128Type, Decimal128>(decimal_type, values, &array);

  DecimalArrayFlightSqlAccessor <Decimal128Array, CDataType_NUMERIC> accessor(array.get());

  std::vector <NUMERIC_STRUCT> buffer(values.size());
  std::vector <ssize_t> strlen_buffer(values.size());

  ColumnBinding binding(CDataType_NUMERIC, output_precision, output_scale, buffer.data(), 0, strlen_buffer.data());

  int64_t value_offset = 0;
  odbcabstraction::Diagnostics diagnostics("Foo", "Foo", OdbcVersion::V_3);
  ASSERT_EQ(values.size(),
            accessor.GetColumnarData(&binding, 0, values.size(), value_offset, false, diagnostics, nullptr));

  for (int i = 0; i < values.size(); ++i) {
    ASSERT_EQ(sizeof(NUMERIC_STRUCT), strlen_buffer[i]);

    ASSERT_EQ(output_precision, buffer[i].precision);
    ASSERT_EQ(output_scale, buffer[i].scale);
    ASSERT_STREQ(expected_values_str[i].c_str(), ConvertNumericToString(buffer[i]).c_str());
  }
}

TEST(DecimalArrayFlightSqlAccessor, Test_Decimal128Array_CDataType_NUMERIC_SameScale) {
  const std::vector <std::string> &input_values = {"25.212", "-25.212", "-123456789.123", "123456789.123"};
  const std::vector <std::string> &output_values = input_values; // String values should be the same

  AssertNumericOutput(38, 3, input_values, 38, 3, output_values);
}

TEST(DecimalArrayFlightSqlAccessor, Test_Decimal128Array_CDataType_NUMERIC_IncreasingScale) {
  const std::vector <std::string> &input_values = {"25.212", "-25.212", "-123456789.123", "123456789.123"};
  const std::vector <std::string> &output_values = {"25.2120", "-25.2120", "-123456789.1230", "123456789.1230"};

  AssertNumericOutput(38, 3, input_values, 38, 4, output_values);
}

} // namespace flight_sql
} // namespace driver

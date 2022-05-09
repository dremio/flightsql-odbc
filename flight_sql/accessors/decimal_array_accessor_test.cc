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

#include "arrow/util/decimal.h"
#include "arrow/builder.h"
#include "arrow/testing/builder.h"
#include "decimal_array_accessor.h"
#include "gtest/gtest.h"

namespace driver {
namespace flight_sql {

using namespace arrow;
using namespace odbcabstraction;

namespace {

std::vector<Decimal128> MakeDecimalVector(std::vector<std::string> values,
                                                       int32_t scale) {
  std::vector<arrow::Decimal128> ret;
  for (auto str : values) {
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

std::string ConvertNumericToString(NUMERIC_STRUCT& numeric) {
  auto v = reinterpret_cast<int64_t*>(numeric.val);
  auto decimal = Decimal128(v[1], v[0]);
  const std::string &string = decimal.ToString(numeric.scale);

  return (numeric.sign ? "" : "-") + string;
}

}

TEST(DecimalArrayFlightSqlAccessor, Test_Decimal128Array_CDataType_NUMERIC) {
  // schema for input fields
  constexpr int32_t precision = 38;
  constexpr int32_t scale = 3;
  auto decimal_type = std::make_shared<arrow::Decimal128Type>(precision, scale);

  const std::vector<std::string> &values_str = {"25.212", "-25.212", "-123456789.123", "123456789.123"};
  const std::vector<Decimal128> &values = MakeDecimalVector(values_str, scale);
  const std::vector<bool> &is_valid = {true, true, true, true};

  std::shared_ptr<Array> array;
  ArrayFromVector<Decimal128Type, Decimal128>(decimal_type, is_valid, values, &array);

  DecimalArrayFlightSqlAccessor<Decimal128Array, CDataType_NUMERIC> accessor(array.get());

  std::vector<NUMERIC_STRUCT> buffer(values.size());
  std::vector<ssize_t> strlen_buffer(values.size());

  ColumnBinding binding(CDataType_NUMERIC, precision, scale, buffer.data(), 0, strlen_buffer.data());

  odbcabstraction::Diagnostics diagnostics("Foo", "Foo", OdbcVersion::V_3);
  ASSERT_EQ(values.size(),
            accessor.GetColumnarData(&binding, 0, values.size(), 0, diagnostics));

  for (int i = 0; i < values.size(); ++i) {
    ASSERT_EQ(sizeof(NUMERIC_STRUCT), strlen_buffer[i]);

    std::string actual_str = ConvertNumericToString(buffer[i]);
    ASSERT_EQ(0, values_str[i].compare(actual_str));
  }
}

} // namespace flight_sql
} // namespace driver

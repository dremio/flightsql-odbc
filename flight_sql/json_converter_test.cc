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
// under the License

#include "json_converter.h"

#include "gtest/gtest.h"
#include "arrow/testing/builder.h"
#include <arrow/scalar.h>

namespace driver {
namespace flight_sql {

using namespace arrow;

TEST(ConvertToJson, String) {
  ASSERT_EQ("\"test\"", ConvertToJson(StringScalar("test")));
}

TEST(ConvertToJson, Int8) {
  ASSERT_EQ("127", ConvertToJson(Int8Scalar(127)));
  ASSERT_EQ("-128", ConvertToJson(Int8Scalar(-128)));
}

TEST(ConvertToJson, Int16) {
  ASSERT_EQ("32767", ConvertToJson(Int16Scalar(32767)));
  ASSERT_EQ("-32768", ConvertToJson(Int16Scalar(-32768)));
}

TEST(ConvertToJson, Int32) {
  ASSERT_EQ("2147483647", ConvertToJson(Int32Scalar(2147483647)));
  ASSERT_EQ("-2147483648", ConvertToJson(Int32Scalar(-2147483648)));
}

TEST(ConvertToJson, Int64) {
  ASSERT_EQ("9223372036854775807", ConvertToJson(Int64Scalar(9223372036854775807LL)));
  ASSERT_EQ("-9223372036854775808", ConvertToJson(Int64Scalar(-9223372036854775808ULL)));
}

TEST(ConvertToJson, UInt8) {
  ASSERT_EQ("127", ConvertToJson(UInt8Scalar(127)));
  ASSERT_EQ("255", ConvertToJson(UInt8Scalar(255)));
}

TEST(ConvertToJson, UInt16) {
  ASSERT_EQ("32767", ConvertToJson(UInt16Scalar(32767)));
  ASSERT_EQ("65535", ConvertToJson(UInt16Scalar(65535)));
}

TEST(ConvertToJson, UInt32) {
  ASSERT_EQ("2147483647", ConvertToJson(UInt32Scalar(2147483647)));
  ASSERT_EQ("4294967295", ConvertToJson(UInt32Scalar(4294967295)));
}

TEST(ConvertToJson, UInt64) {
  ASSERT_EQ("9223372036854775807", ConvertToJson(UInt64Scalar(9223372036854775807LL)));
  ASSERT_EQ("18446744073709551615", ConvertToJson(UInt64Scalar(18446744073709551615ULL)));
}

TEST(ConvertToJson, Float) {
  ASSERT_EQ("1.5", ConvertToJson(FloatScalar(1.5)));
  ASSERT_EQ("-1.5", ConvertToJson(FloatScalar(-1.5)));
}

TEST(ConvertToJson, Double) {
  ASSERT_EQ("1.5", ConvertToJson(DoubleScalar(1.5)));
  ASSERT_EQ("-1.5", ConvertToJson(DoubleScalar(-1.5)));
}

TEST(ConvertToJson, Boolean) {
  ASSERT_EQ("true", ConvertToJson(BooleanScalar(true)));
}

TEST(ConvertToJson, List) {
  std::vector<std::string> values = {"ABC", "DEF", "XYZ"};
  std::shared_ptr<Array> array;
  ArrayFromVector<StringType, std::string>(values, &array);
  ListScalar scalar(array);
  ASSERT_EQ("[\"ABC\",\"DEF\",\"XYZ\"]", ConvertToJson(scalar));
}

TEST(ConvertToJson, Struct) {
  auto i32 = MakeScalar(1);
  auto f64 = MakeScalar(2.5);
  auto str = MakeScalar("yo");
  ASSERT_OK_AND_ASSIGN(auto scalar, StructScalar::Make({i32, f64, str}, {"i", "f", "s"}));
  ASSERT_EQ("{\"i\":1,\"f\":2.5,\"s\":\"yo\"}", ConvertToJson(*scalar));
}

} // namespace flight_sql
} // namespace driver

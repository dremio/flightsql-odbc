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

#include "utils.h"
#include "arrow/testing/builder.h"
#include <odbcabstraction/platform.h> 
#include "gtest/gtest.h"

namespace driver {
namespace flight_sql {

void TestArrayConversion(const std::vector<std::string>& input,
                         odbcabstraction::CDataType c_type,
                         arrow::Type::type arrow_type) {
  std::shared_ptr<arrow::Array> array;
  arrow::ArrayFromVector<arrow::StringType, std::string>(input, &array);

  auto converter = GetConverter(array->type_id(),
                                c_type);
  auto converted_array = converter(array);

  ASSERT_EQ(converted_array->type_id(), arrow_type);
  ASSERT_EQ(converted_array->length(), input.size());
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

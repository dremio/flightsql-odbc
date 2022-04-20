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

TEST(ConvertToJson, Basic) {
  ASSERT_EQ("\"test\"", ConvertToJson(StringScalar("test")));
  ASSERT_EQ("123", ConvertToJson(Int32Scalar(123)));
  ASSERT_EQ("12.34", ConvertToJson(DoubleScalar(12.34)));
  ASSERT_EQ("true", ConvertToJson(BooleanScalar(true)));
}

TEST(ConvertToJson, Lists) {
  std::vector<std::string> values = {"ABC", "DEF", "XYZ"};
  std::shared_ptr<Array> array;
  ArrayFromVector<StringType, std::string>(values, &array);
  arrow::ListScalar scalar(array);
  ASSERT_EQ("[]", ConvertToJson(scalar));
}

} // namespace flight_sql
} // namespace driver

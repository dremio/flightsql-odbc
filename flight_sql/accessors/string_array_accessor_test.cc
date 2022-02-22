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

#include "arrow/testing/gtest_util.h"
#include "string_array_accessor.h"
#include "gtest/gtest.h"

namespace driver {
namespace flight_sql {

using namespace arrow;
using namespace odbcabstraction;

TEST(StringArrayAccessor, Test_CDataType_CHAR_Basic) {
  std::vector<std::string> values = {"foo", "barx", "baz123"};
  std::shared_ptr<Array> array;
  ArrayFromVector<StringType, std::string>(values, &array);

  FlightSqlAccessor<StringArray, CDataType_CHAR> accessor;
  ASSERT_EQ(CDataType_CHAR, accessor.GetTargetType());

  size_t max_strlen = 64;
  char buffer[values.size() * max_strlen];
  ssize_t strlen_buffer[values.size()];

  ColumnBinding binding(1, CDataType_CHAR, 0, 0, buffer, max_strlen,
                        strlen_buffer);

  ASSERT_EQ(values.size(), accessor.GetColumnarData(array, &binding, 0));

  for (int i = 0; i < values.size(); ++i) {
    ASSERT_EQ(values[i].length() + 1, strlen_buffer[i]);
    ASSERT_EQ(values[i], std::string(buffer + i * max_strlen));
  }
}

TEST(StringArrayAccessor, Test_CDataType_CHAR_Truncation) {
  std::vector<std::string> values = {
      "ABCDEFABCDEFABCDEFABCDEFABCDEFABCDEFABCDEF"};
  std::shared_ptr<Array> array;
  ArrayFromVector<StringType, std::string>(values, &array);

  FlightSqlAccessor<StringArray, CDataType_CHAR> accessor;
  ASSERT_EQ(CDataType_CHAR, accessor.GetTargetType());

  size_t max_strlen = 8;
  char buffer[values.size() * max_strlen];
  ssize_t strlen_buffer[values.size()];

  ColumnBinding binding(1, CDataType_CHAR, 0, 0, buffer, max_strlen,
                        strlen_buffer);

  std::stringstream ss;
  int64_t value_offset = 0;

  // Construct the whole string by concatenating smaller chunks from
  // GetColumnarData
  do {
    ASSERT_EQ(1, accessor.GetColumnarData(array, &binding, value_offset));
    ASSERT_EQ(values[0].length() + 1, strlen_buffer[0]);

    int64_t chunk_length = std::min(static_cast<int64_t>(max_strlen),
                                    strlen_buffer[0] - 1 - value_offset);
    ss << std::string(buffer, buffer + chunk_length);
    value_offset += chunk_length;
  } while (value_offset < strlen_buffer[0] - 1);

  ASSERT_EQ(values[0], ss.str());
}

TEST(StringArrayAccessor, Test_CDataType_BIT) {
  std::vector<std::string> values = {"1", "0"};
  std::shared_ptr<Array> array;
  ArrayFromVector<StringType, std::string>(values, &array);

  FlightSqlAccessor<StringArray, CDataType_BIT> accessor;
  ASSERT_EQ(CDataType_BIT, accessor.GetTargetType());

  unsigned char buffer[values.size()];
  ssize_t strlen_buffer[values.size()];

  ColumnBinding binding(1, CDataType_BIT, 0, 0, buffer, 0, strlen_buffer);

  ASSERT_EQ(2, accessor.GetColumnarData(array, &binding, 0));

  ASSERT_EQ(1, strlen_buffer[0]);
  ASSERT_EQ(1, buffer[0]);
  ASSERT_EQ(1, strlen_buffer[1]);
  ASSERT_EQ(0, buffer[1]);
}

TEST(StringArrayAccessor, Test_CDataType_SBIGINT) {
  std::vector<std::string> values = {"9223372036854775807",
                                     "-9223372036854775808"};
  std::shared_ptr<Array> array;
  ArrayFromVector<StringType, std::string>(values, &array);

  FlightSqlAccessor<StringArray, CDataType_SBIGINT> accessor;
  ASSERT_EQ(CDataType_SBIGINT, accessor.GetTargetType());

  int64_t buffer[values.size()];
  ssize_t strlen_buffer[values.size()];

  ColumnBinding binding(1, CDataType_SBIGINT, 0, 0, buffer, 0, strlen_buffer);

  ASSERT_EQ(2, accessor.GetColumnarData(array, &binding, 0));

  ASSERT_EQ(8, strlen_buffer[0]);
  ASSERT_EQ(9223372036854775807, buffer[0]);
  ASSERT_EQ(8, strlen_buffer[1]);
  ASSERT_EQ(-9223372036854775808U, buffer[1]);
}

TEST(StringArrayAccessor, Test_CDataType_UBIGINT) {
  std::vector<std::string> values = {"123", "123"};
  std::shared_ptr<Array> array;
  ArrayFromVector<StringType, std::string>(values, &array);

  FlightSqlAccessor<StringArray, CDataType_UBIGINT> accessor;
  ASSERT_EQ(CDataType_UBIGINT, accessor.GetTargetType());

  uint64_t buffer[values.size()];
  ssize_t strlen_buffer[values.size()];

  ColumnBinding binding(1, CDataType_UBIGINT, 0, 0, buffer, 0, strlen_buffer);

  ASSERT_EQ(2, accessor.GetColumnarData(array, &binding, 0));

  ASSERT_EQ(8, strlen_buffer[0]);
  ASSERT_EQ(123, buffer[0]);
  ASSERT_EQ(8, strlen_buffer[1]);
  ASSERT_EQ(18446744073709551615U, buffer[1]);
}

} // namespace flight_sql
} // namespace driver

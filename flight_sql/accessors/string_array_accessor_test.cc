/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include "arrow/testing/builder.h"
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

  StringArrayFlightSqlAccessor<CDataType_CHAR> accessor(array.get());

  size_t max_strlen = 64;
  std::vector<char> buffer(values.size() * max_strlen);
  std::vector<ssize_t> strlen_buffer(values.size());

  ColumnBinding binding(CDataType_CHAR, 0, 0, buffer.data(), max_strlen,
                        strlen_buffer.data());

  int64_t value_offset = 0;
  odbcabstraction::Diagnostics diagnostics("Foo", "Foo", OdbcVersion::V_3);
  ASSERT_EQ(values.size(),
            accessor.GetColumnarData(&binding, 0, values.size(), value_offset, false, diagnostics, nullptr));

  for (int i = 0; i < values.size(); ++i) {
    ASSERT_EQ(values[i].length(), strlen_buffer[i]);
    ASSERT_EQ(values[i], std::string(buffer.data() + i * max_strlen));
  }
}

TEST(StringArrayAccessor, Test_CDataType_CHAR_Truncation) {
  std::vector<std::string> values = {
      "ABCDEFABCDEFABCDEFABCDEFABCDEFABCDEFABCDEF"};
  std::shared_ptr<Array> array;
  ArrayFromVector<StringType, std::string>(values, &array);

  StringArrayFlightSqlAccessor<CDataType_CHAR> accessor(array.get());

  size_t max_strlen = 8;
  std::vector<char> buffer(values.size() * max_strlen);
  std::vector<ssize_t> strlen_buffer(values.size());

  ColumnBinding binding(CDataType_CHAR, 0, 0, buffer.data(), max_strlen,
                        strlen_buffer.data());

  std::stringstream ss;
  int64_t value_offset = 0;

  // Construct the whole string by concatenating smaller chunks from
  // GetColumnarData
  odbcabstraction::Diagnostics diagnostics("Foo", "Foo", OdbcVersion::V_3);
  do {
    diagnostics.Clear();
    int64_t original_value_offset = value_offset;
    ASSERT_EQ(1, accessor.GetColumnarData(&binding, 0, 1, value_offset, true, diagnostics, nullptr));
    ASSERT_EQ(values[0].length() - original_value_offset, strlen_buffer[0]);

    ss << buffer.data();
  } while (value_offset < values[0].length() && value_offset != -1);

  ASSERT_EQ(values[0], ss.str());
}

TEST(StringArrayAccessor, Test_CDataType_WCHAR_Basic) {
  std::vector<std::string> values = {"foo", "barx", "baz123"};
  std::shared_ptr<Array> array;
  ArrayFromVector<StringType, std::string>(values, &array);

  StringArrayFlightSqlAccessor<CDataType_WCHAR> accessor(array.get());

  size_t max_strlen = 64;
  std::vector<SqlWChar> buffer(values.size() * max_strlen);
  std::vector<ssize_t> strlen_buffer(values.size());

  ColumnBinding binding(CDataType_WCHAR, 0, 0, buffer.data(), max_strlen,
                        strlen_buffer.data());

  int64_t value_offset = 0;
  odbcabstraction::Diagnostics diagnostics("Foo", "Foo", OdbcVersion::V_3);
  ASSERT_EQ(values.size(),
            accessor.GetColumnarData(&binding, 0, values.size(), value_offset, false, diagnostics, nullptr));

  for (int i = 0; i < values.size(); ++i) {
    ASSERT_EQ(values[i].length() * sizeof(SqlWChar), strlen_buffer[i]);
    auto expected = CharToWStrConverter().from_bytes(values[i].c_str());
    auto actual = SqlWString(buffer.data() + i * max_strlen / sizeof(SqlWChar));
    ASSERT_EQ(0, expected.compare(actual));
  }
}

TEST(StringArrayAccessor, Test_CDataType_WCHAR_Truncation) {
  std::vector<std::string> values = {
      "ABCDEFABCDEFABCDEFABCDEFABCDEFABCDEFABCDEF"};
  std::shared_ptr<Array> array;
  ArrayFromVector<StringType, std::string>(values, &array);

  StringArrayFlightSqlAccessor<CDataType_WCHAR> accessor(array.get());

  size_t max_strlen = 8;
  std::vector<SqlWChar> buffer(values.size() * max_strlen);
  std::vector<ssize_t> strlen_buffer(values.size());

  ColumnBinding binding(CDataType_WCHAR, 0, 0, buffer.data(),
                        max_strlen * sizeof(SqlWChar), strlen_buffer.data());

  std::basic_stringstream<SqlWChar> ss;
  int64_t value_offset = 0;

  // Construct the whole string by concatenating smaller chunks from
  // GetColumnarData
  std::basic_string<SqlWChar> finalStr;
  driver::odbcabstraction::Diagnostics diagnostics("Dummy", "Dummy", odbcabstraction::V_3);
  do {
    int64_t original_value_offset = value_offset;
    ASSERT_EQ(1, accessor.GetColumnarData(&binding, 0, 1, value_offset, true, diagnostics, nullptr));
    ASSERT_EQ(values[0].length() * sizeof(SqlWChar) - original_value_offset, strlen_buffer[0]);

    finalStr += std::basic_string<SqlWChar>(buffer.data());
  } while (value_offset < values[0].length() * sizeof(SqlWChar) && value_offset != -1);

  auto expected = CharToWStrConverter().from_bytes(values[0].c_str());
  auto actual = finalStr;
  ASSERT_EQ(0, expected.compare(actual));
}

} // namespace flight_sql
} // namespace driver

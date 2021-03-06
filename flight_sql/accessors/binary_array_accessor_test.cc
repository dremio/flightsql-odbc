/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include "arrow/testing/gtest_util.h"
#include "arrow/testing/builder.h"
#include "binary_array_accessor.h"
#include "gtest/gtest.h"

namespace driver {
namespace flight_sql {

using namespace arrow;
using namespace odbcabstraction;

TEST(BinaryArrayAccessor, Test_CDataType_BINARY_Basic) {
  std::vector<std::string> values = {"foo", "barx", "baz123"};
  std::shared_ptr<Array> array;
  ArrayFromVector<BinaryType, std::string>(values, &array);

  BinaryArrayFlightSqlAccessor<CDataType_BINARY> accessor(array.get());

  size_t max_strlen = 64;
  std::vector<char> buffer(values.size() * max_strlen);
  std::vector<ssize_t> strlen_buffer(values.size());

  ColumnBinding binding(CDataType_BINARY, 0, 0, buffer.data(), max_strlen,
                        strlen_buffer.data());

  int64_t value_offset = 0;
  odbcabstraction::Diagnostics diagnostics("Foo", "Foo", OdbcVersion::V_3);
  ASSERT_EQ(values.size(),
            accessor.GetColumnarData(&binding, 0, values.size(), value_offset, false, diagnostics, nullptr));

  for (int i = 0; i < values.size(); ++i) {
    ASSERT_EQ(values[i].length(), strlen_buffer[i]);
    // Beware that CDataType_BINARY values are not null terminated.
    // It's safe to create a std::string from this data because we know it's
    // ASCII, this doesn't work with arbitrary binary data.
    ASSERT_EQ(values[i],
              std::string(buffer.data() + i * max_strlen,
                          buffer.data() + i * max_strlen + strlen_buffer[i]));
  }
}

TEST(BinaryArrayAccessor, Test_CDataType_BINARY_Truncation) {
  std::vector<std::string> values = {
      "ABCDEFABCDEFABCDEFABCDEFABCDEFABCDEFABCDEF"};
  std::shared_ptr<Array> array;
  ArrayFromVector<BinaryType, std::string>(values, &array);

  BinaryArrayFlightSqlAccessor<CDataType_BINARY> accessor(array.get());

  size_t max_strlen = 8;
  std::vector<char> buffer(values.size() * max_strlen);
  std::vector<ssize_t> strlen_buffer(values.size());

  ColumnBinding binding(CDataType_BINARY, 0, 0, buffer.data(), max_strlen,
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

    int64_t chunk_length = 0;
    if (value_offset == -1) {
      chunk_length = strlen_buffer[0];
    } else {
      chunk_length = max_strlen;
    }
    
    // Beware that CDataType_BINARY values are not null terminated.
    // It's safe to create a std::string from this data because we know it's
    // ASCII, this doesn't work with arbitrary binary data.
    ss << std::string(buffer.data(), buffer.data() + chunk_length);
  } while (value_offset < values[0].length() && value_offset != -1);

  ASSERT_EQ(values[0], ss.str());
}

} // namespace flight_sql
} // namespace driver

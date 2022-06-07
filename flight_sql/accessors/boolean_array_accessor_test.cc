/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include "arrow/testing/builder.h"
#include "boolean_array_accessor.h"
#include "gtest/gtest.h"

namespace driver {
namespace flight_sql {

using namespace arrow;
using namespace odbcabstraction;

TEST(BooleanArrayFlightSqlAccessor, Test_BooleanArray_CDataType_BIT) {
  const std::vector<bool> values = {true, false, true};
  std::shared_ptr<Array> array;
  ArrayFromVector<BooleanType>(values, &array);

  BooleanArrayFlightSqlAccessor<CDataType_BIT> accessor(array.get());

  std::vector<char> buffer(values.size());
  std::vector<ssize_t> strlen_buffer(values.size());

  ColumnBinding binding(CDataType_BIT, 0, 0, buffer.data(), 0, strlen_buffer.data());

  int64_t value_offset = 0;
  odbcabstraction::Diagnostics diagnostics("Foo", "Foo", OdbcVersion::V_3);
  ASSERT_EQ(values.size(),
            accessor.GetColumnarData(&binding, 0, values.size(), value_offset, false, diagnostics, nullptr));

  for (int i = 0; i < values.size(); ++i) {
    ASSERT_EQ(sizeof(unsigned char), strlen_buffer[i]);
    ASSERT_EQ(values[i] ? 1 : 0, buffer[i]);
  }
}

} // namespace flight_sql
} // namespace driver

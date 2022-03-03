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
#include "boolean_array_accessor.h"
#include "gtest/gtest.h"

namespace driver {
namespace flight_sql {

using namespace arrow;
using namespace odbcabstraction;

TEST(BooleanArrayFlightSqlAccessor, Test_BooleanArray_CDataType_BIT) {
  std::vector<bool> values = {true, false, true};
  std::shared_ptr<Array> array;
  ArrayFromVector<BooleanType>(values, &array);

  BooleanArrayFlightSqlAccessor<CDataType_BIT> accessor(array.get());

  char buffer[values.size()];
  ssize_t strlen_buffer[values.size()];

  ColumnBinding binding(CDataType_BIT, 0, 0, buffer, 0, strlen_buffer);

  ASSERT_EQ(values.size(),
            accessor.GetColumnarData(&binding, 0, values.size(), 0));

  for (int i = 0; i < values.size(); ++i) {
    ASSERT_EQ(sizeof(unsigned char), strlen_buffer[i]);
    ASSERT_EQ(values[i] ? '1' : '0', buffer[i]);
  }
}

} // namespace flight_sql
} // namespace driver

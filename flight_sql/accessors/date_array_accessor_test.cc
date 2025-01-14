/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include "arrow/testing/builder.h"
#include "boolean_array_accessor.h"
#include "date_array_accessor.h"
#include "gtest/gtest.h"
#include "odbcabstraction/calendar_utils.h"

namespace driver {
namespace flight_sql {

using namespace arrow;
using namespace odbcabstraction;

TEST(DateArrayAccessor, Test_Date32Array_CDataType_DATE) {
  std::vector<int32_t> values = {7589, 12320, 18980, 19095, -1, 0};
  std::vector<DATE_STRUCT> expected = {
    {1990, 10, 12},
    {2003,  9, 25},
    {2021, 12, 19},
    {2022,  4, 13},
    {1969, 12, 31},
    {1970,  1,  1},
  };

  std::shared_ptr<Array> array;
  ArrayFromVector<Date32Type, int32_t>(values, &array);

  DateArrayFlightSqlAccessor<CDataType_DATE, Date32Array> accessor(
      dynamic_cast<NumericArray<Date32Type> *>(array.get()));

  std::vector<DATE_STRUCT> buffer(values.size());
  std::vector<ssize_t> strlen_buffer(values.size());

  ColumnBinding binding(CDataType_DATE, 0, 0, buffer.data(), 0, strlen_buffer.data());

  int64_t value_offset = 0;
  odbcabstraction::Diagnostics diagnostics("Foo", "Foo", OdbcVersion::V_3);
  ASSERT_EQ(values.size(),
          accessor.GetColumnarData(&binding, 0, values.size(), value_offset, false, diagnostics, nullptr));

  for (size_t i = 0; i < values.size(); ++i) {
    ASSERT_EQ(sizeof(DATE_STRUCT), strlen_buffer[i]);

    ASSERT_EQ(expected[i].year, buffer[i].year);
    ASSERT_EQ(expected[i].month, buffer[i].month);
    ASSERT_EQ(expected[i].day, buffer[i].day);
  }
}

TEST(DateArrayAccessor, Test_Date64Array_CDataType_DATE) {
  std::vector<int64_t> values = {86400000,  172800000, 259200000, 1649793238110, 0, 
                                 345600000, 432000000, 518400000, -86400000, -17987443200000};
  std::vector<DATE_STRUCT> expected = {
    /* year(16), month(u16), day(u16) */
    {1970,  1,  2},
    {1970,  1,  3},
    {1970,  1,  4},
    {2022,  4, 12},
    {1970,  1,  1},
    {1970,  1,  5},
    {1970,  1,  6},
    {1970,  1,  7},
    {1969, 12, 31},
    // This is the documented lower limit of supported Gregorian dates for some parts of Boost,
    // however boost::posix_time may go lower?
    {1400,  1,  1},
  };

  std::shared_ptr<Array> array;
  ArrayFromVector<Date64Type, int64_t>(values, &array);

  DateArrayFlightSqlAccessor<CDataType_DATE, Date64Array> accessor(
      dynamic_cast<NumericArray<Date64Type> *>(array.get()));

  std::vector<DATE_STRUCT> buffer(values.size());
  std::vector<ssize_t> strlen_buffer(values.size());

  ColumnBinding binding(CDataType_DATE, 0, 0, buffer.data(), 0, strlen_buffer.data());

  int64_t value_offset = 0;
  odbcabstraction::Diagnostics diagnostics("Foo", "Foo", OdbcVersion::V_3);
  ASSERT_EQ(values.size(),
          accessor.GetColumnarData(&binding, 0, values.size(), value_offset, false, diagnostics, nullptr));

  for (size_t i = 0; i < values.size(); ++i) {
    ASSERT_EQ(sizeof(DATE_STRUCT), strlen_buffer[i]);
    tm date{};

    ASSERT_EQ(expected[i].year, buffer[i].year);
    ASSERT_EQ(expected[i].month, buffer[i].month);
    ASSERT_EQ(expected[i].day, buffer[i].day);
  }
}

} // namespace flight_sql
} // namespace driver

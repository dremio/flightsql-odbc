/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include "flight_sql_statement_get_tables.h"
#include <odbcabstraction/platform.h>
#include "gtest/gtest.h"

namespace driver {
namespace flight_sql {

void AssertParseTest(const std::string &input_string,
                     const std::vector<std::string> &assert_vector) {
  std::vector<std::string> table_types;

  ParseTableTypes(input_string, table_types);
  ASSERT_EQ(table_types, assert_vector);
}

TEST(TableTypeParser, ParsingWithoutSingleQuotesWithLeadingWhiteSpace) {
  AssertParseTest("TABLE, VIEW", {"TABLE", "VIEW"});
}

TEST(TableTypeParser, ParsingWithoutSingleQuotesWithoutLeadingWhiteSpace) {
  AssertParseTest("TABLE,VIEW", {"TABLE", "VIEW"});
}

TEST(TableTypeParser, ParsingWithSingleQuotesWithLeadingWhiteSpace) {
  AssertParseTest("'TABLE', 'VIEW'", {"TABLE", "VIEW"});
}

TEST(TableTypeParser, ParsingWithSingleQuotesWithoutLeadingWhiteSpace) {
  AssertParseTest("'TABLE','VIEW'", {"TABLE", "VIEW"});
}

TEST(TableTypeParser, ParsingWithCommaInsideSingleQuotes) {
  AssertParseTest("'TABLE, TEST', 'VIEW, TEMPORARY'",
                  {"TABLE, TEST", "VIEW, TEMPORARY"});
}
} // namespace flight_sql
} // namespace driver

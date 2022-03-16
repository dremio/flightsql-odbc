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
#include "gtest/gtest.h"

namespace driver {
namespace flight_sql {

using boost::xpressive::regex_match;

TEST(Utils, CreateRegexFromSqlPattern) {
  // Test with no wildcards
  ASSERT_TRUE(
      regex_match(std::string("TEST1"), CreateRegexFromSqlPattern("TEST1")));
  ASSERT_FALSE(
      regex_match(std::string("TEST1"), CreateRegexFromSqlPattern("TEST2")));

  // Test _ wildcard
  ASSERT_TRUE(
      regex_match(std::string("XaY"), CreateRegexFromSqlPattern("X_Y")));
  ASSERT_TRUE(
      regex_match(std::string("XbY"), CreateRegexFromSqlPattern("X_Y")));
  ASSERT_FALSE(
      regex_match(std::string("XY"), CreateRegexFromSqlPattern("X_Y")));
  ASSERT_FALSE(
      regex_match(std::string("XabY"), CreateRegexFromSqlPattern("X_Y")));

  // Test % wildcard
  ASSERT_TRUE(regex_match(std::string("XY"), CreateRegexFromSqlPattern("X%Y")));
  ASSERT_TRUE(
      regex_match(std::string("XaY"), CreateRegexFromSqlPattern("X%Y")));
  ASSERT_TRUE(
      regex_match(std::string("XabY"), CreateRegexFromSqlPattern("X%Y")));
  ASSERT_FALSE(
      regex_match(std::string("Xab"), CreateRegexFromSqlPattern("X%Y")));
}

} // namespace flight_sql
} // namespace driver

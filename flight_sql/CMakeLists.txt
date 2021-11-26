# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

cmake_minimum_required(VERSION 3.11)
set(CMAKE_CXX_STANDARD 11)

include_directories(${CMAKE_SOURCE_DIR}/spi)

SET(Arrow_STATIC ON)
find_package(Arrow REQUIRED)
find_package(ArrowFlight REQUIRED PATHS /usr/local/lib/cmake/arrow/)
find_package(ArrowFlightSql REQUIRED PATHS /usr/local/lib/cmake/arrow/)
find_package(GTest REQUIRED)

enable_testing()

add_library(flight_odbc_driver flight_sql_driver.cc flight_sql_connection.cc flight_sql_auth_method.cc)
target_link_libraries(flight_odbc_driver spi arrow_static arrow_flight arrow_flight_sql)
target_include_directories(flight_odbc_driver PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

# CLI
add_executable(flight_odbc_driver_cli main.cc)
target_link_libraries(flight_odbc_driver_cli flight_odbc_driver)

# Unit tests
add_executable(flight_odbc_driver_test flight_sql_connection_test.cc)
target_link_libraries(flight_odbc_driver_test flight_odbc_driver GTest::gtest GTest::gtest_main)
add_test(connection_test flight_odbc_driver_test)

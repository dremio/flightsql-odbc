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

include_directories(
    include
    ${CMAKE_SOURCE_DIR}/odbcabstraction/include)

SET(Arrow_STATIC ON)

# Get Arrow using git.
include(ExternalProject)

set(ARROW_CMAKE_ARGS
  -DARROW_FLIGHT=ON
  -DARROW_FLIGHT_SQL=ON
  -DARROW_IPC=ON
  -DARROW_BUILD_SHARED=OFF
  -DARROW_BUILD_STATIC=ON
  -DARROW_COMPUTE=ON
  -DARROW_WITH_UTF8PROC=OFF
  -DARROW_BUILD_TESTS=OFF
  -DARROW_DEPENDENCY_USE_SHARED=OFF
  -DCMAKE_DEPENDS_USE_COMPILER=FALSE
  -DCMAKE_INSTALL_PREFIX=${CMAKE_CURRENT_BINARY_DIR}/ApacheArrow-prefix/src/ApacheArrow-install
  ${CMAKE_CURRENT_BINARY_DIR}/ApacheArrow-prefix/src/ApacheArrow/cpp
)

if(NOT ARROW_GIT_REPOSITORY)
  set(ARROW_GIT_REPOSITORY https://github.com/apache/arrow.git)
endif()
if(NOT ARROW_GIT_TAG)
  set(ARROW_GIT_TAG master)
endif()

message("Using Arrow from ${ARROW_GIT_REPOSITORY} on tag ${ARROW_GIT_TAG}")
ExternalProject_Add(ApacheArrow
  GIT_REPOSITORY ${ARROW_GIT_REPOSITORY}
  GIT_TAG ${ARROW_GIT_TAG}
  CMAKE_ARGS ${ARROW_CMAKE_ARGS})

include_directories(${CMAKE_CURRENT_BINARY_DIR}/ApacheArrow-prefix/src/ApacheArrow-install/include)
link_directories(${CMAKE_CURRENT_BINARY_DIR}/ApacheArrow-prefix/src/ApacheArrow-install/lib)

# Add Zlib dependencies needed by Arrow Flight. Should be pre-installed.
find_package(ZLIB REQUIRED)

# Add Protobuf dependencies needed by Arrow Flight. Should be pre-installed.
set(Protobuf_USE_STATIC_LIBS ON)
find_package(Protobuf REQUIRED)

# Add OpenSSL dependencies needed by Arrow Flight. Should be pre-installed.
# May need to set OPENSSL_ROOT_DIR first. On Mac if using brew:
# brew install openssl@1.1
# add to the cmake line -DOPENSSL_ROOT_DIR=/usr/local/Cellar/openssl@1.1/1.1.1m
if (NOT DEFINED OPENSSL_ROOT_DIR AND DEFINED APPLE)
  set(OPENSSL_ROOT_DIR /usr/local/Cellar/openssl@1.1/1.1.1m)
endif()
# This is based on Arrow's FindOpenSSL module. It's not clear if both variables
# need to be set.
set(OpenSSL_USE_STATIC_LIBS ON)
set(OPENSSL_USE_STATIC_LIBS ON)
find_package(OpenSSL REQUIRED)

# Add gRPC dependencies needed by Arrow Flight. Should be pre-installed.
find_package(gRPC 1.36 CONFIG REQUIRED)

if (APPLE)
  add_compile_definitions(WITH_IODBC)
endif()

enable_testing()

set(ARROW_ODBC_SPI_SOURCES
  accessors/binary_array_accessor.cc
  accessors/boolean_array_accessor.cc
  accessors/primitive_array_accessor.cc
  accessors/string_array_accessor.cc
  flight_sql_auth_method.cc
  flight_sql_connection.cc
  flight_sql_driver.cc
  flight_sql_result_set.cc
  flight_sql_result_set_accessors.cc
  flight_sql_result_set_column.cc
  flight_sql_result_set_metadata.cc
  flight_sql_statement.cc
  flight_sql_statement_get_tables.cc
  flight_sql_stream_chunk_iterator.cc
  get_info_cache.cc
  record_batch_transformer.cc
  scalar_function_reporter.cc
)

set(ARROW_ODBC_SPI_THIRDPARTY_LIBS
  arrow_flight_sql
  arrow_flight
  arrow
  arrow_bundled_dependencies
  ${ZLIB_LIBRARIES}
  ${Protobuf_LIBRARIES}
  gRPC::grpc++
)

add_library(arrow_odbc_spi_impl ${ARROW_ODBC_SPI_SOURCES})
add_dependencies(arrow_odbc_spi_impl ApacheArrow)

set_target_properties(arrow_odbc_spi_impl
  PROPERTIES
  ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/$<CONFIG>/lib
  LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/$<CONFIG>/lib
  RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/$<CONFIG>/lib
)

target_link_libraries(
    arrow_odbc_spi_impl 
    odbcabstraction 
    ${ARROW_ODBC_SPI_THIRDPARTY_LIBS})
target_include_directories(arrow_odbc_spi_impl PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

# CLI
add_executable(arrow_odbc_spi_impl_cli main.cc)
set_target_properties(arrow_odbc_spi_impl_cli
  PROPERTIES
  RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/$<CONFIG>/bin
)
target_link_libraries(arrow_odbc_spi_impl_cli arrow_odbc_spi_impl)

# Unit tests
set(ARROW_ODBC_SPI_TEST_SOURCES
  accessors/boolean_array_accessor_test.cc
  accessors/binary_array_accessor_test.cc
  accessors/primitive_array_accessor_test.cc
  accessors/string_array_accessor_test.cc
  flight_sql_connection_test.cc
  parse_table_types_test.cc
  record_batch_transformer_test.cc
)

add_executable(arrow_odbc_spi_impl_test ${ARROW_ODBC_SPI_TEST_SOURCES})
add_dependencies(arrow_odbc_spi_impl_test ApacheArrow)
set_target_properties(arrow_odbc_spi_impl_test
  PROPERTIES
  RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/test/$<CONFIG>/bin
)
target_link_libraries(arrow_odbc_spi_impl_test
        arrow_odbc_spi_impl
        gtest gtest_main)
add_test(connection_test arrow_odbc_spi_impl_test)
add_test(transformer_test arrow_odbc_spi_impl_test)

# Copyright (C) 2020-2022 Dremio Corporation
#
# See "LICENSE" for license information.
#

cmake_minimum_required(VERSION 3.11)
set(CMAKE_CXX_STANDARD 11)

set(CMAKE_POSITION_INDEPENDENT_CODE ON)

include_directories(
    include
    include/flight_sql
    ${CMAKE_SOURCE_DIR}/odbcabstraction/include)

if (DEFINED CMAKE_TOOLCHAIN_FILE)
  include(${CMAKE_TOOLCHAIN_FILE})
endif()

# Add Zlib dependencies needed by Arrow Flight. Should be pre-installed unless provided by VCPKG.
find_package(ZLIB REQUIRED)

# Add Protobuf dependencies needed by Arrow Flight. Should be pre-installed.
set(Protobuf_USE_STATIC_LIBS ON)
find_package(Protobuf REQUIRED)

# Add OpenSSL dependencies needed by Arrow Flight. Should be pre-installed.
# May need to set OPENSSL_ROOT_DIR first. On Mac if using brew:
# brew install openssl@1.1
# add to the cmake line -DOPENSSL_ROOT_DIR=/usr/local/Cellar/openssl@1.1/1.1.1m
if (NOT DEFINED OPENSSL_ROOT_DIR AND DEFINED APPLE AND NOT DEFINED CMAKE_TOOLCHAIN_FILE)
  set(OPENSSL_ROOT_DIR /usr/local/Cellar/openssl@1.1/1.1.1m)
endif()
# This is based on Arrow's FindOpenSSL module. It's not clear if both variables
# need to be set.
if (NOT DEFINED MSVC)
  set(OpenSSL_USE_STATIC_LIBS ON)
  set(OPENSSL_USE_STATIC_LIBS ON)
endif()
find_package(OpenSSL REQUIRED)

# OpenSSL depends on krb5 on CentOS
if (UNIX)
  list(APPEND OPENSSL_LIBRARIES krb5 k5crypto)
endif()

# Add gRPC dependencies needed by Arrow Flight. Should be pre-installed.
find_package(gRPC 1.36 CONFIG REQUIRED)

find_package(RapidJSON CONFIG REQUIRED)

SET(Arrow_STATIC ON)

# Get Arrow using git.
include(ExternalProject)

if (MSVC)
  set(ARROW_CMAKE_ARGS
    -DARROW_FLIGHT=ON
    -DARROW_FLIGHT_SQL=ON
    -DARROW_COMPUTE=ON
    -DARROW_IPC=ON
    -DARROW_BUILD_SHARED=OFF
    -DARROW_BUILD_STATIC=ON
    -DARROW_WITH_UTF8PROC=OFF
    -DARROW_BUILD_TESTS=OFF
    -DARROW_DEPENDENCY_SOURCE=VCPKG
    -DVCPKG_TARGET_TRIPLET=${VCPKG_TARGET_TRIPLET}
    -DVCPKG_MANIFEST_MODE=${VCPKG_MANIFEST_MODE}
    -DCMAKE_DEPENDS_USE_COMPILER=FALSE
    -DCMAKE_INSTALL_PREFIX=${CMAKE_CURRENT_BINARY_DIR}/ApacheArrow-prefix/src/ApacheArrow-install
    -DCMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE}
    ${CMAKE_CURRENT_BINARY_DIR}/ApacheArrow-prefix/src/ApacheArrow/cpp
  )
elseif(APPLE)
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
          -DARROW_DEPENDENCY_USE_STATIC=ON
          -DCMAKE_DEPENDS_USE_COMPILER=FALSE
          -DVCPKG_TARGET_TRIPLET=${VCPKG_TARGET_TRIPLET}
          -DVCPKG_MANIFEST_MODE=OFF
          -DCMAKE_DEPENDS_USE_COMPILER=FALSE
          -DCMAKE_INSTALL_PREFIX=${CMAKE_CURRENT_BINARY_DIR}/ApacheArrow-prefix/src/ApacheArrow-install
          -DCMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE}
          ${CMAKE_CURRENT_BINARY_DIR}/ApacheArrow-prefix/src/ApacheArrow/cpp
          )
  if (DEFINED CMAKE_TOOLCHAIN_FILE)
    list(APPEND ARROW_CMAKE_ARGS -DARROW_DEPENDENCY_SOURCE=VCPKG)
  endif()
else()
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
    -DOPENSSL_INCLUDE_DIR=${OPENSSL_INCLUDE_DIR}
    -DCMAKE_INSTALL_PREFIX=${CMAKE_CURRENT_BINARY_DIR}/ApacheArrow-prefix/src/ApacheArrow-install
    ${CMAKE_CURRENT_BINARY_DIR}/ApacheArrow-prefix/src/ApacheArrow/cpp
  )
endif()

set(ARROW_GIT_REPOSITORY "https://github.com/apache/arrow.git" CACHE STRING "Arrow repository path or URL")
set(ARROW_GIT_TAG "b050bd0d31db6412256cec3362c0d57c9732e1f2" CACHE STRING "Tag for the Arrow repository")

message("Using Arrow from ${ARROW_GIT_REPOSITORY} on tag ${ARROW_GIT_TAG}")
ExternalProject_Add(ApacheArrow
  GIT_REPOSITORY ${ARROW_GIT_REPOSITORY}
  GIT_TAG ${ARROW_GIT_TAG}
  CMAKE_ARGS ${ARROW_CMAKE_ARGS})

include_directories(BEFORE ${CMAKE_CURRENT_BINARY_DIR}/ApacheArrow-prefix/src/ApacheArrow-install/include)
IF(${CMAKE_SYSTEM_NAME} MATCHES "Linux")
  set(ARROW_LIB_DIR lib64)
else()
  set(ARROW_LIB_DIR lib)
endif()
link_directories(${CMAKE_CURRENT_BINARY_DIR}/ApacheArrow-prefix/src/ApacheArrow-install/${ARROW_LIB_DIR})

if (MSVC)
  # the following definitions stop arrow from using __declspec when staticly linking and will break on windows without them
  add_compile_definitions(ARROW_STATIC ARROW_FLIGHT_STATIC)
endif()

enable_testing()

set(ARROW_ODBC_SPI_SOURCES
  include/flight_sql/flight_sql_driver.h
  accessors/binary_array_accessor.cc
  accessors/binary_array_accessor.h
  accessors/boolean_array_accessor.cc
  accessors/boolean_array_accessor.h
  accessors/common.h
  accessors/date_array_accessor.cc
  accessors/date_array_accessor.h
  accessors/decimal_array_accessor.cc
  accessors/decimal_array_accessor.h
  accessors/main.h
  accessors/primitive_array_accessor.cc
  accessors/primitive_array_accessor.h
  accessors/string_array_accessor.cc
  accessors/string_array_accessor.h
  accessors/time_array_accessor.cc
  accessors/time_array_accessor.h
  accessors/timestamp_array_accessor.cc
  accessors/timestamp_array_accessor.h
  address_info.cc
  address_info.h
  flight_sql_auth_method.cc
  flight_sql_auth_method.h
  flight_sql_connection.cc
  flight_sql_connection.h
  flight_sql_driver.cc
  flight_sql_get_tables_reader.cc
  flight_sql_get_tables_reader.h
  flight_sql_get_type_info_reader.cc
  flight_sql_get_type_info_reader.h
  flight_sql_result_set.cc
  flight_sql_result_set.h
  flight_sql_result_set_accessors.cc
  flight_sql_result_set_accessors.h
  flight_sql_result_set_column.cc
  flight_sql_result_set_column.h
  flight_sql_result_set_metadata.cc
  flight_sql_result_set_metadata.h
  flight_sql_ssl_config.cc
  flight_sql_ssl_config.h
  flight_sql_statement.cc
  flight_sql_statement.h
  flight_sql_statement_get_columns.cc
  flight_sql_statement_get_columns.h
  flight_sql_statement_get_tables.cc
  flight_sql_statement_get_tables.h
  flight_sql_statement_get_type_info.cc
  flight_sql_statement_get_type_info.h
  flight_sql_stream_chunk_buffer.cc
  flight_sql_stream_chunk_buffer.h
  get_info_cache.cc
  get_info_cache.h
  json_converter.cc
  json_converter.h
  record_batch_transformer.cc
  record_batch_transformer.h
  scalar_function_reporter.cc
  scalar_function_reporter.h
  system_trust_store.cc
  system_trust_store.h
  utils.cc)

if (WIN32)
    include_directories(flight_sql/include)

    list(APPEND ARROW_ODBC_SPI_SOURCES
     include/flight_sql/config/configuration.h
     include/flight_sql/config/connection_string_parser.h
     include/flight_sql/ui/add_property_window.h
     include/flight_sql/ui/custom_window.h
     include/flight_sql/ui/dsn_configuration_window.h
     include/flight_sql/ui/window.h
     config/configuration.cc
     config/connection_string_parser.cc
     ui/custom_window.cc
     ui/window.cc
     ui/dsn_configuration_window.cc
     ui/add_property_window.cc
     system_dsn.cc)
endif()

if (MSVC)
  set(CMAKE_CXX_FLAGS_RELEASE "/MD")
  set(CMAKE_CXX_FLAGS_DEBUG "/MDd")
  set(ARROW_LIBS
    arrow_flight_sql_static
    arrow_flight_static
    arrow_static
  )
else()
  set(ARROW_LIBS
    arrow_flight_sql
    arrow_flight
    arrow
    arrow_bundled_dependencies
  )
endif()

set(ARROW_ODBC_SPI_THIRDPARTY_LIBS
  ${ARROW_LIBS}
  gRPC::grpc++
  ${ZLIB_LIBRARIES}
  ${Protobuf_LIBRARIES}
  ${OPENSSL_LIBRARIES}
  ${RapidJSON_LIBRARIES}
)

if (MSVC)
  find_package(Boost REQUIRED COMPONENTS locale)
  list(APPEND ARROW_ODBC_SPI_THIRDPARTY_LIBS ${Boost_LIBRARIES})
endif()

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
  accessors/date_array_accessor_test.cc
  accessors/decimal_array_accessor_test.cc
  accessors/primitive_array_accessor_test.cc
  accessors/string_array_accessor_test.cc
  accessors/time_array_accessor_test.cc
  accessors/timestamp_array_accessor_test.cc
  flight_sql_connection_test.cc
  parse_table_types_test.cc
  json_converter_test.cc
  record_batch_transformer_test.cc
  utils_test.cc
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

add_custom_command(
     TARGET arrow_odbc_spi_impl_test
     COMMENT "Run tests"
     POST_BUILD 
     COMMAND ${CMAKE_BINARY_DIR}/test/$<CONFIG>/bin/arrow_odbc_spi_impl_test
)

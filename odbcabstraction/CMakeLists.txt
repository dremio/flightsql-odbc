# Copyright (C) 2020-2022 Dremio Corporation
#
# See "LICENSE" for license information.
#

set(CMAKE_POSITION_INDEPENDENT_CODE ON)

include_directories(include)

add_library(odbcabstraction
  include/odbcabstraction/calendar_utils.h
  include/odbcabstraction/diagnostics.h
  include/odbcabstraction/error_codes.h
  include/odbcabstraction/exceptions.h
  include/odbcabstraction/logger.h
  include/odbcabstraction/platform.h
  include/odbcabstraction/spd_logger.h
  include/odbcabstraction/types.h
  include/odbcabstraction/utils.h
  include/odbcabstraction/odbc_impl/AttributeUtils.h
  include/odbcabstraction/odbc_impl/EncodingUtils.h
  include/odbcabstraction/odbc_impl/ODBCConnection.h
  include/odbcabstraction/odbc_impl/ODBCDescriptor.h
  include/odbcabstraction/odbc_impl/ODBCEnvironment.h
  include/odbcabstraction/odbc_impl/ODBCHandle.h
  include/odbcabstraction/odbc_impl/ODBCStatement.h
  include/odbcabstraction/odbc_impl/TypeUtilities.h
  include/odbcabstraction/spi/connection.h
  include/odbcabstraction/spi/driver.h
  include/odbcabstraction/spi/result_set.h
  include/odbcabstraction/spi/result_set_metadata.h
  include/odbcabstraction/spi/statement.h
  calendar_utils.cc
  diagnostics.cc
  encoding.cc
  exceptions.cc
  logger.cc
  spd_logger.cc
  utils.cc
  whereami.h
  whereami.cc
  odbc_impl/ODBCConnection.cc
  odbc_impl/ODBCDescriptor.cc
  odbc_impl/ODBCEnvironment.cc
  odbc_impl/ODBCStatement.cc
)

set_target_properties(odbcabstraction
    PROPERTIES
    ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/$<CONFIG>/lib
    LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/$<CONFIG>/lib
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/$<CONFIG>/lib
    )

add_compile_definitions(FMT_HEADER_ONLY)

include(ExternalProject)

message("Using spdlog from https://github.com/gabime/spdlog on tag v1.10.0")
ExternalProject_Add(
        dep_spdlog
        GIT_REPOSITORY "https://github.com/gabime/spdlog"
        GIT_TAG "v1.10.0"
        GIT_SHALLOW 1
        UPDATE_COMMAND ""
        PATCH_COMMAND ""
        CMAKE_ARGS
        -DCMAKE_INSTALL_PREFIX=${CMAKE_CURRENT_BINARY_DIR}/dep_spdlog-prefix/src/dep_spdlog-install
        -DSPDLOG_BUILD_EXAMPLES=OFF
        -DSPDLOG_BUILD_BENCH=OFF
        -DSPDLOG_BUILD_TESTS=OFF
        TEST_COMMAND ""
)

include_directories(${CMAKE_CURRENT_BINARY_DIR}/dep_spdlog-prefix/src/dep_spdlog/include)
add_dependencies(odbcabstraction dep_spdlog)

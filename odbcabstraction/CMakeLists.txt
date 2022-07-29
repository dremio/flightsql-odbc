# Copyright (C) 2020-2022 Dremio Corporation
#
# See "LICENSE" for license information.
#

set(CMAKE_POSITION_INDEPENDENT_CODE ON)

include_directories(include)

# Ensure fmt is loaded as header only
add_compile_definitions(FMT_HEADER_ONLY)

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

include(FetchContent)
FetchContent_Declare(
        spdlog
        URL https://github.com/gabime/spdlog/archive/76fb40d95455f249bd70824ecfcae7a8f0930fa3.zip
        CONFIGURE_COMMAND ""
        BUILD_COMMAND ""
)
FetchContent_GetProperties(spdlog)
if(NOT spdlog_POPULATED)
    FetchContent_Populate(spdlog)
endif()

add_library(spdlog INTERFACE)
target_include_directories(spdlog INTERFACE ${spdlog_SOURCE_DIR}/include)

add_dependencies(odbcabstraction spdlog)
target_include_directories(odbcabstraction PUBLIC ${spdlog_SOURCE_DIR}/include)

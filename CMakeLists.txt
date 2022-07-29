# Copyright (C) 2020-2022 Dremio Corporation
#
# See "LICENSE" for license information.
#

cmake_minimum_required(VERSION 3.11)
set(CMAKE_CXX_STANDARD 11)

project(flightsql_odbc)

# Add Boost dependencies. Should be pre-installed (Brew on Mac).
find_package(Boost REQUIRED)
include_directories(${Boost_INCLUDE_DIRS})

# Add ODBC dependencies.
if (APPLE)
    set(ODBC_INCLUDE_DIRS /usr/local/Cellar/libiodbc/3.52.15/include)
    add_compile_definitions(HAVE_LONG_LONG SQLCOLATTRIBUTE_SQLLEN WITH_IODBC)
else()
    find_package(ODBC REQUIRED)
endif()
include_directories(${ODBC_INCLUDE_DIRS})

if(CMAKE_BUILD_TYPE STREQUAL "Release")
    add_compile_definitions(NDEBUG)
endif()

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

# Fetch and include GTest
# Adapted from Google's documentation: https://google.github.io/googletest/quickstart-cmake.html#set-up-a-project
include(FetchContent)
FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/609281088cfefc76f9d0ce82e1ff6c30cc3591e5.zip
)
# For Windows: Prevent overriding the parent project's compiler/linker settings
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

add_subdirectory(flight_sql)
add_subdirectory(odbcabstraction)

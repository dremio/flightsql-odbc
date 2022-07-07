# Copyright (C) 2020-2022 Dremio Corporation
#
# See "LICENSE" for license information.
#

cmake_minimum_required(VERSION 3.11)
set(CMAKE_CXX_STANDARD 11)

SET(GCC_EXTRA_OPTS  "-O2 -g -DNDEBUG -pg")
SET(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} ${GCC_EXTRA_OPTS}")
SET(CMAKE_EXE_LINKER_FLAGS  "${CMAKE_EXE_LINKER_FLAGS} ${GCC_COVERAGE_LINK_FLAGS}")

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

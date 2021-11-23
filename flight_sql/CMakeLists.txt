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

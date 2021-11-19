cmake_minimum_required(VERSION 3.11)
set(CMAKE_CXX_STANDARD 11)

project(flight_odbc_driver)
include(FindODBC)

SET(Arrow_STATIC ON)
find_package(Arrow REQUIRED)
find_package(ArrowFlight REQUIRED PATHS /usr/local/lib/cmake/arrow/)
find_package(ArrowFlightSql REQUIRED PATHS /usr/local/lib/cmake/arrow/)

enable_testing()
find_package(GTest REQUIRED)

message(STATUS "Arrow version: ${ARROW_VERSION}")
message(STATUS "Arrow SO version: ${ARROW_FULL_SO_VERSION}")

add_executable(flight_odbc_driver main.cc
        connection.cc flight_sql/FlightSqlDriver.cc
        flight_sql/FlightSqlConnection.cc
        flight_sql/flight_sql_auth_method.cc)
target_link_libraries(flight_odbc_driver ${ODBC_LIBRARIES} arrow_static arrow_flight arrow_flight_sql )

add_executable(odbc_test connection.cc flight_sql/FlightConnectionTest.cc flight_sql/FlightSqlDriver.cc
        flight_sql/FlightSqlConnection.cc flight_sql/flight_sql_auth_method.cc)

target_link_libraries(odbc_test GTest::gtest GTest::gtest_main arrow_static arrow_flight arrow_flight_sql)

add_test(connection_test odbc_test)
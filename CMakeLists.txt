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

add_executable(flight_odbc_driver main.cpp
       connection.cpp flight_sql/FlightSqlDriver.cpp flight_sql/FlightSqlDriver.h
        flight_sql/FlightSqlConnection.cpp flight_sql/FlightSqlConnection.h)
target_link_libraries(flight_odbc_driver ${ODBC_LIBRARIES} arrow_static arrow_flight arrow_flight_sql )

add_executable(odbc_test connection.cpp flight_sql/FlightConnectionTest.cpp flight_sql/FlightSqlDriver.cpp flight_sql/FlightSqlDriver.h
        flight_sql/FlightSqlConnection.cpp)

target_link_libraries(odbc_test GTest::gtest GTest::gtest_main arrow_static arrow_flight arrow_flight_sql)

add_test(connection_test odbc_test)
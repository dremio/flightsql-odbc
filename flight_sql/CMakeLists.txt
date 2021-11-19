cmake_minimum_required(VERSION 3.11)
set(CMAKE_CXX_STANDARD 11)

SET(Arrow_STATIC ON)
find_package(Arrow REQUIRED)
find_package(ArrowFlight REQUIRED PATHS /usr/local/lib/cmake/arrow/)
find_package(ArrowFlightSql REQUIRED PATHS /usr/local/lib/cmake/arrow/)
find_package(GTest REQUIRED)

enable_testing()
add_executable(sql_moduler main.cc FlightSqlDriver.cc FlightSqlDriver.h
        FlightSqlConnection.cc FlightSqlConnection.h)

target_link_libraries(sql_moduler flight_odbc_driver arrow_static arrow_flight arrow_flight_sql)

target_include_directories(sql_moduler PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

add_executable(odbc_test FlightConnectionTest.cc FlightSqlDriver.cc FlightSqlDriver.h
        FlightSqlConnection.cc)

target_link_libraries(odbc_test flight_odbc_driver GTest::gtest GTest::gtest_main arrow_static arrow_flight arrow_flight_sql)

add_test(connection_test odbc_test)
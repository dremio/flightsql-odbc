cmake_minimum_required(VERSION 3.11)
set(CMAKE_CXX_STANDARD 11)

enable_testing()
add_executable(odbc_flight_sql FlightSqlDriver.cc FlightSqlDriver.h
        FlightSqlConnection.cc FlightSqlConnection.h)

target_include_directories(odbc_flight_sql PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

add_executable(odbc_test  ../connection.cc FlightConnectionTest.cc FlightSqlDriver.cc FlightSqlDriver.h
        FlightSqlConnection.cc)

target_link_libraries(odbc_test GTest::gtest GTest::gtest_main arrow_static arrow_flight arrow_flight_sql)

add_test(connection_test odbc_test)
cmake_minimum_required(VERSION 3.11)
set(CMAKE_CXX_STANDARD 11)

project(flight_odbc_driver)
include(FindODBC)

message(STATUS "Arrow version: ${ARROW_VERSION}")
message(STATUS "Arrow SO version: ${ARROW_FULL_SO_VERSION}")

add_subdirectory(flight_sql)

add_library(flight_odbc_driver connection.cc)
target_link_libraries(flight_odbc_driver ${ODBC_LIBRARIES})
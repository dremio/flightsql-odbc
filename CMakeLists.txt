cmake_minimum_required(VERSION 3.11)
set(CMAKE_CXX_STANDARD 11)

project(flight_odbc_driver)
include(FindODBC)

add_subdirectory(flight_sql)

add_library(abstraction_layer connection.cc)
target_link_libraries(abstraction_layer ${ODBC_LIBRARIES})

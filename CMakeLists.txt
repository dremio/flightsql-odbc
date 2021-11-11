cmake_minimum_required(VERSION 3.11)
project(flight_odbc_driver)
include(FindODBC)
find_package(Boost)

set(CMAKE_CXX_STANDARD 11)

add_executable(flight_odbc_driver main.cpp connection.h driver.h statement.h)
target_link_libraries(flight_odbc_driver ${ODBC_LIBRARIES})

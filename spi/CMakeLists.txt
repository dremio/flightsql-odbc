include(FindODBC)

add_library(spi connection.cc exceptions.cc)
target_link_libraries(spi ${ODBC_LIBRARIES})

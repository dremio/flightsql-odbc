include(FindODBC)

add_library(abstraction_layer connection.cc)
target_link_libraries(abstraction_layer ${ODBC_LIBRARIES})

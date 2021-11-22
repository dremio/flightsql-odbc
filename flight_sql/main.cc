#include "flight_sql_driver.h"
#include <arrow/flight/api.h>
#include <arrow/flight/flight_sql/api.h>
#include <iostream>

using arrow::Status;
using arrow::flight::FlightClient;
using arrow::flight::Location;
using arrow::flight::sql::FlightSqlClient;

using abstraction_layer::Connection;
using flight_sql_odbc::FlightSqlDriver;

int main() {
  FlightSqlDriver driver;

  const std::shared_ptr<Connection> &connection =
      driver.CreateConnection(abstraction_layer::V_3);

  std::map<std::string, Connection::Property> properties = {
      {Connection::HOST, std::string("0.0.0.0")},
      {Connection::PORT, 32010},
      {Connection::USER, std::string("user")},
      {Connection::PASSWORD, std::string("password")},
  };
  std::vector<std::string> missing_attr;
  connection->Connect(properties, missing_attr);

  return 0;
}

#include <iostream>
#include <arrow/flight/api.h>
#include <arrow/flight/flight_sql/api.h>
#include "flight_sql/FlightSqlDriver.h"

using arrow::Status;
using arrow::flight::Location;
using arrow::flight::FlightClient;
using arrow::flight::sql::FlightSqlClient;

int main() {
  FlightSqlDriver driver;

  const std::shared_ptr<Connection> &connection = driver.CreateConnection();

  std::map<std::string, Connection::Property> properties = {
          {Connection::HOST, std::string("0.0.0.0")},
          {Connection::PORT, 31337},
  };
  std::vector<std::string> missing_attr;
  connection->Connect(properties, missing_attr);

  return 0;
}

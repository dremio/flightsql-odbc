#include "flight_sql/FlightSqlDriver.h"
#include <arrow/flight/api.h>
#include <arrow/flight/flight_sql/api.h>
#include <iostream>

using arrow::Status;
using arrow::flight::FlightClient;
using arrow::flight::Location;
using arrow::flight::sql::FlightSqlClient;

int main() {
  FlightSqlDriver driver;

  const std::shared_ptr<Connection> &connection = driver.CreateConnection(V_3);

  std::map<std::string, Connection::Property> properties = {
      {Connection::HOST, std::string("0.0.0.0")},
      {Connection::PORT, 32010},
      {Connection::USER, std::string("dremio")},
      {Connection::PASSWORD, std::string("dremio123")},
  };
  std::vector<std::string> missing_attr;
  connection->Connect(properties, missing_attr);

  return 0;
}

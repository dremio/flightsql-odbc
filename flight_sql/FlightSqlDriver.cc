#include "FlightSqlDriver.h"
#include "FlightSqlConnection.h"

std::shared_ptr<Connection> FlightSqlDriver::CreateConnection() {
  return std::make_shared<FlightSqlConnection>();
}

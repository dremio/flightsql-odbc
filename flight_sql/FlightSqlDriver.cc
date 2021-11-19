#include "FlightSqlDriver.h"
#include "FlightSqlConnection.h"

std::shared_ptr<Connection>
FlightSqlDriver::CreateConnection(OdbcVersion odbc_version) {
  return std::make_shared<FlightSqlConnection>(odbc_version);
}

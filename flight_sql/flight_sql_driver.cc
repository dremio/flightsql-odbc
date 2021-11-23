#include "flight_sql_driver.h"
#include "flight_sql_connection.h"

namespace driver {
namespace flight_sql {

using spi::Connection;
using spi::OdbcVersion;

std::shared_ptr<Connection>
FlightSqlDriver::CreateConnection(OdbcVersion odbc_version) {
  return std::make_shared<FlightSqlConnection>(odbc_version);
}
} // namespace flight_sql
} // namespace driver

#include "flight_sql_driver.h"
#include "flight_sql_connection.h"

namespace flight_sql_odbc {
std::shared_ptr<Connection>
FlightSqlDriver::CreateConnection(OdbcVersion odbc_version) {
  return std::make_shared<FlightSqlConnection>(odbc_version);
}
} // namespace flight_sql_odbc
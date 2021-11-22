#pragma once

#include "connection.h"
#include "flight_sql_connection.h"
#include <arrow/flight/client.h>
#include <map>
#include <memory>

namespace flight_sql_odbc {

using arrow::flight::FlightCallOptions;
using arrow::flight::FlightClient;

class FlightSqlAuthMethod {
public:
  virtual ~FlightSqlAuthMethod() = default;

  virtual void Authenticate(FlightSqlConnection &connection,
                            FlightCallOptions &call_options) = 0;

  static std::unique_ptr<FlightSqlAuthMethod>
  FromProperties(const std::unique_ptr<FlightClient> &client,
                 const std::map<std::string, Connection::Property> &properties);

protected:
  FlightSqlAuthMethod() = default;
};

} // namespace flight_sql_odbc

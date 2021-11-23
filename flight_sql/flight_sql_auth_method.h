#pragma once

#include "connection.h"
#include "flight_sql_connection.h"
#include <arrow/flight/client.h>
#include <map>
#include <memory>

namespace driver {
namespace flight_sql {

class FlightSqlAuthMethod {
public:
  virtual ~FlightSqlAuthMethod() = default;

  virtual void Authenticate(FlightSqlConnection &connection,
                            arrow::flight::FlightCallOptions &call_options) = 0;

  static std::unique_ptr<FlightSqlAuthMethod> FromProperties(
      const std::unique_ptr<arrow::flight::FlightClient> &client,
      const std::map<std::string, spi::Connection::Property> &properties);

protected:
  FlightSqlAuthMethod() = default;
};

} // namespace flight_sql
} // namespace driver

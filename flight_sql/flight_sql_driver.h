#pragma once

#include "driver.h"

namespace flight_sql_odbc {

using abstraction_layer::Connection;
using abstraction_layer::Driver;
using abstraction_layer::OdbcVersion;

class FlightSqlDriver : public Driver {
public:
  std::shared_ptr<Connection>
  CreateConnection(OdbcVersion odbc_version) override;
};

}; // namespace flight_sql_odbc
#pragma once

#include "driver.h"

namespace driver {
namespace flight_sql {

using spi::Connection;
using spi::Driver;
using spi::OdbcVersion;

class FlightSqlDriver : public Driver {
public:
  std::shared_ptr<Connection>
  CreateConnection(OdbcVersion odbc_version) override;
};

}; // namespace flight_sql
} // namespace driver

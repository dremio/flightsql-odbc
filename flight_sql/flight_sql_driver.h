#pragma once

#include "driver.h"

namespace driver {
namespace flight_sql {

class FlightSqlDriver : public spi::Driver {
public:
  std::shared_ptr<spi::Connection>
  CreateConnection(spi::OdbcVersion odbc_version) override;
};

}; // namespace flight_sql
} // namespace driver

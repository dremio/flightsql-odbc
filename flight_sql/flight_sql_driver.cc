/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include "flight_sql_connection.h"
#include <odbcabstraction/platform.h>
#include <flight_sql/flight_sql_driver.h>

namespace driver {
namespace flight_sql {

using odbcabstraction::Connection;
using odbcabstraction::OdbcVersion;

FlightSqlDriver::FlightSqlDriver()
    : diagnostics_("Apache Arrow", "Flight SQL", OdbcVersion::V_3)
{}

std::shared_ptr<Connection>
FlightSqlDriver::CreateConnection(OdbcVersion odbc_version) {
  return std::make_shared<FlightSqlConnection>(odbc_version);
}

odbcabstraction::Diagnostics &FlightSqlDriver::GetDiagnostics() {
  return diagnostics_;
}
} // namespace flight_sql
} // namespace driver

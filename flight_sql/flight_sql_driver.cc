/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include "flight_sql_connection.h"
#include <odbcabstraction/platform.h>
#include <flight_sql/flight_sql_driver.h>

#include <grpc/grpc.h>

namespace driver {
namespace flight_sql {

using odbcabstraction::Connection;
using odbcabstraction::OdbcVersion;

FlightSqlDriver::FlightSqlDriver()
    : diagnostics_("Apache Arrow", "Flight SQL", OdbcVersion::V_3),
      version_("0.9.0.0") {
  grpc_init();
}

FlightSqlDriver::~FlightSqlDriver() {
  grpc_shutdown_blocking();
}

std::shared_ptr<Connection>
FlightSqlDriver::CreateConnection(OdbcVersion odbc_version) {
  return std::make_shared<FlightSqlConnection>(odbc_version, version_);
}

odbcabstraction::Diagnostics &FlightSqlDriver::GetDiagnostics() {
  return diagnostics_;
}

void FlightSqlDriver::SetVersion(std::string version) {
  version_ = std::move(version);
}
} // namespace flight_sql
} // namespace driver

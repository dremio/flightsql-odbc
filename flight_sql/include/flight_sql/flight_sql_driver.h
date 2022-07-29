/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#pragma once

#include <odbcabstraction/spi/driver.h>
#include <odbcabstraction/diagnostics.h>

namespace driver {
namespace flight_sql {

class FlightSqlDriver : public odbcabstraction::Driver {
private:
  odbcabstraction::Diagnostics diagnostics_;
  std::string version_;

public:
  FlightSqlDriver();

  std::shared_ptr<odbcabstraction::Connection>
  CreateConnection(odbcabstraction::OdbcVersion odbc_version) override;

  odbcabstraction::Diagnostics &GetDiagnostics() override;

  void SetVersion(std::string version) override;

  void RegisterLog() override;
};

}; // namespace flight_sql
} // namespace driver

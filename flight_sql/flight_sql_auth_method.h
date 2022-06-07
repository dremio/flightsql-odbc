/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#pragma once

#include "flight_sql_connection.h"
#include <arrow/flight/client.h>
#include <map>
#include <memory>
#include <odbcabstraction/spi/connection.h>
#include <string>

namespace driver {
namespace flight_sql {

class FlightSqlAuthMethod {
public:
  virtual ~FlightSqlAuthMethod() = default;

  virtual void Authenticate(FlightSqlConnection &connection,
                            arrow::flight::FlightCallOptions &call_options) = 0;

  virtual std::string GetUser() { return std::string(); }

  static std::unique_ptr<FlightSqlAuthMethod> FromProperties(
      const std::unique_ptr<arrow::flight::FlightClient> &client,
      const odbcabstraction::Connection::ConnPropertyMap &properties);

protected:
  FlightSqlAuthMethod() = default;
};

} // namespace flight_sql
} // namespace driver

/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#pragma once

#include <memory>

#include <odbcabstraction/types.h>
#include <odbcabstraction/diagnostics.h>

namespace driver {
namespace odbcabstraction {

class Connection;

/// \brief High-level representation of an ODBC driver.
class Driver {
protected:
  Driver() = default;

public:
  virtual ~Driver() = default;

  /// \brief Create a connection using given ODBC version.
  /// \param odbc_version ODBC version to be used.
  virtual std::shared_ptr<Connection>
  CreateConnection(OdbcVersion odbc_version) = 0;

  /// \brief Gets the diagnostics for this connection.
  /// \return the diagnostics
  virtual Diagnostics& GetDiagnostics() = 0;

  /// \brief Sets the driver version.
  virtual void SetVersion(std::string version) = 0;

  /// \brief Register a log to be used by the system.
  virtual void RegisterLog() = 0;
};

} // namespace odbcabstraction
} // namespace driver

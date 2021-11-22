#include <memory>

#include "connection.h"
#include "types.h"

#pragma once

/// \brief High-level representation of an ODBC driver.
class Driver {
public:
  virtual ~Driver() = default;

  /// \brief Create a connection using given ODBC version.
  /// \param odbc_version ODBC version to be used.
  virtual std::shared_ptr<Connection>
  CreateConnection(OdbcVersion odbc_version) = 0;

protected:
  Driver() = default;
};

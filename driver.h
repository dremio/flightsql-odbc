#include <sql.h>
#include <memory>

#include "connection.h"

#pragma once

// Let's make all interfaces have public no-op virtual destructors.
class Driver {
public:
  virtual std::shared_ptr<Connection> CreateConnection(int odbc_version) = 0;
};

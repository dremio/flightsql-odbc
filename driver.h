#include <memory>

#include "connection.h"

#pragma once

// Let's make all interfaces have public no-op virtual destructors.
class Driver {
public:
  // This should take an ODBC version which can be 2, 3, or 4.
  virtual std::shared_ptr<Connection> CreateConnection() = 0;
};

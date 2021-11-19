#include <memory>

#include "connection.h"
#include "types.h"

#pragma once

class Driver {
public:
  virtual ~Driver() = default;

  virtual std::shared_ptr<Connection>
  CreateConnection(OdbcVersion odbc_version) = 0;

protected:
  Driver() = default;
};

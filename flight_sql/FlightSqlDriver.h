#pragma once

#include "../driver.h"

class FlightSqlDriver: public Driver {
public:
  std::shared_ptr<Connection> CreateConnection() override;
};

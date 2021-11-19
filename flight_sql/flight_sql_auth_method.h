#pragma once

#include "../connection.h"
#include <arrow/flight/client.h>
#include <map>
#include <memory>

using arrow::flight::FlightCallOptions;
using arrow::flight::FlightClient;

class FlightSqlAuthMethod {
public:
  virtual void Authenticate(FlightCallOptions &call_options) = 0;

  static std::unique_ptr<FlightSqlAuthMethod>
  FromProperties(const std::unique_ptr<FlightClient> &client,
                 const std::map<std::string, Connection::Property> &properties);

protected:
  FlightSqlAuthMethod() = default;
};

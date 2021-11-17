#pragma once

#include "../connection.h"
#include <arrow/flight/api.h>
#include <arrow/flight/flight_sql/api.h>

using arrow::flight::FlightCallOptions;
using arrow::flight::sql::FlightSqlClient;

class FlightSqlConnection: public Connection {
private:
  std::unique_ptr<FlightSqlClient> client_;
  FlightCallOptions call_options_;
  bool closed_;

public:
  void Connect(const std::map<std::string, Property> &properties, std::vector<std::string> &missing_attr) override;

  void Close() override;

  std::shared_ptr<Statement> CreateStatement() override;

  void SetAttribute(AttributeId attribute, const Attribute &value) override;

  Attribute GetAttribute(AttributeId attribute) override;

  Info GetInfo(uint16_t info_type) override;
};

#pragma once

#include "../connection.h"
#include <arrow/flight/api.h>
#include <arrow/flight/flight_sql/api.h>

using arrow::flight::FlightCallOptions;
using arrow::flight::sql::FlightSqlClient;

class FlightSqlConnection : public Connection {
private:
  std::unique_ptr<FlightSqlClient> client_;
  FlightCallOptions call_options_;
  bool closed_;
  std::map<AttributeId, Attribute> attribute_;

public:
  explicit FlightSqlConnection(OdbcVersion odbc_version);

  void Connect(const std::map<std::string, Property> &properties,
               std::vector<std::string> &missing_attr) override;

  void Close() override;

  std::shared_ptr<Statement> CreateStatement() override;

  void SetAttribute(AttributeId attribute, const Attribute &value) override;

  boost::optional<Connection::Attribute>
  GetAttribute(Connection::AttributeId attribute) override;

  Info GetInfo(uint16_t info_type) override;

  static arrow::flight::Location
  GetLocation(const std::map<std::string, Property> &properties);

  static arrow::flight::FlightClientOptions
  GetFlightClientOptions(const std::map<std::string, Property> &properties);
};

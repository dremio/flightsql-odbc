#pragma once

#include "connection.h"
#include <arrow/flight/api.h>
#include <arrow/flight/flight_sql/api.h>

namespace driver {
namespace flight_sql {

class FlightSqlConnection : public spi::Connection {
private:
  spi::OdbcVersion odbc_version_;
  std::unique_ptr<arrow::flight::sql::FlightSqlClient> sql_client_;
  arrow::flight::FlightCallOptions call_options_;
  bool closed_;
  std::map<AttributeId, Attribute> attribute_;

public:
  explicit FlightSqlConnection(spi::OdbcVersion odbc_version);

  void Connect(const std::map<std::string, Property> &properties,
               std::vector<std::string> &missing_attr) override;

  void Close() override;

  std::shared_ptr<spi::Statement> CreateStatement() override;

  void SetAttribute(AttributeId attribute, const Attribute &value) override;

  boost::optional<Connection::Attribute>
  GetAttribute(Connection::AttributeId attribute) override;

  Info GetInfo(uint16_t info_type) override;

  static arrow::flight::Location
  GetLocation(const std::map<std::string, Property> &properties);

  static arrow::flight::FlightClientOptions
  GetFlightClientOptions(const std::map<std::string, Property> &properties);

  arrow::flight::FlightCallOptions BuildCallOptions();
};
} // namespace flight_sql
} // namespace driver

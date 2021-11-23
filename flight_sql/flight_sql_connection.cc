#include "flight_sql_connection.h"
#include "exceptions.h"
#include "flight_sql_auth_method.h"
#include <boost/optional.hpp>
#include <iostream>

namespace flight_sql_odbc {

using abstraction_layer::OdbcException;
using arrow::Result;
using arrow::Status;
using arrow::flight::FlightClient;
using arrow::flight::FlightClientOptions;
using arrow::flight::Location;
using arrow::flight::TimeoutDuration;
using arrow::flight::sql::FlightSqlClient;

inline void ThrowIfNotOK(const Status &status) {
  if (!status.ok()) {
    throw OdbcException(status.ToString());
  }
}

void FlightSqlConnection::Connect(
    const std::map<std::string, Property> &properties,
    std::vector<std::string> &missing_attr) {
  try {
    Location location = GetLocation(properties);
    FlightClientOptions client_options = GetFlightClientOptions(properties);

    std::unique_ptr<FlightClient> flight_client;
    ThrowIfNotOK(
        FlightClient::Connect(location, client_options, &flight_client));

    std::unique_ptr<FlightSqlAuthMethod> auth_method =
        FlightSqlAuthMethod::FromProperties(flight_client, properties);
    auth_method->Authenticate(*this, call_options_);

    sql_client_.reset(new FlightSqlClient(std::move(flight_client)));
    SetAttribute(CONNECTION_DEAD, false);

    call_options_ = BuildCallOptions();
  } catch (std::exception &e) {
    SetAttribute(CONNECTION_DEAD, true);
    sql_client_.reset();

    throw e;
  }
}

FlightCallOptions FlightSqlConnection::BuildCallOptions() {
  // Set CONNECTION_TIMEOUT attribute
  FlightCallOptions call_options;
  const boost::optional<Connection::Attribute> &connection_timeout =
      GetAttribute(CONNECTION_TIMEOUT);
  if (connection_timeout.has_value()) {
    call_options.timeout =
        TimeoutDuration{boost::get<double>(connection_timeout.value())};
  }

  return call_options;
}

FlightClientOptions FlightSqlConnection::GetFlightClientOptions(
    const std::map<std::string, Property> &properties) {
  FlightClientOptions options;
  // TODO: Set up SSL properties
  return options;
}

Location FlightSqlConnection::GetLocation(
    const std::map<std::string, Property> &properties) {
  const std::string &host = boost::get<std::string>(properties.at(HOST));
  const int &port = boost::get<int>(properties.at(PORT));

  Location location;
  const auto &it_use_ssl = properties.find(USE_SSL);
  if (it_use_ssl != properties.end() && boost::get<bool>(it_use_ssl->second)) {
    ThrowIfNotOK(Location::ForGrpcTls(host, port, &location));
  } else {
    ThrowIfNotOK(Location::ForGrpcTcp(host, port, &location));
  }
  return location;
}

void FlightSqlConnection::Close() {
  if (closed_) {
    throw OdbcException("Connection already closed.");
  }

  sql_client_.reset();
  closed_ = true;
}

std::shared_ptr<Statement> FlightSqlConnection::CreateStatement() {
  throw OdbcException("CreateStatement not implemented");
}

void FlightSqlConnection::SetAttribute(Connection::AttributeId attribute,
                                       const Connection::Attribute &value) {
  attribute_[attribute] = value;
}

boost::optional<Connection::Attribute>
FlightSqlConnection::GetAttribute(Connection::AttributeId attribute) {
  const auto &it = attribute_.find(attribute);
  return boost::make_optional(it != attribute_.end(), it->second);
}

Connection::Info FlightSqlConnection::GetInfo(uint16_t info_type) {
  throw OdbcException("GetInfo not implemented");
}

FlightSqlConnection::FlightSqlConnection(OdbcVersion odbc_version)
    : odbc_version_(odbc_version) {}

} // namespace flight_sql_odbc

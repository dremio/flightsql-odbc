#include "FlightSqlConnection.h"
#include "flight_sql_auth_method.h"
#include <boost/optional.hpp>

using arrow::Result;
using arrow::Status;
using arrow::flight::FlightClient;
using arrow::flight::FlightClientOptions;
using arrow::flight::Location;
using arrow::flight::TimeoutDuration;
using arrow::flight::sql::FlightSqlClient;

inline void ThrowIfNotOK(const Status &status) {
  if (!status.ok()) {
    throw std::runtime_error(status.ToString());
  }
}

void FlightSqlConnection::Connect(
    const std::map<std::string, Property> &properties,
    std::vector<std::string> &missing_attr) {
  Location location = GetLocation(properties);
  FlightClientOptions client_options = GetFlightClientOptions(properties);

  std::unique_ptr<FlightClient> client;
  ThrowIfNotOK(FlightClient::Connect(location, client_options, &client));

  const boost::optional<Connection::Attribute> &connection_timeout =
      GetAttribute(CONNECTION_TIMEOUT);
  if (connection_timeout.has_value()) {
    call_options_.timeout =
        TimeoutDuration{boost::get<double>(connection_timeout.value())};
  }
  std::unique_ptr<FlightSqlAuthMethod> auth_method =
      FlightSqlAuthMethod::FromProperties(client, properties);
  auth_method->Authenticate(call_options_);

  client_.reset(new FlightSqlClient(std::move(client)));
}

FlightClientOptions FlightSqlConnection::GetFlightClientOptions(
    const std::map<std::string, Property> &properties) {
  // TODO: Use timeout from attributes
  FlightClientOptions options;
  return options;
}

Location FlightSqlConnection::GetLocation(
    const std::map<std::string, Property> &properties) {
  const std::string &host = boost::get<std::string>(properties.at(HOST));
  const int &port = boost::get<int>(properties.at(PORT));

  Location location;
  if (properties.count(USE_SSL) && boost::get<bool>(properties.at(USE_SSL))) {
    ThrowIfNotOK(Location::ForGrpcTls(host, port, &location));
  } else {
    ThrowIfNotOK(Location::ForGrpcTcp(host, port, &location));
  }
  return location;
}

void FlightSqlConnection::Close() {
  if (closed_) {
    throw std::runtime_error("Connection already closed.");
  }

  client_.reset();
  closed_ = true;
}

std::shared_ptr<Statement> FlightSqlConnection::CreateStatement() {
  throw std::runtime_error("CreateStatement not implemented");
}

void FlightSqlConnection::SetAttribute(Connection::AttributeId attribute,
                                       const Connection::Attribute &value) {
  attribute_[attribute] = value;
}

boost::optional<Connection::Attribute>
FlightSqlConnection::GetAttribute(Connection::AttributeId attribute) {
  return boost::make_optional(attribute_.count(attribute),
                              attribute_.find(attribute)->second);
}

Connection::Info FlightSqlConnection::GetInfo(uint16_t info_type) {
  throw std::runtime_error("GetInfo not implemented");
}

FlightSqlConnection::FlightSqlConnection(OdbcVersion odbc_version)
    : Connection(odbc_version) {}

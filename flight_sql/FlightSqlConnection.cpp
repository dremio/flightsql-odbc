#include "FlightSqlConnection.h"

using arrow::Status;
using arrow::Result;
using arrow::flight::Location;
using arrow::flight::FlightClient;
using arrow::flight::sql::FlightSqlClient;

void ThrowIfNotOK(const Status& status) {
  if (!status.ok()) {
    throw std::runtime_error(status.ToString());
  }
}

void FlightSqlConnection::Connect(const std::map<std::string, Property> &properties,
                                  std::vector<std::string> &missing_attr) {
  const std::string &host = boost::get<std::string>(properties.at("HOST"));
  const int &port = boost::get<int>(properties.at("PORT"));

  Location location;
  if (properties.count("USE_SSL") && boost::get<bool>(properties.at("USE_SSL"))) {
    ThrowIfNotOK(Location::ForGrpcTls(host, port, &location));
  } else {
    ThrowIfNotOK(Location::ForGrpcTcp(host, port, &location));
  }

  std::unique_ptr<FlightClient> client;
  ThrowIfNotOK(FlightClient::Connect(location, &client));

  const std::string &username = boost::get<std::string>(properties.at("USERNAME"));
  const std::string &password = boost::get<std::string>(properties.at("PASSWORD"));

  if (!username.empty() || !password.empty()) {
    Result<std::pair<std::string, std::string>> bearer_result =
            client->AuthenticateBasicToken({}, username, password);
    ThrowIfNotOK(bearer_result.status());

    call_options_.headers.push_back(bearer_result.ValueOrDie());
  }

  client_.reset(new FlightSqlClient(std::move(client)));
}

void FlightSqlConnection::Close() {
  throw std::runtime_error("Close not implemented");
}

std::shared_ptr<Statement> FlightSqlConnection::CreateStatement() {
  throw std::runtime_error("CreateStatement not implemented");
}

void FlightSqlConnection::SetAttribute(Connection::AttributeId attribute, const Connection::Attribute &value) {
  throw std::runtime_error("SetAttribute not implemented");
}

Connection::Attribute FlightSqlConnection::GetAttribute(Connection::AttributeId attribute) {
  throw std::runtime_error("GetAttribute not implemented");
}

Connection::Info FlightSqlConnection::GetInfo(uint16_t info_type) {
  throw std::runtime_error("GetInfo not implemented");
}

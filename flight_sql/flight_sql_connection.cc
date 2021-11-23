// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "flight_sql_connection.h"
#include "exceptions.h"
#include "flight_sql_auth_method.h"
#include <boost/optional.hpp>
#include <iostream>

namespace driver {
namespace flight_sql {

using arrow::Result;
using arrow::Status;
using arrow::flight::FlightCallOptions;
using arrow::flight::FlightClient;
using arrow::flight::FlightClientOptions;
using arrow::flight::Location;
using arrow::flight::TimeoutDuration;
using arrow::flight::sql::FlightSqlClient;
using spi::Connection;
using spi::DriverException;
using spi::OdbcVersion;
using spi::Statement;

inline void ThrowIfNotOK(const Status &status) {
  if (!status.ok()) {
    throw DriverException(status.ToString());
  }
}

void FlightSqlConnection::Connect(
    const std::map<std::string, Property> &properties,
    std::vector<std::string> &missing_attr) {
  try {
    Location location = BuildLocation(properties);
    FlightClientOptions client_options = BuildFlightClientOptions(properties);

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

FlightClientOptions FlightSqlConnection::BuildFlightClientOptions(
    const std::map<std::string, Property> &properties) {
  FlightClientOptions options;
  // TODO: Set up TLS  properties
  return options;
}

Location FlightSqlConnection::BuildLocation(
    const std::map<std::string, Property> &properties) {
  const std::string &host = boost::get<std::string>(properties.at(HOST));
  const int &port = boost::get<int>(properties.at(PORT));

  Location location;
  const auto &it_use_tls = properties.find(USE_TLS);
  if (it_use_tls != properties.end() && boost::get<bool>(it_use_tls->second)) {
    ThrowIfNotOK(Location::ForGrpcTls(host, port, &location));
  } else {
    ThrowIfNotOK(Location::ForGrpcTcp(host, port, &location));
  }
  return location;
}

void FlightSqlConnection::Close() {
  if (closed_) {
    throw DriverException("Connection already closed.");
  }

  sql_client_.reset();
  closed_ = true;
}

std::shared_ptr<Statement> FlightSqlConnection::CreateStatement() {
  throw DriverException("CreateStatement not implemented");
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
  throw DriverException("GetInfo not implemented");
}

FlightSqlConnection::FlightSqlConnection(OdbcVersion odbc_version)
    : odbc_version_(odbc_version) {}

} // namespace flight_sql
} // namespace driver

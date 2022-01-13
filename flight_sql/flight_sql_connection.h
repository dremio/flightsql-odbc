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

#pragma once

#include <odbcabstraction/connection.h>

#include <arrow/flight/api.h>
#include <arrow/flight/sql/api.h>

namespace driver {
namespace flight_sql {

class FlightSqlConnection : public odbcabstraction::Connection {
private:
  std::map<AttributeId, Attribute> attribute_;
  arrow::flight::FlightCallOptions call_options_;
  std::unique_ptr<arrow::flight::sql::FlightSqlClient> sql_client_;
  odbcabstraction::OdbcVersion odbc_version_;
  bool closed_;

public:
  explicit FlightSqlConnection(odbcabstraction::OdbcVersion odbc_version);

  void Connect(const std::map<std::string, Property> &properties,
               std::vector<std::string> &missing_attr) override;

  void Close() override;

  std::shared_ptr<odbcabstraction::Statement> CreateStatement() override;

  void SetAttribute(AttributeId attribute, const Attribute &value) override;

  boost::optional<Connection::Attribute>
  GetAttribute(Connection::AttributeId attribute) override;

  Info GetInfo(uint16_t info_type) override;

  /// \brief Builds a Location used for FlightClient connection.
  /// \note Visible for testing
  static arrow::flight::Location
  BuildLocation(const std::map<std::string, Property> &properties);

  /// \brief Builds a FlightClientOptions used for FlightClient connection.
  /// \note Visible for testing
  static arrow::flight::FlightClientOptions
  BuildFlightClientOptions(const std::map<std::string, Property> &properties);

  /// \brief Builds a FlightCallOptions used on gRPC calls.
  /// \note Visible for testing
  arrow::flight::FlightCallOptions BuildCallOptions();
};
} // namespace flight_sql
} // namespace driver

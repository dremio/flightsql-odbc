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

#include "get_info_cache.h"

namespace driver {
namespace flight_sql {

class FlightSqlConnection : public odbcabstraction::Connection {

private:
  std::map<AttributeId, Attribute> attribute_;
  arrow::flight::FlightCallOptions call_options_;
  std::unique_ptr<arrow::flight::sql::FlightSqlClient> sql_client_;
  GetInfoCache info_;
  odbcabstraction::Diagnostics diagnostics_;
  odbcabstraction::OdbcVersion odbc_version_;
  bool closed_;

public:
  static const std::string HOST;
  static const std::string PORT;
  static const std::string USER;
  static const std::string UID;
  static const std::string PASSWORD;
  static const std::string PWD;
  static const std::string TOKEN;
  static const std::string USE_TLS;

  explicit FlightSqlConnection(odbcabstraction::OdbcVersion odbc_version);

  void Connect(const ConnPropertyMap &properties,
               std::vector<std::string> &missing_attr) override;

  void Close() override;

  std::shared_ptr<odbcabstraction::Statement> CreateStatement() override;

  bool SetAttribute(AttributeId attribute, const Attribute &value) override;

  boost::optional<Connection::Attribute>
  GetAttribute(Connection::AttributeId attribute) override;

  Info GetInfo(uint16_t info_type) override;

  /// \brief Builds a Location used for FlightClient connection.
  /// \note Visible for testing
  static arrow::flight::Location
  BuildLocation(const ConnPropertyMap &properties,
                std::vector<std::string> &missing_attr);

  /// \brief Builds a FlightClientOptions used for FlightClient connection.
  /// \note Visible for testing
  static arrow::flight::FlightClientOptions
  BuildFlightClientOptions(const ConnPropertyMap &properties,
                           std::vector<std::string> &missing_attr);

  /// \brief Builds a FlightCallOptions used on gRPC calls.
  /// \note Visible for testing
  const arrow::flight::FlightCallOptions &PopulateCallOptionsFromAttributes();

  odbcabstraction::Diagnostics &GetDiagnostics() override;
};
} // namespace flight_sql
} // namespace driver

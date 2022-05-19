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

#include "flight_sql_connection.h"
#include <arrow/flight/client.h>
#include <map>
#include <memory>
#include <odbcabstraction/connection.h>
#include <string>

namespace driver {
namespace flight_sql {

class FlightSqlAuthMethod {
public:
  virtual ~FlightSqlAuthMethod() = default;

  virtual void Authenticate(FlightSqlConnection &connection,
                            arrow::flight::FlightCallOptions &call_options) = 0;

  virtual std::string GetUser() { return std::string(); }

  static std::unique_ptr<FlightSqlAuthMethod> FromProperties(
      const std::unique_ptr<arrow::flight::FlightClient> &client,
      const odbcabstraction::Connection::ConnPropertyMap &properties);

protected:
  FlightSqlAuthMethod() = default;
};

} // namespace flight_sql
} // namespace driver
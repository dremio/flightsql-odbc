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

#include <flight_sql/flight_sql_driver.h>

#include <arrow/flight/api.h>
#include <arrow/flight/sql/api.h>
#include "flight_sql_connection.h"
#include <iostream>

using arrow::Status;
using arrow::flight::FlightClient;
using arrow::flight::Location;
using arrow::flight::sql::FlightSqlClient;

using driver::flight_sql::FlightSqlDriver;
using driver::flight_sql::FlightSqlConnection;
using driver::odbcabstraction::Connection;

int main() {
  FlightSqlDriver driver;

  const std::shared_ptr<Connection> &connection =
      driver.CreateConnection(driver::odbcabstraction::V_3);

  std::map<std::string, Connection::Property> properties = {
      {FlightSqlConnection::HOST, std::string("0.0.0.0")},
      {FlightSqlConnection::PORT, 32010},
      {FlightSqlConnection::USER, std::string("user")},
      {FlightSqlConnection::PASSWORD, std::string("password")},
  };
  std::vector<std::string> missing_attr;
  connection->Connect(properties, missing_attr);

  connection->Close();
  return 0;
}

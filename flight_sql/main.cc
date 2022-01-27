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

#include "flight_sql_connection.h"
#include "flight_sql_result_set.h"
#include "flight_sql_result_set_metadata.h"
#include "flight_sql_statement.h"

#include <arrow/flight/api.h>
#include <arrow/flight/sql/api.h>
#include <iostream>
#include <memory>

using arrow::Status;
using arrow::flight::FlightClient;
using arrow::flight::Location;
using arrow::flight::sql::FlightSqlClient;

using driver::flight_sql::FlightSqlConnection;
using driver::flight_sql::FlightSqlDriver;
using driver::odbcabstraction::Connection;
using driver::odbcabstraction::ResultSet;
using driver::odbcabstraction::ResultSetMetadata;
using driver::odbcabstraction::Statement;

int main() {
  FlightSqlDriver driver;

  const std::shared_ptr<Connection> &connection =
      driver.CreateConnection(driver::odbcabstraction::V_3);

  Connection::ConnPropertyMap properties = {
      {FlightSqlConnection::HOST, std::string("0.0.0.0")},
      {FlightSqlConnection::PORT, std::string("32010")},
      {FlightSqlConnection::USER, std::string("dremio")},
      {FlightSqlConnection::PASSWORD, std::string("dremio123")},
  };
  std::vector<std::string> missing_attr;
  connection->Connect(properties, missing_attr);

  const std::shared_ptr<Statement> &statement = connection->CreateStatement();
  statement->Execute("SELECT IncidntNum, Category FROM \"@dremio\".Test");

  const std::shared_ptr<ResultSet> &result_set = statement->GetResultSet();

  const std::shared_ptr<ResultSetMetadata> &metadata =
      result_set->GetMetadata();

  std::cout << metadata->GetColumnCount() << std::endl;
  std::cout << metadata->GetColumnName(1) << std::endl;
  std::cout << metadata->GetColumnName(2) << std::endl;

  int batch_size = 10;
  int max_strlen = 1000;

  char IncidntNum[batch_size][max_strlen];
  size_t IncidntNum_length[batch_size];

  char Category[batch_size][max_strlen];
  size_t Category_length[batch_size];

  result_set->BindColumn(1, driver::odbcabstraction::VARCHAR, 0, 0, IncidntNum,
                         max_strlen, IncidntNum_length);
  result_set->BindColumn(2, driver::odbcabstraction::VARCHAR, 0, 0, Category,
                         max_strlen, Category_length);

  size_t fetched_rows = result_set->Move(batch_size);
  std::cout << "Fetched " << fetched_rows << " rows." << std::endl;

  for (int i = 0; i < fetched_rows; ++i) {
    std::cout << "Row[" << i << "] IncidntNum: '" << IncidntNum[i]
              << "', Category: '" << Category[i] << "'" << std::endl;
  }

  connection->Close();
  return 0;
}

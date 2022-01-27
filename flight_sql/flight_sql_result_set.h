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

#include <map>
#include <memory>

#include <odbcabstraction/result_set.h>
#include <odbcabstraction/types.h>

#pragma once

namespace driver {
namespace odbcabstraction {
class ResultSetMetadata;
}
} // namespace driver

namespace driver {
namespace flight_sql {
class FlightSqlResultSet : public odbcabstraction::ResultSet {
private:
  std::shared_ptr<odbcabstraction::ResultSetMetadata> metadata_;

public:
  explicit FlightSqlResultSet(
      std::shared_ptr<odbcabstraction::ResultSetMetadata> metadata)
      : metadata_(metadata) {}

public:
  virtual ~FlightSqlResultSet() = default;

  virtual std::shared_ptr<odbcabstraction::ResultSetMetadata> GetMetadata() {
    return metadata_;
  };

  virtual void Close() {}

  virtual void BindColumn(int column, odbcabstraction::DataType target_type,
                          int precision, int scale, void *buffer,
                          size_t buffer_length, size_t *strlen_buffer,
                          size_t strlen_buffer_len) {}

  virtual size_t Move(size_t rows) { return 0; }

  virtual bool GetData(int column, odbcabstraction::DataType target_type,
                       int precision, int scale, void *buffer,
                       size_t buffer_length, size_t *strlen_buffer) {
    return false;
  };
};

} // namespace flight_sql
} // namespace driver

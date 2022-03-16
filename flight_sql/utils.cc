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

#include "utils.h"
#include <arrow/type.h>
#include <odbcabstraction/types.h>

namespace driver {
namespace flight_sql {

using odbcabstraction::SqlDataType;

SqlDataType
GetDataTypeFromArrowField(const std::shared_ptr<arrow::Field> &field) {
  const std::shared_ptr<arrow::DataType> &type = field->type();

  switch (type->id()) {
  case arrow::Type::BOOL:
    return odbcabstraction::SqlDataType_BIT;
  case arrow::Type::UINT8:
  case arrow::Type::INT8:
    return odbcabstraction::SqlDataType_TINYINT;
  case arrow::Type::UINT16:
  case arrow::Type::INT16:
    return odbcabstraction::SqlDataType_SMALLINT;
  case arrow::Type::UINT32:
  case arrow::Type::INT32:
    return odbcabstraction::SqlDataType_INTEGER;
  case arrow::Type::UINT64:
  case arrow::Type::INT64:
    return odbcabstraction::SqlDataType_BIGINT;
  case arrow::Type::HALF_FLOAT:
  case arrow::Type::FLOAT:
    return odbcabstraction::SqlDataType_FLOAT;
  case arrow::Type::DOUBLE:
    return odbcabstraction::SqlDataType_DOUBLE;
  case arrow::Type::BINARY:
  case arrow::Type::FIXED_SIZE_BINARY:
  case arrow::Type::LARGE_BINARY:
    return odbcabstraction::SqlDataType_BINARY;
  case arrow::Type::STRING:
  case arrow::Type::LARGE_STRING:
    return odbcabstraction::SqlDataType_VARCHAR;
  case arrow::Type::DATE32:
  case arrow::Type::DATE64:
    return odbcabstraction::SqlDataType_TYPE_DATE;
  case arrow::Type::TIMESTAMP:
    return odbcabstraction::SqlDataType_TYPE_TIMESTAMP;
  case arrow::Type::DECIMAL128:
  case arrow::Type::DECIMAL256:
    return odbcabstraction::SqlDataType_DECIMAL;
  case arrow::Type::TIME32:
  case arrow::Type::TIME64:
    return odbcabstraction::SqlDataType_TYPE_TIME;

    // TODO: Handle remaining types.
  case arrow::Type::INTERVAL_MONTHS:
  case arrow::Type::INTERVAL_DAY_TIME:
  case arrow::Type::INTERVAL_MONTH_DAY_NANO:
  case arrow::Type::LIST:
  case arrow::Type::STRUCT:
  case arrow::Type::SPARSE_UNION:
  case arrow::Type::DENSE_UNION:
  case arrow::Type::DICTIONARY:
  case arrow::Type::MAP:
  case arrow::Type::EXTENSION:
  case arrow::Type::FIXED_SIZE_LIST:
  case arrow::Type::DURATION:
  case arrow::Type::LARGE_LIST:
  case arrow::Type::MAX_ID:
  case arrow::Type::NA:
    break;
  }

  throw driver::odbcabstraction::DriverException("Unsupported data type: " +
                                                 type->ToString());
}

} // namespace flight_sql
} // namespace driver

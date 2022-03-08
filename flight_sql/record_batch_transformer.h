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

#include <arrow/flight/client.h>
#include <arrow/type.h>
#include <memory>

namespace driver {
namespace flight_sql {

class RecordBatchTransformer {
private:
  std::vector<std::function<std::shared_ptr<arrow::Array>(
      const std::shared_ptr<arrow::RecordBatch> &originalRecord,
      const std::shared_ptr<arrow::Schema> &transformedSchema)>>
      transform_task_collection;
  std::vector<std::shared_ptr<arrow::Field>> fields;

public:
  /// It execute the transformation based on the predeclared tasks created by
  /// RenameRecord method and/or AddEmptyFields.
  /// \param original     The original RecordBatch that will be used as base
  ///                     for the transformation.
  /// \return The new transformed RecordBatch.
  std::shared_ptr<arrow::RecordBatch>
  Transform(const std::shared_ptr<arrow::RecordBatch> &original);

  /// It uses the new list of fields constructed during creation of task
  /// to return the new schema.
  /// \return     the schema from the transformedRecordBatch.
  std::shared_ptr<arrow::Schema> GetTransformedSchema();

public:
  class Builder {
  public:
    friend class RecordBatchTransformer;

    /// Based on the original array name and in a target array name it prepares
    /// tasks that will execute the transformation
    /// \param originalName     The original name of the fields.
    /// \param transformedName  The name after the transformation.
    Builder &RenameRecord(const std::string &originalName,
                          const std::string &transformedName);

    /// It add an empty fields to the transformed record batch
    /// \param fieldName   The name of the empty fields.
    /// \param dataType    The target data type for the new fields.
    Builder &AddEmptyFields(const std::string &fieldName,
                            const std::shared_ptr<arrow::DataType> &dataType);

    /// It creates an object of RecordBatchTransformer
    /// \return a RecordBatchTransformer object.
    RecordBatchTransformer Build() const;

    /// It instantiate a Builder object
    /// \param schema   The schema from the original RecordBatch.
    explicit Builder(std::shared_ptr<arrow::Schema> schema);

  private:
    std::vector<std::shared_ptr<arrow::Field>> new_fields;
    std::vector<std::function<std::shared_ptr<arrow::Array>(
        const std::shared_ptr<arrow::RecordBatch> &originalRecord,
        const std::shared_ptr<arrow::Schema> &transformedSchema)>>
        task_collection;
    std::shared_ptr<arrow::Schema> schema;
  };
};
} // namespace flight_sql
} // namespace driver
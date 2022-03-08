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

/// A transformer class which is responsible to convert the name of fields
/// inside a RecordBatch. These fields are changed based on tasks created by the
/// methods RenameRecord() and AddEmptyFields(). The execution of the tasks is handled
/// by the method transformer.
class RecordBatchTransformer {
public :
  class Builder;

  /// Execute the transformation based on predeclared tasks created by
  /// RenameRecord() method and/or AddEmptyFields().
  /// \param original     The original RecordBatch that will be used as base
  ///                     for the transformation.
  /// \return The new transformed RecordBatch.
  std::shared_ptr<arrow::RecordBatch>
  Transform(const std::shared_ptr<arrow::RecordBatch> &original);

  /// Use the new list of fields constructed during creation of task
  /// to return the new schema.
  /// \return     the schema from the transformedRecordBatch.
  std::shared_ptr<arrow::Schema> GetTransformedSchema();

  class Builder {
  public:
    friend class RecordBatchTransformer;

    /// Based on the original array name and in a target array name it prepares
    /// a task that will execute the transformation.
    /// \param originalName     The original name of the field.
    /// \param transformedName  The name after the transformation.
    Builder &RenameRecord(const std::string &originalName,
                          const std::string &transformedName);

    /// Add an empty field to the transformed record batch.
    /// \param fieldName   The name of the empty fields.
    /// \param dataType    The target data type for the new fields.
    Builder &AddEmptyFields(const std::string &fieldName,
                            const std::shared_ptr<arrow::DataType> &dataType);

    /// It creates an object of RecordBatchTransformer
    /// \return a RecordBatchTransformer object.
    RecordBatchTransformer Build();

    /// Instantiate a Builder object.
    /// \param schema   The schema from the original RecordBatch.
    explicit Builder(std::shared_ptr<arrow::Schema> schema);

    const std::vector<std::shared_ptr<arrow::Field>> &getNewFields() const;

    const std::vector<std::function<std::shared_ptr<arrow::Array>(
      const std::shared_ptr<arrow::RecordBatch> &,
      const std::shared_ptr<arrow::Schema> &)>> &getTaskCollection() const;

  private:
    std::vector<std::shared_ptr<arrow::Field>> new_fields_;
    std::vector<std::function<std::shared_ptr<arrow::Array>(
        const std::shared_ptr<arrow::RecordBatch> &originalRecord,
        const std::shared_ptr<arrow::Schema> &transformedSchema)>>
        task_collection_;
    std::shared_ptr<arrow::Schema> schema_;
  };

private:
  Builder builder_;

  explicit RecordBatchTransformer(Builder builder);
};
} // namespace flight_sql
} // namespace driver

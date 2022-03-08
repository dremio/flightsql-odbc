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

using namespace arrow;

/// A transformer class which is responsible to convert the name of fields
/// inside a RecordBatch. These fields are changed based on tasks created by the
/// methods RenameRecord() and AddEmptyFields(). The execution of the tasks is
/// handled by the method transformer.
class RecordBatchTransformer {
private:
  std::vector<std::shared_ptr<Field>> fields_;
  std::vector<std::function<std::shared_ptr<Array>(
      const std::shared_ptr<RecordBatch> &original_record_batch,
      const std::shared_ptr<Schema> &transformed_schema)>>
      tasks_;

public:
  class Builder {
  private:
    std::vector<std::shared_ptr<Field>> new_fields_;
    std::vector<std::function<std::shared_ptr<Array>(
        const std::shared_ptr<RecordBatch> &original_record_batch,
        const std::shared_ptr<Schema> &transformed_schema)>>
        task_collection_;
    std::shared_ptr<Schema> schema_;

  public:
    friend class RecordBatchTransformer;

    /// Based on the original array name and in a target array name it prepares
    /// a task that will execute the transformation.
    /// \param original_name     The original name of the field.
    /// \param transformed_name  The name after the transformation.
    Builder &RenameRecord(const std::string &original_name,
                          const std::string &transformed_name);

    /// Add an empty field to the transformed record batch.
    /// \param field_name   The name of the empty fields.
    /// \param data_type    The target data type for the new fields.
    Builder &AddEmptyFields(const std::string &field_name,
                            const std::shared_ptr<DataType> &data_type);

    /// It creates an object of RecordBatchTransformer
    /// \return a RecordBatchTransformer object.
    RecordBatchTransformer Build();

    /// Instantiate a Builder object.
    /// \param schema   The schema from the original RecordBatch.
    explicit Builder(std::shared_ptr<Schema> schema);
  };

  /// Execute the transformation based on predeclared tasks created by
  /// RenameRecord() method and/or AddEmptyFields().
  /// \param original     The original RecordBatch that will be used as base
  ///                     for the transformation.
  /// \return The new transformed RecordBatch.
  std::shared_ptr<RecordBatch>
  Transform(const std::shared_ptr<RecordBatch> &original);

  /// Use the new list of fields constructed during creation of task
  /// to return the new schema.
  /// \return     the schema from the transformedRecordBatch.
  std::shared_ptr<Schema> GetTransformedSchema();

private:
  explicit RecordBatchTransformer(Builder &builder);
};
} // namespace flight_sql
} // namespace driver

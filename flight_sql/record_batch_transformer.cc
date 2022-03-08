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

#include "record_batch_transformer.h"

#include "utils.h"
#include <arrow/array/util.h>
#include <arrow/testing/gtest_util.h>
#include <iostream>
#include <utility>

#include "arrow/array/array_base.h"

using namespace arrow;

namespace driver {
namespace flight_sql {

namespace {
Result<std::shared_ptr<Array>> MakeEmptyArray(std::shared_ptr<DataType> type,
                                              MemoryPool *memory_pool) {
  std::unique_ptr<ArrayBuilder> builder;
  RETURN_NOT_OK(MakeBuilder(memory_pool, type, &builder));
  RETURN_NOT_OK(builder->Resize(0));
  return builder->Finish();
}
} // namespace

RecordBatchTransformer::Builder &RecordBatchTransformer::Builder::RenameRecord(
    const std::string &originalName, const std::string &transformedName) {

  auto pFunction =
      [&originalName, &transformedName](
          const std::shared_ptr<arrow::RecordBatch> &originalRecord,
          const std::shared_ptr<arrow::Schema> &transformedSchema) {
        auto originalDataType =
            originalRecord->schema()->GetFieldByName(originalName);
        auto transformedDataType =
            transformedSchema->GetFieldByName(transformedName);

        if (originalDataType->type() != transformedDataType->type()) {
          throw odbcabstraction::DriverException(
              "Original data and target data has different types");
        }

        return originalRecord->GetColumnByName(originalName);
      };

  task_collection_.emplace_back(pFunction);

  auto originalFields = schema_->GetFieldByName(originalName);

  new_fields_.push_back(arrow::field(transformedName, originalFields->type(),
                                     originalFields->metadata()));

  return *this;
}

RecordBatchTransformer::Builder &
RecordBatchTransformer::Builder::AddEmptyFields(
    const std::string &fieldName,
    const std::shared_ptr<arrow::DataType> &dataType) {
  auto pFunction =
      [=](const std::shared_ptr<arrow::RecordBatch> &originalRecord,
          const std::shared_ptr<arrow::Schema> &transformedSchema) {
        auto result = MakeEmptyArray(dataType, nullptr);
        driver::flight_sql::ThrowIfNotOK(result.status());

        return result.ValueOrDie();
      };

  task_collection_.emplace_back(pFunction);

  new_fields_.push_back(arrow::field(fieldName, dataType));

  return *this;
}

RecordBatchTransformer RecordBatchTransformer::Builder::Build() {
  RecordBatchTransformer transformer(task_collection_, new_fields_);

  return transformer;
}

RecordBatchTransformer::Builder::Builder(std::shared_ptr<arrow::Schema> schema)
    : schema_(std::move(schema)) {}

std::shared_ptr<arrow::RecordBatch> RecordBatchTransformer::Transform(
    const std::shared_ptr<arrow::RecordBatch> &original) {
  auto newSchema = arrow::schema(fields_);

  std::vector<std::shared_ptr<arrow::Array>> arrays;

  for (const auto &item : transform_task_collection_) {
    arrays.push_back(item(original, newSchema));
  }

  auto transformedBatch =
      arrow::RecordBatch::Make(newSchema, original->num_rows(), arrays);
  return transformedBatch;
}

std::shared_ptr<arrow::Schema> RecordBatchTransformer::GetTransformedSchema() {
  return arrow::schema(fields_);
}

RecordBatchTransformer::RecordBatchTransformer(
  std::vector<std::function<std::shared_ptr<arrow::Array>(
    const std::shared_ptr<arrow::RecordBatch> &,
    const std::shared_ptr<arrow::Schema> &)>> transformTaskCollection,
  std::vector<std::shared_ptr<arrow::Field>> fields) : transform_task_collection_(std::move(
  transformTaskCollection)), fields_(std::move(fields)) {}
} // namespace flight_sql
} // namespace driver

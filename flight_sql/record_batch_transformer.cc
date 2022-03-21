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
#include <arrow/builder.h>
#include <iostream>
#include <utility>

#include "arrow/array/array_base.h"

namespace driver {
namespace flight_sql {

using namespace arrow;

namespace {
Result<std::shared_ptr<Array>> MakeEmptyArray(std::shared_ptr<DataType> type,
                                              MemoryPool *memory_pool,
                                              int64_t array_size) {
  std::unique_ptr<ArrayBuilder> builder;
  RETURN_NOT_OK(MakeBuilder(memory_pool, type, &builder));
  RETURN_NOT_OK(builder->AppendNulls(array_size));
  return builder->Finish();
}
} // namespace

RecordBatchTransformer::Builder &RecordBatchTransformer::Builder::RenameRecord(
    const std::string &original_name, const std::string &transformed_name) {

  auto rename_task = [&original_name, &transformed_name](
                         const std::shared_ptr<RecordBatch> &original_record,
                         const std::shared_ptr<Schema> &transformed_schema) {
    auto original_data_type =
        original_record->schema()->GetFieldByName(original_name);
    auto transformed_data_type =
        transformed_schema->GetFieldByName(transformed_name);

    if (original_data_type->type() != transformed_data_type->type()) {
      throw odbcabstraction::DriverException(
          "Original data and target data has different types");
    }

    return original_record->GetColumnByName(original_name);
  };

  task_collection_.emplace_back(rename_task);

  auto original_fields = schema_->GetFieldByName(original_name);

  new_fields_.push_back(field(transformed_name, original_fields->type(),
                              original_fields->metadata()));

  return *this;
}

RecordBatchTransformer::Builder &
RecordBatchTransformer::Builder::AddEmptyFields(
    const std::string &field_name, const std::shared_ptr<DataType> &data_type) {
  auto empty_fields_task =
      [=](const std::shared_ptr<RecordBatch> &original_record,
          const std::shared_ptr<Schema> &transformed_schema) {
        auto result =
            MakeEmptyArray(data_type, nullptr, original_record->num_rows());
        driver::flight_sql::ThrowIfNotOK(result.status());

        return result.ValueOrDie();
      };

  task_collection_.emplace_back(empty_fields_task);

  new_fields_.push_back(field(field_name, data_type));

  return *this;
}

RecordBatchTransformer RecordBatchTransformer::Builder::Build() {
  RecordBatchTransformer transformer(*this);

  return transformer;
}

RecordBatchTransformer::Builder::Builder(std::shared_ptr<Schema> schema)
    : schema_(std::move(schema)) {}

std::shared_ptr<RecordBatch> RecordBatchTransformer::Transform(
    const std::shared_ptr<RecordBatch> &original) {
  auto new_schema = schema(fields_);

  std::vector<std::shared_ptr<Array>> arrays;

  for (const auto &item : tasks_) {
    arrays.push_back(item(original, new_schema));
  }

  auto transformed_batch =
      RecordBatch::Make(new_schema, original->num_rows(), arrays);
  return transformed_batch;
}

std::shared_ptr<Schema> RecordBatchTransformer::GetTransformedSchema() {
  return schema(fields_);
}

RecordBatchTransformer::RecordBatchTransformer(
    RecordBatchTransformer::Builder &builder) {
  this->fields_.swap(builder.new_fields_);
  this->tasks_.swap(builder.task_collection_);
}
} // namespace flight_sql
} // namespace driver

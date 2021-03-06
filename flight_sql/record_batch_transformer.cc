/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include "record_batch_transformer.h"
#include <odbcabstraction/platform.h> 

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

/// A transformer class which is responsible to convert the name of fields
/// inside a RecordBatch. These fields are changed based on tasks created by the
/// methods RenameField() and AddFieldOfNulls(). The execution of the tasks is
/// handled by the method transformer.
class RecordBatchTransformerWithTasks : public RecordBatchTransformer {
private:
  std::vector<std::shared_ptr<Field>> fields_;
  std::vector<std::function<std::shared_ptr<Array>(
      const std::shared_ptr<RecordBatch> &original_record_batch,
      const std::shared_ptr<Schema> &transformed_schema)>>
      tasks_;

public:
  RecordBatchTransformerWithTasks(
      std::vector<std::shared_ptr<Field>> fields,
      std::vector<std::function<std::shared_ptr<Array>(
          const std::shared_ptr<RecordBatch> &original_record_batch,
          const std::shared_ptr<Schema> &transformed_schema)>>
          tasks) {
    this->fields_.swap(fields);
    this->tasks_.swap(tasks);
  }

  std::shared_ptr<RecordBatch>
  Transform(const std::shared_ptr<RecordBatch> &original) override {
    auto new_schema = schema(fields_);

    std::vector<std::shared_ptr<Array>> arrays;
    arrays.reserve(new_schema->num_fields());

    for (const auto &item : tasks_) {
      arrays.emplace_back(item(original, new_schema));
    }

    auto transformed_batch =
        RecordBatch::Make(new_schema, original->num_rows(), arrays);
    return transformed_batch;
  }

  std::shared_ptr<Schema> GetTransformedSchema() override {
    return schema(fields_);
  }
};
} // namespace

RecordBatchTransformerWithTasksBuilder &
RecordBatchTransformerWithTasksBuilder::RenameField(
    const std::string &original_name, const std::string &transformed_name) {

  auto rename_task = [=](const std::shared_ptr<RecordBatch> &original_record,
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

  if (original_fields->HasMetadata()) {
    new_fields_.push_back(field(transformed_name, original_fields->type(),
                                original_fields->metadata()));
  } else {
    new_fields_.push_back(
        field(transformed_name, original_fields->type(), std::shared_ptr<const KeyValueMetadata>()));
  }

  return *this;
}

RecordBatchTransformerWithTasksBuilder &
RecordBatchTransformerWithTasksBuilder::AddFieldOfNulls(
    const std::string &field_name, const std::shared_ptr<DataType> &data_type) {
  auto empty_fields_task =
      [=](const std::shared_ptr<RecordBatch> &original_record,
          const std::shared_ptr<Schema> &transformed_schema) {
        auto result =
            MakeEmptyArray(data_type, nullptr, original_record->num_rows());
        ThrowIfNotOK(result.status());

        return result.ValueOrDie();
      };

  task_collection_.emplace_back(empty_fields_task);

  new_fields_.push_back(field(field_name, data_type));

  return *this;
}

std::shared_ptr<RecordBatchTransformer>
RecordBatchTransformerWithTasksBuilder::Build() {
  std::shared_ptr<RecordBatchTransformerWithTasks> transformer(
      new RecordBatchTransformerWithTasks(this->new_fields_,
                                          this->task_collection_));

  return transformer;
}

RecordBatchTransformerWithTasksBuilder::RecordBatchTransformerWithTasksBuilder(
    std::shared_ptr<Schema> schema)
    : schema_(std::move(schema)) {}
} // namespace flight_sql
} // namespace driver

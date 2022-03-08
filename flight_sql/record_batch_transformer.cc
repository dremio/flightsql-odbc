#include "record_batch_transformer.h"

#include <utility>
#include <iostream>
#include <arrow/array/util.h>
#include <arrow/testing/gtest_util.h>
#include "utils.h"

#include "arrow/array/array_base.h"

using namespace arrow;

namespace {
  Result<std::shared_ptr<Array>> MakeEmptyArray(std::shared_ptr<DataType> type,
                                                MemoryPool* memory_pool) {
    std::unique_ptr<ArrayBuilder> builder;
    RETURN_NOT_OK(MakeBuilder(memory_pool, type, &builder));
    RETURN_NOT_OK(builder->Resize(0));
    return builder->Finish();
  }
}


RecordBatchTransformer::Builder &
RecordBatchTransformer::Builder::renameRecord(const std::string &originalName,
                                              const std::string &transformedName) {

  auto pFunction = [&originalName, &transformedName](
    const std::shared_ptr<arrow::RecordBatch> &originalRecord,
    const std::shared_ptr<arrow::Schema> &transformedSchema) {
    
    auto originalDataType = originalRecord->schema()->GetFieldByName(originalName);
    auto transformedDataType = transformedSchema->GetFieldByName(transformedName);

    if (originalDataType->type() != transformedDataType->type()) {
      // TODO Refactor this exception
      throw std::invalid_argument("not the same type");
    }

    return originalRecord->GetColumnByName(originalName);
  };

  taskCollection.emplace_back(pFunction);

  auto originalFields = schema->GetFieldByName(originalName);

  newFields.push_back(
    arrow::field(
      transformedName, originalFields->type(),
      originalFields->metadata()));

  return *this;
}

RecordBatchTransformer::Builder &
RecordBatchTransformer::Builder::addEmptyFields(const std::string &fieldName,
                                                const std::shared_ptr<arrow::DataType>& dataType) {
  auto pFunction = [=](
    const std::shared_ptr<arrow::RecordBatch> &originalRecord,
    const std::shared_ptr<arrow::Schema> &transformedSchema) {

    auto result = MakeEmptyArray(dataType, nullptr);
    driver::flight_sql::ThrowIfNotOK(result.status());

    return result.ValueOrDie();
  };

  taskCollection.emplace_back(pFunction);

  newFields.push_back(arrow::field(fieldName, dataType));

  return *this;
}

RecordBatchTransformer RecordBatchTransformer::Builder::Build() const {
  RecordBatchTransformer transformer;

  transformer.transformTaskCollection = taskCollection;
  transformer.fields = newFields;

  return transformer;
}

RecordBatchTransformer::Builder::Builder(std::shared_ptr<arrow::Schema> schema) : schema(std::move(schema)){

}

std::shared_ptr<arrow::RecordBatch> RecordBatchTransformer::transform(
  const std::shared_ptr<arrow::RecordBatch> &original) {
  auto newSchema = arrow::schema(fields);

  std::vector<std::shared_ptr<arrow::Array>> arrays;

  for (const auto &item : transformTaskCollection) {
    arrays.push_back(item(original, newSchema));
  }


  auto transformedBatch = arrow::RecordBatch::Make(newSchema, original->num_rows(),
                                                     arrays);
  return transformedBatch;
}

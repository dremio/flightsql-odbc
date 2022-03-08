#include "record_batch_transformer.h"
#include "gtest/gtest.h"
#include <arrow/record_batch.h>
#include <arrow/testing/gtest_util.h>

namespace driver {
namespace flight_sql {

TEST(Transformer, TransformerTest) {
  std::vector<int> values = {1, 2, 3, 4, 5};
  std::shared_ptr<arrow::Array> array;

  arrow::ArrayFromVector<arrow::Int32Type, int32_t>(values, &array);

  auto schema = arrow::schema({arrow::field("test", arrow::int32(), false)});

  auto originalRecord = arrow::RecordBatch::Make(schema, 4, {array});

  std::string originalName("test");
  std::string transformed("test1");

  auto transformer = RecordBatchTransformer::Builder(schema)
                         .renameRecord(originalName, transformed)
                         .Build();

  const std::shared_ptr<arrow::RecordBatch> &transformedBatch =
      transformer.transform(originalRecord);

  ASSERT_NE(originalRecord->schema(), transformedBatch->schema());
  ASSERT_EQ(originalRecord->GetColumnByName(originalName),
            transformedBatch->GetColumnByName(transformed));
}

TEST(Transformer, TransformerAddEmptyVectorTest) {
  std::vector<int> values = {1, 2, 3, 4, 5};
  std::shared_ptr<arrow::Array> array;

  arrow::ArrayFromVector<arrow::Int32Type, int32_t>(values, &array);

  auto schema = arrow::schema({arrow::field("test", arrow::int32(), false)});

  auto originalBatch = arrow::RecordBatch::Make(schema, 4, {array});

  std::string originalName("test");
  std::string transformed("test1");

  auto transformer = RecordBatchTransformer::Builder(schema)
                         .renameRecord(originalName, transformed)
                         .addEmptyFields(std::string("empty"), arrow::int32())
                         .Build();

  const std::shared_ptr<arrow::RecordBatch> &transformedBatch =
      transformer.transform(originalBatch);

  ASSERT_NE(originalBatch->schema(), transformedBatch->schema());
  ASSERT_EQ(originalBatch->GetColumnByName(originalName),
            transformedBatch->GetColumnByName(transformed));
}
} // namespace flight_sql
} // namespace driver
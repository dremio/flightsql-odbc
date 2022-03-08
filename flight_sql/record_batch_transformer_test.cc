#include "record_batch_transformer.h"
#include "gtest/gtest.h"
#include <arrow/record_batch.h>
#include <arrow/testing/gtest_util.h>

namespace {
std::shared_ptr<arrow::RecordBatch> CreateOriginalRecordBatch() {
  std::vector<int> values = {1, 2, 3, 4, 5};
  std::shared_ptr<arrow::Array> array;

  arrow::ArrayFromVector<arrow::Int32Type, int32_t>(values, &array);

  auto schema = arrow::schema({arrow::field("test", arrow::int32(), false)});

  return arrow::RecordBatch::Make(schema, 4, {array});
}
} // namespace

namespace driver {
namespace flight_sql {

TEST(Transformer, TransformerRenameTest) {
  // Prepare the Original Record Batch
  auto original_record_batch = CreateOriginalRecordBatch();
  auto schema = original_record_batch->schema();

  // Execute the transformation of the Record Batch
  std::string original_name("test");
  std::string transformed_name("test1");

  auto transformer = RecordBatchTransformer::Builder(schema)
    .RenameRecord(original_name, transformed_name)
    .Build();

  auto transformed_record_batch =
      transformer.Transform(original_record_batch);

  // Assert if the schema is not the same
  ASSERT_NE(original_record_batch->schema(), transformed_record_batch->schema());
  // Assert if the data is not changed
  ASSERT_EQ(original_record_batch->GetColumnByName(original_name),
            transformed_record_batch->GetColumnByName(transformed_name));
}

TEST(Transformer, TransformerAddEmptyVectorTest) {
  // Prepare the Original Record Batch
  auto original_record_batch = CreateOriginalRecordBatch();
  auto schema = original_record_batch->schema();

  std::string original_name("test");
  std::string transformed_name("test1");
  auto emptyField = std::string("empty");

  auto transformer = RecordBatchTransformer::Builder(schema)
  .RenameRecord(original_name, transformed_name)
  .AddEmptyFields(emptyField, arrow::int32())
  .Build();

  auto transformed_schema = transformer.GetTransformedSchema();

  ASSERT_EQ(transformed_schema->num_fields(), 2);
  ASSERT_EQ(transformed_schema->GetFieldIndex(transformed_name), 0);
  ASSERT_EQ(transformed_schema->GetFieldIndex(emptyField), 1);

  auto transformed_record_batch = transformer.Transform(original_record_batch);

  // Assert if the schema is not the same
  ASSERT_NE(original_record_batch->schema(), transformed_record_batch->schema());
  // Assert if the data is not changed
  ASSERT_EQ(original_record_batch->GetColumnByName(original_name),
            transformed_record_batch->GetColumnByName(transformed_name));
}

TEST(Transformer, TransformerChangingOrderOfArrayTest) {
  std::vector<int> first_array_value = {1, 2, 3, 4, 5};
  std::vector<int> second_array_value = {6, 7, 8, 9, 10};
  std::vector<int> third_array_value = {2, 4, 6, 8, 10};
  std::shared_ptr<arrow::Array> first_array;
  std::shared_ptr<arrow::Array> second_array;
  std::shared_ptr<arrow::Array> third_array;

  arrow::ArrayFromVector<arrow::Int32Type, int32_t>(first_array_value,
                                                    &first_array);
  arrow::ArrayFromVector<arrow::Int32Type, int32_t>(second_array_value,
                                                    &second_array);
  arrow::ArrayFromVector<arrow::Int32Type, int32_t>(third_array_value,
                                                    &third_array);

  auto schema =
      arrow::schema({arrow::field("first_array", arrow::int32(), false),
                     arrow::field("second_array", arrow::int32(), false),
                     arrow::field("third_array", arrow::int32(), false)});

  auto original_record_batch = arrow::RecordBatch::Make(
      schema, 5, {first_array, second_array, third_array});

  auto transformer =
    RecordBatchTransformer::Builder(schema)
      .RenameRecord(std::string("third_array"), std::string("test3"))
      .RenameRecord(std::string("second_array"), std::string("test2"))
      .RenameRecord(std::string("first_array"), std::string("test1"))
      .AddEmptyFields(std::string("empty"), arrow::int32())
      .Build();

  const std::shared_ptr<arrow::RecordBatch> &transformed_record_batch =
      transformer.Transform(original_record_batch);

  auto transformed_schema = transformed_record_batch->schema();

  // Assert to check if the empty fields was added
  ASSERT_EQ(transformed_record_batch->num_columns(), 4);

  // Assert to make sure that the elements changed his order.
  ASSERT_EQ(transformed_schema->GetFieldIndex("test3"), 0);
  ASSERT_EQ(transformed_schema->GetFieldIndex("test2"), 1);
  ASSERT_EQ(transformed_schema->GetFieldIndex("test1"), 2);
  ASSERT_EQ(transformed_schema->GetFieldIndex("empty"), 3);

  // Assert to make sure that the data didn't change after renaming the arrays
  ASSERT_EQ(transformed_record_batch->GetColumnByName("test3"), third_array);
  ASSERT_EQ(transformed_record_batch->GetColumnByName("test2"), second_array);
  ASSERT_EQ(transformed_record_batch->GetColumnByName("test1"), first_array);
}
} // namespace flight_sql
} // namespace driver
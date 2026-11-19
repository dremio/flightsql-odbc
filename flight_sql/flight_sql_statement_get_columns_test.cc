/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include <odbcabstraction/platform.h>
#include "flight_sql_statement_get_columns.h"
#include "arrow/testing/builder.h"
#include "gtest/gtest.h"
#include <arrow/array.h>
#include <arrow/ipc/writer.h>
#include <arrow/record_batch.h>
#include <arrow/util/key_value_metadata.h>
#include <odbcabstraction/types.h>

using namespace arrow;

namespace {
// One row of a GetTables result that includes the table schema: the shape
// GetColumns_Transformer reads its columns from.
std::shared_ptr<RecordBatch>
CreateGetTablesRecordBatch(const std::shared_ptr<Schema> &table_schema) {
  const std::shared_ptr<Buffer> serialized_schema =
      ipc::SerializeSchema(*table_schema).ValueOrDie();

  std::shared_ptr<Array> catalog_name;
  std::shared_ptr<Array> db_schema_name;
  std::shared_ptr<Array> table_name;
  std::shared_ptr<Array> table_type;
  std::shared_ptr<Array> schema_array;
  ArrayFromVector<StringType, std::string>({"catalog"}, &catalog_name);
  ArrayFromVector<StringType, std::string>({"schema"}, &db_schema_name);
  ArrayFromVector<StringType, std::string>({"table"}, &table_name);
  ArrayFromVector<StringType, std::string>({"TABLE"}, &table_type);
  ArrayFromVector<BinaryType, std::string>({serialized_schema->ToString()},
                                           &schema_array);

  auto schema = arrow::schema({field("catalog_name", utf8()),
                               field("db_schema_name", utf8()),
                               field("table_name", utf8(), false),
                               field("table_type", utf8(), false),
                               field("table_schema", binary(), false)});

  return RecordBatch::Make(
      schema, 1,
      {catalog_name, db_schema_name, table_name, table_type, schema_array});
}

std::string GetString(const std::shared_ptr<RecordBatch> &batch,
                      const std::string &column, int64_t row) {
  return std::static_pointer_cast<StringArray>(batch->GetColumnByName(column))
      ->GetString(row);
}
} // namespace

namespace driver {
namespace flight_sql {

// A server is free to send fields without key-value metadata; SQLColumns must
// still describe them, with the type name taken from the SQL data type.
TEST(GetColumnsTransformer, FieldsWithoutMetadata) {
  auto table_schema = arrow::schema(
      {field("id", int32(), false), field("name", utf8(), true)});
  ASSERT_EQ(nullptr, table_schema->field(0)->metadata());

  odbcabstraction::MetadataSettings metadata_settings{};
  GetColumns_Transformer transformer(metadata_settings, odbcabstraction::V_3,
                                     nullptr);

  auto transformed =
      transformer.Transform(CreateGetTablesRecordBatch(table_schema));

  ASSERT_EQ(2, transformed->num_rows());
  ASSERT_EQ("id", GetString(transformed, "COLUMN_NAME", 0));
  ASSERT_EQ("name", GetString(transformed, "COLUMN_NAME", 1));
  ASSERT_EQ("INTEGER", GetString(transformed, "TYPE_NAME", 0));
  ASSERT_EQ("NO", GetString(transformed, "IS_NULLABLE", 0));
  ASSERT_EQ("YES", GetString(transformed, "IS_NULLABLE", 1));
}

// A type name supplied by the server in the field metadata is still used.
TEST(GetColumnsTransformer, FieldsWithMetadata) {
  auto metadata =
      key_value_metadata({"ARROW:FLIGHT:SQL:TYPE_NAME"}, {"SERVER_INT"});
  auto table_schema =
      arrow::schema({field("id", int32(), false, metadata)});

  odbcabstraction::MetadataSettings metadata_settings{};
  GetColumns_Transformer transformer(metadata_settings, odbcabstraction::V_3,
                                     nullptr);

  auto transformed =
      transformer.Transform(CreateGetTablesRecordBatch(table_schema));

  ASSERT_EQ(1, transformed->num_rows());
  ASSERT_EQ("SERVER_INT", GetString(transformed, "TYPE_NAME", 0));
}

} // namespace flight_sql
} // namespace driver

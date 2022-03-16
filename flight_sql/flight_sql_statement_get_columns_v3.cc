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

#include "flight_sql_statement_get_columns_v3.h"
#include "flight_sql_get_tables_reader.h"
#include "utils.h"

namespace driver {
namespace flight_sql {

using arrow::util::nullopt;
using arrow::util::optional;

namespace {
std::shared_ptr<Schema> GetColumns_V3_Schema() {
  return schema({
      field("TABLE_CAT", utf8()),
      field("TABLE_SCHEM", utf8()),
      field("TABLE_NAME", utf8()),
      field("COLUMN_NAME", utf8()),
      field("DATA_TYPE", int16()),
      field("TYPE_NAME", utf8()),
      field("COLUMN_SIZE", int32()),
      field("BUFFER_LENGTH", int32()),
      field("DECIMAL_DIGITS", int16()),
      field("NUM_PREC_RADIX", int16()),
      field("NULLABLE", int16()),
      field("REMARKS", utf8()),
      field("COLUMN_DEF", utf8()),
      field("SQL_DATA_TYPE", int16()),
      field("SQL_DATETIME_SUB", int16()),
      field("CHAR_OCTET_LENGTH", int32()),
      field("ORDINAL_POSITION", int32()),
      field("IS_NULLABLE", int32()),
  });
}

template <typename BUILDER, typename T>
Status AppendToBuilder(BUILDER &builder, optional<T> opt_value) {
  if (opt_value.has_value()) {
    return builder.Append(opt_value.value());
  } else {
    return builder.AppendNull();
  }
}

template <typename BUILDER, typename T>
Status AppendToBuilder(BUILDER &builder, T value) {
  return builder.Append(value);
}

Result<std::shared_ptr<RecordBatch>>
Transform_inner(const std::shared_ptr<RecordBatch> &original) {
  GetColumns_V3_RecordBatchBuilder builder;
  GetColumns_V3_RecordBatchBuilder::Data data;

  GetTablesReader reader(original);

  while (reader.Next()) {
    const auto &table_catalog = reader.GetCatalogName();
    const auto &table_schema = reader.GetDbSchemaName();
    const auto &table_name = reader.GetTableName();
    const std::shared_ptr<Schema> &schema = reader.GetSchema();
    for (int i = 0; i < schema->num_fields(); ++i) {
      const std::shared_ptr<Field> &field = schema->field(i);

      data.table_cat = table_catalog;
      data.table_schem = table_schema;
      data.table_name = table_name;
      data.column_name = field->name();
      data.data_type = GetDataTypeFromArrowField_V3(field);
      data.type_name = GetTypeNameFromSqlDataType(data.data_type);
      // TODO: Get from field's metadata "ARROW:FLIGHT:SQL:PRECISION"
      data.column_size = nullopt;
      // TODO: Get from column_size
      data.buffer_length = nullopt;
      // TODO: Get from field's metadata "ARROW:FLIGHT:SQL:SCALE"
      data.decimal_digits = nullopt;
      data.num_prec_radix = GetRadixFromSqlDataType(data.data_type);
      data.nullable = field->nullable();
      data.remarks = nullopt;
      data.column_def = nullopt;
      data.sql_data_type = GetNonConciseDataType(data.data_type);
      data.sql_datetime_sub = GetSqlDateTimeSubCode(data.data_type);
      // TODO: Get from column_size
      data.char_octet_length = nullopt;
      data.ordinal_position = i + 1;
      data.is_nullable = field->nullable() ? "YES" : "NO";

      ARROW_RETURN_NOT_OK(builder.Append(data));
    }
  }

  return builder.Build();
}
} // namespace

Result<std::shared_ptr<RecordBatch>> GetColumns_V3_RecordBatchBuilder::Build() {
  ARROW_ASSIGN_OR_RAISE(auto TABLE_CAT_Array, TABLE_CAT_Builder.Finish())
  ARROW_ASSIGN_OR_RAISE(auto TABLE_SCHEM_Array, TABLE_SCHEM_Builder.Finish())
  ARROW_ASSIGN_OR_RAISE(auto TABLE_NAME_Array, TABLE_NAME_Builder.Finish())
  ARROW_ASSIGN_OR_RAISE(auto COLUMN_NAME_Array, COLUMN_NAME_Builder.Finish())
  ARROW_ASSIGN_OR_RAISE(auto DATA_TYPE_Array, DATA_TYPE_Builder.Finish())
  ARROW_ASSIGN_OR_RAISE(auto TYPE_NAME_Array, TYPE_NAME_Builder.Finish())
  ARROW_ASSIGN_OR_RAISE(auto COLUMN_SIZE_Array, COLUMN_SIZE_Builder.Finish())
  ARROW_ASSIGN_OR_RAISE(auto BUFFER_LENGTH_Array,
                        BUFFER_LENGTH_Builder.Finish())
  ARROW_ASSIGN_OR_RAISE(auto DECIMAL_DIGITS_Array,
                        DECIMAL_DIGITS_Builder.Finish())
  ARROW_ASSIGN_OR_RAISE(auto NUM_PREC_RADIX_Array,
                        NUM_PREC_RADIX_Builder.Finish())
  ARROW_ASSIGN_OR_RAISE(auto NULLABLE_Array, NULLABLE_Builder.Finish())
  ARROW_ASSIGN_OR_RAISE(auto REMARKS_Array, REMARKS_Builder.Finish())
  ARROW_ASSIGN_OR_RAISE(auto COLUMN_DEF_Array, COLUMN_DEF_Builder.Finish())
  ARROW_ASSIGN_OR_RAISE(auto SQL_DATA_TYPE_Array,
                        SQL_DATA_TYPE_Builder.Finish())
  ARROW_ASSIGN_OR_RAISE(auto SQL_DATETIME_SUB_Array,
                        SQL_DATETIME_SUB_Builder.Finish())
  ARROW_ASSIGN_OR_RAISE(auto CHAR_OCTET_LENGTH_Array,
                        CHAR_OCTET_LENGTH_Builder.Finish())
  ARROW_ASSIGN_OR_RAISE(auto ORDINAL_POSITION_Array,
                        ORDINAL_POSITION_Builder.Finish())
  ARROW_ASSIGN_OR_RAISE(auto IS_NULLABLE_Array, IS_NULLABLE_Builder.Finish())

  std::vector<std::shared_ptr<Array>> arrays = {
      TABLE_CAT_Array,         TABLE_SCHEM_Array,      TABLE_NAME_Array,
      COLUMN_NAME_Array,       DATA_TYPE_Array,        TYPE_NAME_Array,
      COLUMN_SIZE_Array,       BUFFER_LENGTH_Array,    DECIMAL_DIGITS_Array,
      NUM_PREC_RADIX_Array,    NULLABLE_Array,         REMARKS_Array,
      COLUMN_DEF_Array,        SQL_DATA_TYPE_Array,    SQL_DATETIME_SUB_Array,
      CHAR_OCTET_LENGTH_Array, ORDINAL_POSITION_Array, IS_NULLABLE_Array};

  return RecordBatch::Make(GetColumns_V3_Schema(), num_rows, arrays);
}

Status GetColumns_V3_RecordBatchBuilder::Append(
    const GetColumns_V3_RecordBatchBuilder::Data &data) {
  ARROW_RETURN_NOT_OK(AppendToBuilder(TABLE_CAT_Builder, data.table_cat));
  ARROW_RETURN_NOT_OK(AppendToBuilder(TABLE_SCHEM_Builder, data.table_schem));
  ARROW_RETURN_NOT_OK(AppendToBuilder(TABLE_NAME_Builder, data.table_name));
  ARROW_RETURN_NOT_OK(AppendToBuilder(COLUMN_NAME_Builder, data.column_name));
  ARROW_RETURN_NOT_OK(AppendToBuilder(DATA_TYPE_Builder, data.data_type));
  ARROW_RETURN_NOT_OK(AppendToBuilder(TYPE_NAME_Builder, data.type_name));
  ARROW_RETURN_NOT_OK(AppendToBuilder(COLUMN_SIZE_Builder, data.column_size));
  ARROW_RETURN_NOT_OK(
      AppendToBuilder(BUFFER_LENGTH_Builder, data.buffer_length));
  ARROW_RETURN_NOT_OK(
      AppendToBuilder(DECIMAL_DIGITS_Builder, data.decimal_digits));
  ARROW_RETURN_NOT_OK(
      AppendToBuilder(NUM_PREC_RADIX_Builder, data.num_prec_radix));
  ARROW_RETURN_NOT_OK(AppendToBuilder(NULLABLE_Builder, data.nullable));
  ARROW_RETURN_NOT_OK(AppendToBuilder(REMARKS_Builder, data.remarks));
  ARROW_RETURN_NOT_OK(AppendToBuilder(COLUMN_DEF_Builder, data.column_def));
  ARROW_RETURN_NOT_OK(
      AppendToBuilder(SQL_DATA_TYPE_Builder, data.sql_data_type));
  ARROW_RETURN_NOT_OK(
      AppendToBuilder(SQL_DATETIME_SUB_Builder, data.sql_datetime_sub));
  ARROW_RETURN_NOT_OK(
      AppendToBuilder(CHAR_OCTET_LENGTH_Builder, data.char_octet_length));
  ARROW_RETURN_NOT_OK(
      AppendToBuilder(ORDINAL_POSITION_Builder, data.ordinal_position));
  ARROW_RETURN_NOT_OK(AppendToBuilder(IS_NULLABLE_Builder, data.is_nullable));
  num_rows++;

  return Status::OK();
}

std::shared_ptr<RecordBatch> GetColumns_V3_Transformer::Transform(
    const std::shared_ptr<RecordBatch> &original) {
  const Result<std::shared_ptr<RecordBatch>> &result =
      Transform_inner(original);
  ThrowIfNotOK(result.status());

  return result.ValueOrDie();
}

std::shared_ptr<Schema> GetColumns_V3_Transformer::GetTransformedSchema() {
  return GetColumns_V3_Schema();
}

} // namespace flight_sql
} // namespace driver

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

#include "flight_sql_statement_get_columns_v2.h"
#include "flight_sql_get_tables_reader.h"
#include "utils.h"

namespace driver {
namespace flight_sql {

using arrow::util::make_optional;
using arrow::util::nullopt;
using arrow::util::optional;

namespace {
std::shared_ptr<Schema> GetColumns_V2_Schema() {
  return schema({
      field("TABLE_QUALIFIER", utf8()),
      field("TABLE_OWNER", utf8()),
      field("TABLE_NAME", utf8()),
      field("COLUMN_NAME", utf8()),
      field("DATA_TYPE", int16()),
      field("TYPE_NAME", utf8()),
      field("PRECISION", int32()),
      field("LENGTH", int32()),
      field("SCALE", int16()),
      field("RADIX", int16()),
      field("NULLABLE", int16()),
      field("REMARKS", utf8()),
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
Transform_inner(const std::shared_ptr<RecordBatch> &original,
                const optional<std::string> &column_name_pattern) {
  GetColumns_V2_RecordBatchBuilder builder;
  GetColumns_V2_RecordBatchBuilder::Data data;

  GetTablesReader reader(original);

  optional<boost::regex> column_name_regex =
      column_name_pattern.has_value() ? make_optional(CreateRegexFromSqlPattern(
                                            column_name_pattern.value()))
                                      : nullopt;

  while (reader.Next()) {
    const auto &table_catalog = reader.GetCatalogName();
    const auto &table_schema = reader.GetDbSchemaName();
    const auto &table_name = reader.GetTableName();
    const std::shared_ptr<Schema> &schema = reader.GetSchema();
    for (int i = 0; i < schema->num_fields(); ++i) {
      const std::shared_ptr<Field> &field = schema->field(i);

      if (column_name_regex.has_value() &&
          !boost::regex_match(field->name(), column_name_regex.value())) {
        continue;
      }

      data.table_qualifier = table_catalog;
      data.table_owner = table_schema;
      data.table_name = table_name;
      data.column_name = field->name();
      data.data_type = GetDataTypeFromArrowField_V2(field);
      data.type_name = GetTypeNameFromSqlDataType(data.data_type);
      // TODO: Get from field's metadata "ARROW:FLIGHT:SQL:PRECISION"
      data.precision = nullopt;
      // TODO: Get from precision
      data.length = nullopt;
      // TODO: Get from field's metadata "ARROW:FLIGHT:SQL:SCALE"
      data.scale = nullopt;
      data.radix = GetRadixFromSqlDataType(data.data_type);
      data.nullable = field->nullable();
      data.remarks = nullopt;

      ARROW_RETURN_NOT_OK(builder.Append(data));
    }
  }

  return builder.Build();
}
} // namespace

Result<std::shared_ptr<RecordBatch>> GetColumns_V2_RecordBatchBuilder::Build() {
  ARROW_ASSIGN_OR_RAISE(auto TABLE_QUALIFIER_Array,
                        TABLE_QUALIFIER_Builder.Finish())
  ARROW_ASSIGN_OR_RAISE(auto TABLE_OWNER_Array, TABLE_OWNER_Builder.Finish())
  ARROW_ASSIGN_OR_RAISE(auto TABLE_NAME_Array, TABLE_NAME_Builder.Finish())
  ARROW_ASSIGN_OR_RAISE(auto COLUMN_NAME_Array, COLUMN_NAME_Builder.Finish())
  ARROW_ASSIGN_OR_RAISE(auto DATA_TYPE_Array, DATA_TYPE_Builder.Finish())
  ARROW_ASSIGN_OR_RAISE(auto TYPE_NAME_Array, TYPE_NAME_Builder.Finish())
  ARROW_ASSIGN_OR_RAISE(auto PRECISION_Array, PRECISION_Builder.Finish())
  ARROW_ASSIGN_OR_RAISE(auto LENGTH_Array, LENGTH_Builder.Finish())
  ARROW_ASSIGN_OR_RAISE(auto SCALE_Array, SCALE_Builder.Finish())
  ARROW_ASSIGN_OR_RAISE(auto RADIX_Array, RADIX_Builder.Finish())
  ARROW_ASSIGN_OR_RAISE(auto NULLABLE_Array, NULLABLE_Builder.Finish())
  ARROW_ASSIGN_OR_RAISE(auto REMARKS_Array, REMARKS_Builder.Finish())

  std::vector<std::shared_ptr<Array>> arrays = {
      TABLE_QUALIFIER_Array, TABLE_OWNER_Array, TABLE_NAME_Array,
      COLUMN_NAME_Array,     DATA_TYPE_Array,   TYPE_NAME_Array,
      PRECISION_Array,       LENGTH_Array,      SCALE_Array,
      RADIX_Array,           NULLABLE_Array,    REMARKS_Array};

  return RecordBatch::Make(GetColumns_V2_Schema(), num_rows, arrays);
}

Status GetColumns_V2_RecordBatchBuilder::Append(
    const GetColumns_V2_RecordBatchBuilder::Data &data) {
  ARROW_RETURN_NOT_OK(
      AppendToBuilder(TABLE_QUALIFIER_Builder, data.table_qualifier));
  ARROW_RETURN_NOT_OK(AppendToBuilder(TABLE_OWNER_Builder, data.table_owner));
  ARROW_RETURN_NOT_OK(AppendToBuilder(TABLE_NAME_Builder, data.table_name));
  ARROW_RETURN_NOT_OK(AppendToBuilder(COLUMN_NAME_Builder, data.column_name));
  ARROW_RETURN_NOT_OK(AppendToBuilder(DATA_TYPE_Builder, data.data_type));
  ARROW_RETURN_NOT_OK(AppendToBuilder(TYPE_NAME_Builder, data.type_name));
  ARROW_RETURN_NOT_OK(AppendToBuilder(PRECISION_Builder, data.precision));
  ARROW_RETURN_NOT_OK(AppendToBuilder(LENGTH_Builder, data.length));
  ARROW_RETURN_NOT_OK(AppendToBuilder(SCALE_Builder, data.scale));
  ARROW_RETURN_NOT_OK(AppendToBuilder(RADIX_Builder, data.radix));
  ARROW_RETURN_NOT_OK(AppendToBuilder(NULLABLE_Builder, data.nullable));
  ARROW_RETURN_NOT_OK(AppendToBuilder(REMARKS_Builder, data.remarks));
  num_rows++;

  return Status::OK();
}

GetColumns_V2_Transformer::GetColumns_V2_Transformer(
    const std::string *column_name_pattern)
    : column_name_pattern_(
          column_name_pattern ? make_optional(*column_name_pattern) : nullopt) {
}

std::shared_ptr<RecordBatch> GetColumns_V2_Transformer::Transform(
    const std::shared_ptr<RecordBatch> &original) {
  const Result<std::shared_ptr<RecordBatch>> &result =
      Transform_inner(original, column_name_pattern_);
  ThrowIfNotOK(result.status());

  return result.ValueOrDie();
}

std::shared_ptr<Schema> GetColumns_V2_Transformer::GetTransformedSchema() {
  return GetColumns_V2_Schema();
}

} // namespace flight_sql
} // namespace driver

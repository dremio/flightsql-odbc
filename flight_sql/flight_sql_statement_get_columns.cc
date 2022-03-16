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

#include "flight_sql_statement_get_columns.h"
#include "flight_sql_get_tables_reader.h"
#include "utils.h"

namespace driver {
namespace flight_sql {

using arrow::util::make_optional;
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
      field("COLUMN_DEF", utf8()),
      field("SQL_DATA_TYPE", int16()),
      field("SQL_DATETIME_SUB", int16()),
      field("CHAR_OCTET_LENGTH", int32()),
      field("ORDINAL_POSITION", int32()),
      field("IS_NULLABLE", int32()),
  });
}

Result<std::shared_ptr<RecordBatch>>
Transform_inner(const odbcabstraction::OdbcVersion odbc_version,
                const std::shared_ptr<RecordBatch> &original,
                const optional<std::string> &column_name_pattern) {
  GetColumns_RecordBatchBuilder builder(odbc_version);
  GetColumns_RecordBatchBuilder::Data data;

  GetTablesReader reader(original);

  optional<boost::xpressive::sregex> column_name_regex =
      column_name_pattern.has_value() ? make_optional(ConvertSqlPatternToRegex(
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
          !boost::xpressive::regex_match(field->name(),
                                         column_name_regex.value())) {
        continue;
      }

      odbcabstraction::SqlDataType data_type_v3 =
          GetDataTypeFromArrowField_V3(field);

      data.table_cat = table_catalog;
      data.table_schem = table_schema;
      data.table_name = table_name;
      data.column_name = field->name();
      data.data_type = odbc_version == odbcabstraction::V_3
                           ? data_type_v3
                           : GetDataTypeFromArrowField_V2(field);
      data.type_name = GetTypeNameFromSqlDataType(data_type_v3);
      // TODO: Get from field's metadata "ARROW:FLIGHT:SQL:PRECISION"
      data.column_size = nullopt;
      // TODO: Get from column_size
      data.buffer_length = nullopt;
      // TODO: Get from field's metadata "ARROW:FLIGHT:SQL:SCALE"
      data.decimal_digits = nullopt;
      data.num_prec_radix = GetRadixFromSqlDataType(data_type_v3);
      data.nullable = field->nullable();
      data.remarks = nullopt;
      data.column_def = nullopt;
      data.sql_data_type = GetNonConciseDataType(data_type_v3);
      data.sql_datetime_sub = GetSqlDateTimeSubCode(data_type_v3);
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

GetColumns_RecordBatchBuilder::GetColumns_RecordBatchBuilder(
    odbcabstraction::OdbcVersion odbc_version)
    : odbc_version_(odbc_version) {}

Result<std::shared_ptr<RecordBatch>> GetColumns_RecordBatchBuilder::Build() {
  ARROW_ASSIGN_OR_RAISE(auto TABLE_CAT_Array, TABLE_CAT_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto TABLE_SCHEM_Array, TABLE_SCHEM_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto TABLE_NAME_Array, TABLE_NAME_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto COLUMN_NAME_Array, COLUMN_NAME_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto DATA_TYPE_Array, DATA_TYPE_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto TYPE_NAME_Array, TYPE_NAME_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto COLUMN_SIZE_Array, COLUMN_SIZE_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto BUFFER_LENGTH_Array,
                        BUFFER_LENGTH_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto DECIMAL_DIGITS_Array,
                        DECIMAL_DIGITS_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto NUM_PREC_RADIX_Array,
                        NUM_PREC_RADIX_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto NULLABLE_Array, NULLABLE_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto REMARKS_Array, REMARKS_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto COLUMN_DEF_Array, COLUMN_DEF_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto SQL_DATA_TYPE_Array,
                        SQL_DATA_TYPE_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto SQL_DATETIME_SUB_Array,
                        SQL_DATETIME_SUB_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto CHAR_OCTET_LENGTH_Array,
                        CHAR_OCTET_LENGTH_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto ORDINAL_POSITION_Array,
                        ORDINAL_POSITION_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto IS_NULLABLE_Array, IS_NULLABLE_Builder_.Finish())

  std::vector<std::shared_ptr<Array>> arrays = {
      TABLE_CAT_Array,         TABLE_SCHEM_Array,      TABLE_NAME_Array,
      COLUMN_NAME_Array,       DATA_TYPE_Array,        TYPE_NAME_Array,
      COLUMN_SIZE_Array,       BUFFER_LENGTH_Array,    DECIMAL_DIGITS_Array,
      NUM_PREC_RADIX_Array,    NULLABLE_Array,         REMARKS_Array,
      COLUMN_DEF_Array,        SQL_DATA_TYPE_Array,    SQL_DATETIME_SUB_Array,
      CHAR_OCTET_LENGTH_Array, ORDINAL_POSITION_Array, IS_NULLABLE_Array};

  const std::shared_ptr<Schema> &schema = odbc_version_ == odbcabstraction::V_3
                                              ? GetColumns_V3_Schema()
                                              : GetColumns_V2_Schema();
  return RecordBatch::Make(schema, num_rows_, arrays);
}

Status GetColumns_RecordBatchBuilder::Append(
    const GetColumns_RecordBatchBuilder::Data &data) {
  ARROW_RETURN_NOT_OK(AppendToBuilder(TABLE_CAT_Builder_, data.table_cat));
  ARROW_RETURN_NOT_OK(AppendToBuilder(TABLE_SCHEM_Builder_, data.table_schem));
  ARROW_RETURN_NOT_OK(AppendToBuilder(TABLE_NAME_Builder_, data.table_name));
  ARROW_RETURN_NOT_OK(AppendToBuilder(COLUMN_NAME_Builder_, data.column_name));
  ARROW_RETURN_NOT_OK(AppendToBuilder(DATA_TYPE_Builder_, data.data_type));
  ARROW_RETURN_NOT_OK(AppendToBuilder(TYPE_NAME_Builder_, data.type_name));
  ARROW_RETURN_NOT_OK(AppendToBuilder(COLUMN_SIZE_Builder_, data.column_size));
  ARROW_RETURN_NOT_OK(
      AppendToBuilder(BUFFER_LENGTH_Builder_, data.buffer_length));
  ARROW_RETURN_NOT_OK(
      AppendToBuilder(DECIMAL_DIGITS_Builder_, data.decimal_digits));
  ARROW_RETURN_NOT_OK(
      AppendToBuilder(NUM_PREC_RADIX_Builder_, data.num_prec_radix));
  ARROW_RETURN_NOT_OK(AppendToBuilder(NULLABLE_Builder_, data.nullable));
  ARROW_RETURN_NOT_OK(AppendToBuilder(REMARKS_Builder_, data.remarks));
  ARROW_RETURN_NOT_OK(AppendToBuilder(COLUMN_DEF_Builder_, data.column_def));
  ARROW_RETURN_NOT_OK(
      AppendToBuilder(SQL_DATA_TYPE_Builder_, data.sql_data_type));
  ARROW_RETURN_NOT_OK(
      AppendToBuilder(SQL_DATETIME_SUB_Builder_, data.sql_datetime_sub));
  ARROW_RETURN_NOT_OK(
      AppendToBuilder(CHAR_OCTET_LENGTH_Builder_, data.char_octet_length));
  ARROW_RETURN_NOT_OK(
      AppendToBuilder(ORDINAL_POSITION_Builder_, data.ordinal_position));
  ARROW_RETURN_NOT_OK(AppendToBuilder(IS_NULLABLE_Builder_, data.is_nullable));
  num_rows_++;

  return Status::OK();
}

GetColumns_Transformer::GetColumns_Transformer(
    const odbcabstraction::OdbcVersion odbc_version,
    const std::string *column_name_pattern)
    : odbc_version_(odbc_version),
      column_name_pattern_(
          column_name_pattern ? make_optional(*column_name_pattern) : nullopt) {
}

std::shared_ptr<RecordBatch> GetColumns_Transformer::Transform(
    const std::shared_ptr<RecordBatch> &original) {
  const Result<std::shared_ptr<RecordBatch>> &result =
      Transform_inner(odbc_version_, original, column_name_pattern_);
  ThrowIfNotOK(result.status());

  return result.ValueOrDie();
}

std::shared_ptr<Schema> GetColumns_Transformer::GetTransformedSchema() {
  return odbc_version_ == odbcabstraction::V_3 ? GetColumns_V3_Schema()
                                               : GetColumns_V2_Schema();
}

} // namespace flight_sql
} // namespace driver

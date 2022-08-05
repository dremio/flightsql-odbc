/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include "flight_sql_statement_get_type_info.h"
#include <odbcabstraction/platform.h>
#include "flight_sql_get_type_info_reader.h"
#include "flight_sql_connection.h"
#include "utils.h"
#include "odbcabstraction/logger.h"
#include <boost/algorithm/string/join.hpp>

namespace driver {
namespace flight_sql {

using arrow::util::make_optional;
using arrow::util::nullopt;
using arrow::util::optional;

namespace {
std::shared_ptr<Schema> GetTypeInfo_V3_Schema() {
  return schema({
      field("TYPE_NAME", utf8(), false),
      field("DATA_TYPE", int16(), false),
      field("COLUMN_SIZE", int32()),
      field("LITERAL_PREFIX", utf8()),
      field("LITERAL_SUFFIX", utf8()),
      field("CREATE_PARAMS", utf8()),
      field("NULLABLE", int16(), false),
      field("CASE_SENSITIVE", int16(), false),
      field("SEARCHABLE", int16(), false),
      field("UNSIGNED_ATTRIBUTE", int16()),
      field("FIXED_PREC_SCALE", int16(), false),
      field("AUTO_UNIQUE_VALUE", int16()),
      field("LOCAL_TYPE_NAME", utf8()),
      field("MINIMUM_SCALE", int16()),
      field("MAXIMUM_SCALE", int16()),
      field("SQL_DATA_TYPE", int16(), false),
      field("SQL_DATETIME_SUB", int16()),
      field("NUM_PREC_RADIX", int32()),
      field("INTERVAL_PRECISION", int16()),
  });
}

std::shared_ptr<Schema> GetTypeInfo_V2_Schema() {
  return schema({
      field("TYPE_NAME", utf8(), false),
      field("DATA_TYPE", int16(), false),
      field("PRECISION", int32()),
      field("LITERAL_PREFIX", utf8()),
      field("LITERAL_SUFFIX", utf8()),
      field("CREATE_PARAMS", utf8()),
      field("NULLABLE", int16(), false),
      field("CASE_SENSITIVE", int16(), false),
      field("SEARCHABLE", int16(), false),
      field("UNSIGNED_ATTRIBUTE", int16()),
      field("MONEY", int16(), false),
      field("AUTO_INCREMENT", int16()),
      field("LOCAL_TYPE_NAME", utf8()),
      field("MINIMUM_SCALE", int16()),
      field("MAXIMUM_SCALE", int16()),
      field("SQL_DATA_TYPE", int16(), false),
      field("SQL_DATETIME_SUB", int16()),
      field("NUM_PREC_RADIX", int32()),
      field("INTERVAL_PRECISION", int16()),
  });
}

Result<std::shared_ptr<RecordBatch>>
Transform_inner(const odbcabstraction::OdbcVersion odbc_version,
                const std::shared_ptr<RecordBatch> &original,
                int data_type,
                const MetadataSettings& metadata_settings_) {
  LOG_TRACE("[{}] Entry with parameters: data_type '{}'", __FUNCTION__, data_type)

  GetTypeInfo_RecordBatchBuilder builder(odbc_version);
  GetTypeInfo_RecordBatchBuilder::Data data;

  GetTypeInfoReader reader(original);

  while (reader.Next()) {
    auto data_type_v3 = EnsureRightSqlCharType(static_cast<odbcabstraction::SqlDataType>(reader.GetDataType()), metadata_settings_.use_wide_char_);
    int16_t data_type_v2 = ConvertSqlDataTypeFromV3ToV2(data_type_v3);

    if (data_type != odbcabstraction::ALL_TYPES && data_type_v3 != data_type && data_type_v2 != data_type) {
      continue;
    }

    data.data_type = odbc_version == odbcabstraction::V_3
                     ? data_type_v3
                     : data_type_v2;
    data.type_name = reader.GetTypeName();
    data.column_size = reader.GetColumnSize();
    data.literal_prefix = reader.GetLiteralPrefix();
    data.literal_suffix = reader.GetLiteralSuffix();

    const auto &create_params = reader.GetCreateParams();
    if (create_params) {
      data.create_params = boost::algorithm::join(*create_params, ",");
    } else {
      data.create_params = nullopt;
    }

    data.nullable = reader.GetNullable() ? odbcabstraction::NULLABILITY_NULLABLE : odbcabstraction::NULLABILITY_NO_NULLS;
    data.case_sensitive = reader.GetCaseSensitive();
    data.searchable = reader.GetSearchable() ? odbcabstraction::SEARCHABILITY_ALL : odbcabstraction::SEARCHABILITY_NONE;
    data.unsigned_attribute = reader.GetUnsignedAttribute();
    data.fixed_prec_scale = reader.GetFixedPrecScale();
    data.auto_unique_value = reader.GetAutoIncrement();
    data.local_type_name = reader.GetLocalTypeName();
    data.minimum_scale = reader.GetMinimumScale();
    data.maximum_scale = reader.GetMaximumScale();
    data.sql_data_type = EnsureRightSqlCharType(static_cast<odbcabstraction::SqlDataType>(reader.GetSqlDataType()), metadata_settings_.use_wide_char_);
    data.sql_datetime_sub = GetSqlDateTimeSubCode(static_cast<odbcabstraction::SqlDataType>(data.data_type));
    data.num_prec_radix = reader.GetNumPrecRadix();
    data.interval_precision = reader.GetIntervalPrecision();

    ARROW_RETURN_NOT_OK(builder.Append(data));
  }

  auto result = builder.Build();

  LOG_TRACE("[{}] Exiting successfully with RecordBatch", __FUNCTION__)
  return result;
}
} // namespace

GetTypeInfo_RecordBatchBuilder::GetTypeInfo_RecordBatchBuilder(
    odbcabstraction::OdbcVersion odbc_version)
    : odbc_version_(odbc_version) {}

Result<std::shared_ptr<RecordBatch>> GetTypeInfo_RecordBatchBuilder::Build() {
  LOG_TRACE("[{}] Entering function", __FUNCTION__)

  ARROW_ASSIGN_OR_RAISE(auto TYPE_NAME_Array, TYPE_NAME_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto DATA_TYPE_Array, DATA_TYPE_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto COLUMN_SIZE_Array, COLUMN_SIZE_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto LITERAL_PREFIX_Array, LITERAL_PREFIX_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto LITERAL_SUFFIX_Array, LITERAL_SUFFIX_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto CREATE_PARAMS_Array, CREATE_PARAMS_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto NULLABLE_Array, NULLABLE_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto CASE_SENSITIVE_Array, CASE_SENSITIVE_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto SEARCHABLE_Array, SEARCHABLE_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto UNSIGNED_ATTRIBUTE_Array, UNSIGNED_ATTRIBUTE_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto FIXED_PREC_SCALE_Array, FIXED_PREC_SCALE_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto AUTO_UNIQUE_VALUE_Array, AUTO_UNIQUE_VALUE_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto LOCAL_TYPE_NAME_Array, LOCAL_TYPE_NAME_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto MINIMUM_SCALE_Array, MINIMUM_SCALE_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto MAXIMUM_SCALE_Array, MAXIMUM_SCALE_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto SQL_DATA_TYPE_Array, SQL_DATA_TYPE_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto SQL_DATETIME_SUB_Array, SQL_DATETIME_SUB_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto NUM_PREC_RADIX_Array, NUM_PREC_RADIX_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto INTERVAL_PRECISION_Array, INTERVAL_PRECISION_Builder_.Finish())

  std::vector<std::shared_ptr<Array>> arrays = {
          TYPE_NAME_Array,
          DATA_TYPE_Array,
          COLUMN_SIZE_Array,
          LITERAL_PREFIX_Array,
          LITERAL_SUFFIX_Array,
          CREATE_PARAMS_Array,
          NULLABLE_Array,
          CASE_SENSITIVE_Array,
          SEARCHABLE_Array,
          UNSIGNED_ATTRIBUTE_Array,
          FIXED_PREC_SCALE_Array,
          AUTO_UNIQUE_VALUE_Array,
          LOCAL_TYPE_NAME_Array,
          MINIMUM_SCALE_Array,
          MAXIMUM_SCALE_Array,
          SQL_DATA_TYPE_Array,
          SQL_DATETIME_SUB_Array,
          NUM_PREC_RADIX_Array,
          INTERVAL_PRECISION_Array
  };

  const std::shared_ptr<Schema> &schema = odbc_version_ == odbcabstraction::V_3
                                              ? GetTypeInfo_V3_Schema()
                                              : GetTypeInfo_V2_Schema();

  auto return_ptr = RecordBatch::Make(schema, num_rows_, arrays);
  LOG_TRACE("[{}] Exiting successfully with RecordBatch", __FUNCTION__)
  return return_ptr;
}

Status GetTypeInfo_RecordBatchBuilder::Append(
    const GetTypeInfo_RecordBatchBuilder::Data &data) {
  LOG_TRACE("[{}] Entering function", __FUNCTION__)

  ARROW_RETURN_NOT_OK(AppendToBuilder(TYPE_NAME_Builder_, data.type_name));
  ARROW_RETURN_NOT_OK(AppendToBuilder(DATA_TYPE_Builder_, data.data_type));
  ARROW_RETURN_NOT_OK(AppendToBuilder(COLUMN_SIZE_Builder_, data.column_size));
  ARROW_RETURN_NOT_OK(AppendToBuilder(LITERAL_PREFIX_Builder_, data.literal_prefix));
  ARROW_RETURN_NOT_OK(AppendToBuilder(LITERAL_SUFFIX_Builder_, data.literal_suffix));
  ARROW_RETURN_NOT_OK(AppendToBuilder(CREATE_PARAMS_Builder_, data.create_params));
  ARROW_RETURN_NOT_OK(AppendToBuilder(NULLABLE_Builder_, data.nullable));
  ARROW_RETURN_NOT_OK(AppendToBuilder(CASE_SENSITIVE_Builder_, data.case_sensitive));
  ARROW_RETURN_NOT_OK(AppendToBuilder(SEARCHABLE_Builder_, data.searchable));
  ARROW_RETURN_NOT_OK(AppendToBuilder(UNSIGNED_ATTRIBUTE_Builder_, data.unsigned_attribute));
  ARROW_RETURN_NOT_OK(AppendToBuilder(FIXED_PREC_SCALE_Builder_, data.fixed_prec_scale));
  ARROW_RETURN_NOT_OK(AppendToBuilder(AUTO_UNIQUE_VALUE_Builder_, data.auto_unique_value));
  ARROW_RETURN_NOT_OK(AppendToBuilder(LOCAL_TYPE_NAME_Builder_, data.local_type_name));
  ARROW_RETURN_NOT_OK(AppendToBuilder(MINIMUM_SCALE_Builder_, data.minimum_scale));
  ARROW_RETURN_NOT_OK(AppendToBuilder(MAXIMUM_SCALE_Builder_, data.maximum_scale));
  ARROW_RETURN_NOT_OK(AppendToBuilder(SQL_DATA_TYPE_Builder_, data.sql_data_type));
  ARROW_RETURN_NOT_OK(AppendToBuilder(SQL_DATETIME_SUB_Builder_, data.sql_datetime_sub));
  ARROW_RETURN_NOT_OK(AppendToBuilder(NUM_PREC_RADIX_Builder_, data.num_prec_radix));
  ARROW_RETURN_NOT_OK(AppendToBuilder(INTERVAL_PRECISION_Builder_, data.interval_precision));
  num_rows_++;

  LOG_TRACE("[{}] Exiting successfully with Status::OK", __FUNCTION__)
  return Status::OK();
}

GetTypeInfo_Transformer::GetTypeInfo_Transformer(
    const MetadataSettings& metadata_settings,
    const odbcabstraction::OdbcVersion odbc_version,
    int data_type)
    : metadata_settings_(metadata_settings),
      odbc_version_(odbc_version),
      data_type_(data_type) {
}

std::shared_ptr<RecordBatch> GetTypeInfo_Transformer::Transform(
    const std::shared_ptr<RecordBatch> &original) {
  LOG_TRACE("[{}] Entering function", __FUNCTION__)

  const Result<std::shared_ptr<RecordBatch>> &result =
      Transform_inner(odbc_version_, original, data_type_, metadata_settings_);
  ThrowIfNotOK(result.status());

  auto return_ptr = result.ValueOrDie();
  LOG_TRACE("[{}] Exiting successfully with RecordBatch", __FUNCTION__)
  return return_ptr;
}

std::shared_ptr<Schema> GetTypeInfo_Transformer::GetTransformedSchema() {
  return odbc_version_ == odbcabstraction::V_3 ? GetTypeInfo_V3_Schema()
                                               : GetTypeInfo_V2_Schema();
}

} // namespace flight_sql
} // namespace driver

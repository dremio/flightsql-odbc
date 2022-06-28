/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include "record_batch_transformer.h"
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/status.h>
#include <arrow/util/optional.h>
#include <odbcabstraction/types.h>

namespace driver {
namespace flight_sql {

using odbcabstraction::MetadataSettings;
using arrow::util::optional;

class GetTypeInfo_RecordBatchBuilder {
private:
  odbcabstraction::OdbcVersion odbc_version_;

  StringBuilder TYPE_NAME_Builder_;
  Int16Builder DATA_TYPE_Builder_;
  Int32Builder COLUMN_SIZE_Builder_;
  StringBuilder LITERAL_PREFIX_Builder_;
  StringBuilder LITERAL_SUFFIX_Builder_;
  StringBuilder CREATE_PARAMS_Builder_;
  Int16Builder NULLABLE_Builder_;
  Int16Builder CASE_SENSITIVE_Builder_;
  Int16Builder SEARCHABLE_Builder_;
  Int16Builder UNSIGNED_ATTRIBUTE_Builder_;
  Int16Builder FIXED_PREC_SCALE_Builder_;
  Int16Builder AUTO_UNIQUE_VALUE_Builder_;
  StringBuilder LOCAL_TYPE_NAME_Builder_;
  Int16Builder MINIMUM_SCALE_Builder_;
  Int16Builder MAXIMUM_SCALE_Builder_;
  Int16Builder SQL_DATA_TYPE_Builder_;
  Int16Builder SQL_DATETIME_SUB_Builder_;
  Int32Builder NUM_PREC_RADIX_Builder_;
  Int16Builder INTERVAL_PRECISION_Builder_;
  int64_t num_rows_{0};

public:
  struct Data {
    std::string type_name;
    int16_t data_type;
    optional<int32_t> column_size;
    optional<std::string> literal_prefix;
    optional<std::string> literal_suffix;
    optional<std::string> create_params;
    int16_t nullable;
    int16_t case_sensitive;
    int16_t searchable;
    optional<int16_t> unsigned_attribute;
    int16_t fixed_prec_scale;
    optional<int16_t> auto_unique_value;
    optional<std::string> local_type_name;
    optional<int16_t> minimum_scale;
    optional<int16_t> maximum_scale;
    int16_t sql_data_type;
    optional<int16_t> sql_datetime_sub;
    optional<int32_t> num_prec_radix;
    optional<int16_t> interval_precision;
  };

  explicit GetTypeInfo_RecordBatchBuilder(
          odbcabstraction::OdbcVersion odbc_version);

  Result<std::shared_ptr<RecordBatch>> Build();

  Status Append(const Data &data);
};

class GetTypeInfo_Transformer : public RecordBatchTransformer {
private:
  const MetadataSettings& metadata_settings_;
  odbcabstraction::OdbcVersion odbc_version_;
  int data_type_;

public:
  explicit GetTypeInfo_Transformer(const MetadataSettings& metadata_settings,
                                   odbcabstraction::OdbcVersion odbc_version,
                                   int data_type);

  std::shared_ptr<RecordBatch>
  Transform(const std::shared_ptr<RecordBatch> &original) override;

  std::shared_ptr<Schema> GetTransformedSchema() override;
};

} // namespace flight_sql
} // namespace driver

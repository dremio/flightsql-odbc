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

using arrow::util::optional;

class GetColumns_RecordBatchBuilder {
private:
  odbcabstraction::OdbcVersion odbc_version_;

  StringBuilder TABLE_CAT_Builder_;
  StringBuilder TABLE_SCHEM_Builder_;
  StringBuilder TABLE_NAME_Builder_;
  StringBuilder COLUMN_NAME_Builder_;
  Int16Builder DATA_TYPE_Builder_;
  StringBuilder TYPE_NAME_Builder_;
  Int32Builder COLUMN_SIZE_Builder_;
  Int32Builder BUFFER_LENGTH_Builder_;
  Int16Builder DECIMAL_DIGITS_Builder_;
  Int16Builder NUM_PREC_RADIX_Builder_;
  Int16Builder NULLABLE_Builder_;
  StringBuilder REMARKS_Builder_;
  StringBuilder COLUMN_DEF_Builder_;
  Int16Builder SQL_DATA_TYPE_Builder_;
  Int16Builder SQL_DATETIME_SUB_Builder_;
  Int32Builder CHAR_OCTET_LENGTH_Builder_;
  Int32Builder ORDINAL_POSITION_Builder_;
  StringBuilder IS_NULLABLE_Builder_;
  int64_t num_rows_{0};

public:
  struct Data {
    optional<std::string> table_cat;
    optional<std::string> table_schem;
    std::string table_name;
    std::string column_name;
    std::string type_name;
    optional<int32_t> column_size;
    optional<int32_t> buffer_length;
    optional<int16_t> decimal_digits;
    optional<int16_t> num_prec_radix;
    optional<std::string> remarks;
    optional<std::string> column_def;
    int16_t sql_data_type{};
    optional<int16_t> sql_datetime_sub;
    optional<int32_t> char_octet_length;
    optional<std::string> is_nullable;
    int16_t data_type;
    int16_t nullable;
    int32_t ordinal_position;
  };

  explicit GetColumns_RecordBatchBuilder(
      odbcabstraction::OdbcVersion odbc_version);

  Result<std::shared_ptr<RecordBatch>> Build();

  Status Append(const Data &data);
};

class GetColumns_Transformer : public RecordBatchTransformer {
private:
  odbcabstraction::OdbcVersion odbc_version_;
  optional<std::string> column_name_pattern_;

public:
  explicit GetColumns_Transformer(odbcabstraction::OdbcVersion odbc_version_,
                                  const std::string *column_name_pattern);

  std::shared_ptr<RecordBatch>
  Transform(const std::shared_ptr<RecordBatch> &original) override;

  std::shared_ptr<Schema> GetTransformedSchema() override;
};

} // namespace flight_sql
} // namespace driver

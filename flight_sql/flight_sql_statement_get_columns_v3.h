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

#include "record_batch_transformer.h"
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/status.h>
#include <arrow/util/optional.h>

namespace driver {
namespace flight_sql {

using namespace arrow;
using arrow::util::optional;

class GetColumns_V3_RecordBatchBuilder {
private:
  StringBuilder TABLE_CAT_Builder;
  StringBuilder TABLE_SCHEM_Builder;
  StringBuilder TABLE_NAME_Builder;
  StringBuilder COLUMN_NAME_Builder;
  Int16Builder DATA_TYPE_Builder;
  StringBuilder TYPE_NAME_Builder;
  Int32Builder COLUMN_SIZE_Builder;
  Int32Builder BUFFER_LENGTH_Builder;
  Int16Builder DECIMAL_DIGITS_Builder;
  Int16Builder NUM_PREC_RADIX_Builder;
  Int16Builder NULLABLE_Builder;
  StringBuilder REMARKS_Builder;
  StringBuilder COLUMN_DEF_Builder;
  Int16Builder SQL_DATA_TYPE_Builder;
  Int16Builder SQL_DATETIME_SUB_Builder;
  Int32Builder CHAR_OCTET_LENGTH_Builder;
  Int32Builder ORDINAL_POSITION_Builder;
  StringBuilder IS_NULLABLE_Builder;
  int64_t num_rows{0};

public:
  struct Data {
    optional<std::string> table_cat;
    optional<std::string> table_schem;
    std::string table_name;
    std::string column_name;
    int16_t data_type{};
    std::string type_name;
    optional<int32_t> column_size;
    optional<int32_t> buffer_length;
    optional<int16_t> decimal_digits;
    optional<int16_t> num_prec_radix;
    int16_t nullable{};
    optional<std::string> remarks;
    optional<std::string> column_def;
    int16_t sql_data_type{};
    optional<int16_t> sql_datetime_sub;
    optional<int32_t> char_octet_length;
    int32_t ordinal_position{};
    optional<std::string> is_nullable;
  };

  Result<std::shared_ptr<RecordBatch>> Build();

  Status Append(const Data &data);
};

class GetColumns_V3_Transformer : public RecordBatchTransformer {
public:
  virtual ~GetColumns_V3_Transformer() = default;

  std::shared_ptr<RecordBatch>
  Transform(const std::shared_ptr<RecordBatch> &original) override;

  std::shared_ptr<Schema> GetTransformedSchema() override;
};

} // namespace flight_sql
} // namespace driver

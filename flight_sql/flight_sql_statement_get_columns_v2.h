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

class GetColumns_V2_RecordBatchBuilder {
private:
  StringBuilder TABLE_QUALIFIER_Builder;
  StringBuilder TABLE_OWNER_Builder;
  StringBuilder TABLE_NAME_Builder;
  StringBuilder COLUMN_NAME_Builder;
  Int16Builder DATA_TYPE_Builder;
  StringBuilder TYPE_NAME_Builder;
  Int32Builder PRECISION_Builder;
  Int32Builder LENGTH_Builder;
  Int16Builder SCALE_Builder;
  Int16Builder RADIX_Builder;
  Int16Builder NULLABLE_Builder;
  StringBuilder REMARKS_Builder;
  int64_t num_rows{0};

public:
  struct Data {
    optional<std::string> table_qualifier;
    optional<std::string> table_owner;
    std::string table_name;
    std::string column_name;
    int16_t data_type;
    std::string type_name;
    optional<int32_t> precision;
    optional<int32_t> length;
    optional<int16_t> scale;
    optional<int16_t> radix;
    int16_t nullable;
    optional<std::string> remarks;
  };

  Result<std::shared_ptr<RecordBatch>> Build();

  Status Append(const Data &data);
};

class GetColumns_V2_Transformer : public RecordBatchTransformer {
public:
  virtual ~GetColumns_V2_Transformer() = default;

  std::shared_ptr<RecordBatch>
  Transform(const std::shared_ptr<RecordBatch> &original) override;

  std::shared_ptr<Schema> GetTransformedSchema() override;
};

} // namespace flight_sql
} // namespace driver

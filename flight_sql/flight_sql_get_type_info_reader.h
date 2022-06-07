/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include "record_batch_transformer.h"
#include <arrow/util/optional.h>

namespace driver {
namespace flight_sql {

using namespace arrow;
using arrow::util::optional;

class GetTypeInfoReader {
private:
  std::shared_ptr<RecordBatch> record_batch_;
  int64_t current_row_;

public:
  explicit GetTypeInfoReader(std::shared_ptr<RecordBatch> record_batch);

  bool Next();

  std::string GetTypeName();

  int32_t GetDataType();

  optional<int32_t> GetColumnSize();

  optional<std::string> GetLiteralPrefix();

  optional<std::string> GetLiteralSuffix();

  optional<std::vector<std::string>> GetCreateParams();

  int32_t GetNullable();

  bool GetCaseSensitive();

  int32_t GetSearchable();

  optional<bool> GetUnsignedAttribute();

  bool GetFixedPrecScale();

  optional<bool> GetAutoIncrement();

  optional<std::string> GetLocalTypeName();

  optional<int32_t> GetMinimumScale();

  optional<int32_t> GetMaximumScale();

  int32_t GetSqlDataType();

  optional<int32_t> GetDatetimeSubcode();

  optional<int32_t> GetNumPrecRadix();

  optional<int32_t> GetIntervalPrecision();

};

} // namespace flight_sql
} // namespace driver

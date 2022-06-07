/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include "flight_sql_get_type_info_reader.h"
#include <odbcabstraction/platform.h>
#include <arrow/io/memory.h>
#include <arrow/array.h>
#include "utils.h"

#include <utility>

namespace driver {
namespace flight_sql {

using arrow::internal::checked_pointer_cast;
using arrow::util::nullopt;

GetTypeInfoReader::GetTypeInfoReader(std::shared_ptr<RecordBatch> record_batch)
    : record_batch_(std::move(record_batch)), current_row_(-1) {}

bool GetTypeInfoReader::Next() {
  return ++current_row_ < record_batch_->num_rows();
}

std::string GetTypeInfoReader::GetTypeName() {
  const auto &array =
          checked_pointer_cast<StringArray>(record_batch_->column(0));

  return array->GetString(current_row_);
}

int32_t GetTypeInfoReader::GetDataType() {
  const auto &array =
          checked_pointer_cast<Int32Array>(record_batch_->column(1));

  return array->GetView(current_row_);
}

optional<int32_t> GetTypeInfoReader::GetColumnSize() {
  const auto &array =
          checked_pointer_cast<Int32Array>(record_batch_->column(2));

  if (array->IsNull(current_row_))
    return nullopt;

  return array->GetView(current_row_);
}

optional<std::string> GetTypeInfoReader::GetLiteralPrefix() {
  const auto &array =
          checked_pointer_cast<StringArray>(record_batch_->column(3));

  if (array->IsNull(current_row_))
    return nullopt;

  return array->GetString(current_row_);
}

optional<std::string> GetTypeInfoReader::GetLiteralSuffix() {
  const auto &array =
          checked_pointer_cast<StringArray>(record_batch_->column(4));

  if (array->IsNull(current_row_))
    return nullopt;

  return array->GetString(current_row_);
}

optional<std::vector<std::string>> GetTypeInfoReader::GetCreateParams() {
  const auto &array =
          checked_pointer_cast<ListArray>(record_batch_->column(5));

  if (array->IsNull(current_row_))
    return nullopt;

  int values_length = array->value_length(current_row_);
  int start_offset = array->value_offset(current_row_);
  const auto &values_array = checked_pointer_cast<StringArray>(array->values());

  std::vector<std::string> result(values_length);
  for (int i = 0; i < values_length; ++i) {
    result[i] = values_array->GetString(start_offset + i);
  }

  return result;
}

int32_t GetTypeInfoReader::GetNullable() {
  const auto &array =
          checked_pointer_cast<Int32Array>(record_batch_->column(6));

  return array->GetView(current_row_);
}

bool GetTypeInfoReader::GetCaseSensitive() {
  const auto &array =
          checked_pointer_cast<BooleanArray>(record_batch_->column(7));

  return array->GetView(current_row_);
}

int32_t GetTypeInfoReader::GetSearchable() {
  const auto &array =
          checked_pointer_cast<Int32Array>(record_batch_->column(8));

  return array->GetView(current_row_);
}

optional<bool> GetTypeInfoReader::GetUnsignedAttribute() {
  const auto &array =
          checked_pointer_cast<BooleanArray>(record_batch_->column(9));

  if (array->IsNull(current_row_))
    return nullopt;

  return array->GetView(current_row_);
}

bool GetTypeInfoReader::GetFixedPrecScale() {
  const auto &array =
          checked_pointer_cast<BooleanArray>(record_batch_->column(10));

  return array->GetView(current_row_);
}

optional<bool> GetTypeInfoReader::GetAutoIncrement() {
  const auto &array =
          checked_pointer_cast<BooleanArray>(record_batch_->column(11));

  if (array->IsNull(current_row_))
    return nullopt;

  return array->GetView(current_row_);
}

optional<std::string> GetTypeInfoReader::GetLocalTypeName() {
  const auto &array =
          checked_pointer_cast<StringArray>(record_batch_->column(12));

  if (array->IsNull(current_row_))
    return nullopt;

  return array->GetString(current_row_);
}

optional<int32_t> GetTypeInfoReader::GetMinimumScale() {
  const auto &array =
          checked_pointer_cast<Int32Array>(record_batch_->column(13));

  if (array->IsNull(current_row_))
    return nullopt;

  return array->GetView(current_row_);
}

optional<int32_t> GetTypeInfoReader::GetMaximumScale() {
  const auto &array =
          checked_pointer_cast<Int32Array>(record_batch_->column(14));

  if (array->IsNull(current_row_))
    return nullopt;

  return array->GetView(current_row_);
}

int32_t GetTypeInfoReader::GetSqlDataType() {
  const auto &array =
          checked_pointer_cast<Int32Array>(record_batch_->column(15));

  return array->GetView(current_row_);
}

optional<int32_t> GetTypeInfoReader::GetDatetimeSubcode() {
  const auto &array =
          checked_pointer_cast<Int32Array>(record_batch_->column(16));

  if (array->IsNull(current_row_))
    return nullopt;

  return array->GetView(current_row_);
}

optional<int32_t> GetTypeInfoReader::GetNumPrecRadix() {
  const auto &array =
          checked_pointer_cast<Int32Array>(record_batch_->column(17));

  if (array->IsNull(current_row_))
    return nullopt;

  return array->GetView(current_row_);
}

optional<int32_t> GetTypeInfoReader::GetIntervalPrecision() {
  const auto &array =
          checked_pointer_cast<Int32Array>(record_batch_->column(18));

  if (array->IsNull(current_row_))
    return nullopt;

  return array->GetView(current_row_);
}

} // namespace flight_sql
} // namespace driver

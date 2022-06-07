/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include "flight_sql_get_tables_reader.h"
#include <odbcabstraction/platform.h>
#include <arrow/io/memory.h>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/status.h>
#include "utils.h"

#include <utility>

namespace driver {
namespace flight_sql {

using arrow::internal::checked_pointer_cast;
using arrow::util::nullopt;

GetTablesReader::GetTablesReader(std::shared_ptr<RecordBatch> record_batch)
    : record_batch_(std::move(record_batch)), current_row_(-1) {}

bool GetTablesReader::Next() {
  return ++current_row_ < record_batch_->num_rows();
}

optional<std::string> GetTablesReader::GetCatalogName() {
  const auto &array =
      checked_pointer_cast<StringArray>(record_batch_->column(0));

  if (array->IsNull(current_row_))
    return nullopt;

  return array->GetString(current_row_);
}

optional<std::string> GetTablesReader::GetDbSchemaName() {
  const auto &array =
      checked_pointer_cast<StringArray>(record_batch_->column(1));

  if (array->IsNull(current_row_))
    return nullopt;

  return array->GetString(current_row_);
}

std::string GetTablesReader::GetTableName() {
  const auto &array =
      checked_pointer_cast<StringArray>(record_batch_->column(2));

  return array->GetString(current_row_);
}

std::string GetTablesReader::GetTableType() {
  const auto &array =
      checked_pointer_cast<StringArray>(record_batch_->column(3));

  return array->GetString(current_row_);
}

std::shared_ptr<Schema> GetTablesReader::GetSchema() {
  const auto &array =
      checked_pointer_cast<BinaryArray>(record_batch_->column(4));
  if (array == nullptr) {
    return nullptr;
  }

  io::BufferReader dataset_schema_reader(array->GetView(current_row_));
  ipc::DictionaryMemo in_memo;
  const Result<std::shared_ptr<Schema>> &result =
      ReadSchema(&dataset_schema_reader, &in_memo);
  if (!result.ok()) {
    // TODO: Ignoring this error until we fix the problem on Dremio server
    // The problem is that complex types columns are being returned without the children types.
    return nullptr;
  }

  return result.ValueOrDie();
}

} // namespace flight_sql
} // namespace driver

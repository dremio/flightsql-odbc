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

class GetTablesReader {
private:
  std::shared_ptr<RecordBatch> record_batch_;
  int64_t current_row_;

public:
  explicit GetTablesReader(std::shared_ptr<RecordBatch> record_batch);

  bool Next();

  optional<std::string> GetCatalogName();

  optional<std::string> GetDbSchemaName();

  std::string GetTableName();

  std::string GetTableType();

  std::shared_ptr<Schema> GetSchema();
};

} // namespace flight_sql
} // namespace driver

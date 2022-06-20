/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include "flight_sql_result_set.h"
#include <odbcabstraction/platform.h>

#include <arrow/flight/types.h>
#include <arrow/scalar.h>
#include <utility>

#include "flight_sql_result_set_column.h"
#include "flight_sql_result_set_metadata.h"
#include "utils.h"
#include "odbcabstraction/types.h"

namespace driver {
namespace flight_sql {

using arrow::Array;
using arrow::RecordBatch;
using arrow::Scalar;
using arrow::Status;
using arrow::flight::FlightEndpoint;
using arrow::flight::FlightStreamChunk;
using arrow::flight::FlightStreamReader;
using odbcabstraction::CDataType;
using odbcabstraction::DriverException;

FlightSqlResultSet::FlightSqlResultSet(
    FlightSqlClient &flight_sql_client,
    const arrow::flight::FlightCallOptions &call_options,
    const std::shared_ptr<FlightInfo> &flight_info,
    const std::shared_ptr<RecordBatchTransformer> &transformer,
    odbcabstraction::Diagnostics& diagnostics)
    : chunk_iterator_(flight_sql_client, call_options, flight_info),
      transformer_(transformer),
      metadata_(transformer ? new FlightSqlResultSetMetadata(
                                  transformer->GetTransformedSchema())
                            : new FlightSqlResultSetMetadata(flight_info)),
      columns_(metadata_->GetColumnCount()),
      get_data_offsets_(metadata_->GetColumnCount(), 0),
      diagnostics_(diagnostics),
      current_row_(0), num_binding_(0), reset_get_data_(false) {
  current_chunk_.data = nullptr;

  for (int i = 0; i < columns_.size(); ++i) {
    columns_[i] = FlightSqlResultSetColumn(this, i + 1);
  }

  ThrowIfNotOK(flight_info->GetSchema(nullptr, &schema_));
}

size_t FlightSqlResultSet::Move(size_t rows, size_t bind_offset, uint16_t *row_status_array) {
  // Consider it might be the first call to Move() and current_chunk is not
  // populated yet
  assert(rows > 0);
  if (current_chunk_.data == nullptr) {
    if (!chunk_iterator_.GetNext(&current_chunk_)) {
      return 0;
    }

    if (transformer_) {
      current_chunk_.data = transformer_->Transform(current_chunk_.data);
    }
  }

  // Reset GetData value offsets.
  if (num_binding_ != get_data_offsets_.size() && reset_get_data_) {
    std::fill(get_data_offsets_.begin(), get_data_offsets_.end(), 0);
  }

  size_t fetched_rows = 0;
  while (fetched_rows < rows) {
    size_t batch_rows = current_chunk_.data->num_rows();
    size_t rows_to_fetch =
        std::min(static_cast<size_t>(rows - fetched_rows),
                 static_cast<size_t>(batch_rows - current_row_));

    if (rows_to_fetch == 0) {
      if (!chunk_iterator_.GetNext(&current_chunk_)) {
        break;
      }

      if (transformer_) {
        current_chunk_.data = transformer_->Transform(current_chunk_.data);
      }

      for (auto &column : columns_) {
        column.ResetAccessor();
      }
      current_row_ = 0;
      continue;
    }

    for (auto & column : columns_) {
      // There can be unbound columns.
      if (!column.is_bound)
        continue;

      auto *accessor = column.GetAccessorForBinding();
      ColumnBinding shifted_binding = column.binding;
      if (shifted_binding.buffer) {
        shifted_binding.buffer =
            static_cast<uint8_t *>(shifted_binding.buffer) +
            accessor->GetCellLength(&shifted_binding) * fetched_rows +
            bind_offset;
      }

      if (shifted_binding.strlen_buffer) {
        shifted_binding.strlen_buffer = reinterpret_cast<ssize_t *>(
            reinterpret_cast<uint8_t *>(
                &shifted_binding.strlen_buffer[fetched_rows]) +
            bind_offset);
      }
      uint16_t *shifted_row_status_array = row_status_array ? &row_status_array[fetched_rows] : nullptr;

      if (shifted_row_status_array) {
        std::fill(shifted_row_status_array, &shifted_row_status_array[rows_to_fetch], odbcabstraction::RowStatus_SUCCESS);
      }

      size_t accessor_rows;
      try {
        int64_t value_offset = 0;
        accessor_rows = accessor->GetColumnarData(&shifted_binding, current_row_, rows_to_fetch, value_offset, false,
                                                  diagnostics_, shifted_row_status_array);
      } catch (...) {
        if (shifted_row_status_array) {
          std::fill(shifted_row_status_array, &shifted_row_status_array[rows_to_fetch], odbcabstraction::RowStatus_ERROR);
        }
        throw;
      }

      if (rows_to_fetch != accessor_rows) {
        throw DriverException(
            "Expected the same number of rows for all columns");
      }
    }

    current_row_ += static_cast<int64_t>(rows_to_fetch);
    fetched_rows += rows_to_fetch;
  }

  if (rows > fetched_rows && row_status_array) {
    std::fill(&row_status_array[fetched_rows], &row_status_array[rows], odbcabstraction::RowStatus_NOROW);
  }
  return fetched_rows;
}

void FlightSqlResultSet::Close() {
  chunk_iterator_.Close();
  current_chunk_.data = nullptr;
}

void FlightSqlResultSet::Cancel() {
  chunk_iterator_.Close();
  current_chunk_.data = nullptr;
}

bool FlightSqlResultSet::GetData(int column_n, int16_t target_type,
                                 int precision, int scale, void *buffer,
                                 size_t buffer_length, ssize_t *strlen_buffer) {
  reset_get_data_ = true;
  // Check if the offset is already at the end.
  int64_t& value_offset = get_data_offsets_[column_n - 1];
  if (value_offset == -1) {
    return false;
  }
  
  ColumnBinding binding(ConvertCDataTypeFromV2ToV3(target_type), precision, scale, buffer, buffer_length,
                        strlen_buffer);

  auto &column = columns_[column_n - 1];
  Accessor *accessor = column.GetAccessorForGetData(binding.target_type);


  // Note: current_row_ is always positioned at the index _after_ the one we are
  // on after calling Move(). So if we want to get data from the _last_ row
  // fetched, we need to subtract one from the current row.
  // TODO: Should GetData update row status array?
  accessor->GetColumnarData(&binding, current_row_ - 1, 1, value_offset, true, diagnostics_, nullptr);

  // If there was truncation, the converter would have reported it to the diagnostics.
  return diagnostics_.HasWarning();
}

std::shared_ptr<arrow::Array>
FlightSqlResultSet::GetArrayForColumn(int column) {
  if (!current_chunk_.data) {
    // This may happen if query is cancelled right after SQLFetch and before SQLGetData.
    throw DriverException("No RecordBatch loaded.", "24000");
  }

  std::shared_ptr<Array> original_array =
      current_chunk_.data->column(column - 1);

  return original_array;
}

std::shared_ptr<ResultSetMetadata> FlightSqlResultSet::GetMetadata() {
  return metadata_;
}

void FlightSqlResultSet::BindColumn(int column_n, int16_t target_type,
                                    int precision, int scale, void *buffer,
                                    size_t buffer_length,
                                    ssize_t *strlen_buffer) {
  auto &column = columns_[column_n - 1];
  if (buffer == nullptr) {
    if (column.is_bound) {
      num_binding_--;
    }
    column.ResetBinding();
    return;
  }

  if (!column.is_bound) {
    num_binding_++;
  }

  ColumnBinding binding(ConvertCDataTypeFromV2ToV3(target_type), precision, scale, buffer, buffer_length,
                        strlen_buffer);
  column.SetBinding(binding);
}

FlightSqlResultSet::~FlightSqlResultSet() = default;
} // namespace flight_sql
} // namespace driver

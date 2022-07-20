/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include "flight_sql_stream_chunk_iterator.h"
#include <odbcabstraction/platform.h>
#include "utils.h"

namespace driver {
namespace flight_sql {

FlightStreamChunkIterator::FlightStreamChunkIterator(std::unique_ptr<FlightStreamReader> stream_reader)
    : stream_reader_(std::move(stream_reader)), closed_(false) {}

FlightStreamChunkIterator::~FlightStreamChunkIterator() { Close(); }

bool FlightStreamChunkIterator::GetNext(FlightStreamChunk *chunk) {
  if (closed_) return false;

  chunk->data = nullptr;
  const auto &chunk_result = stream_reader_->Next();
  ThrowIfNotOK(chunk_result.status());
  chunk->data = chunk_result.ValueOrDie().data;

  return chunk->data != nullptr;
}

void FlightStreamChunkIterator::Close() {
  if (closed_) {
    return;
  }
  stream_reader_->Cancel();
  closed_ = true;
}

} // namespace flight_sql
} // namespace driver

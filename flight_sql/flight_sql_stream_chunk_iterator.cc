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

using arrow::flight::FlightEndpoint;

FlightStreamChunkIterator::FlightStreamChunkIterator(
    FlightSqlClient &flight_sql_client,
    const arrow::flight::FlightCallOptions &call_options,
    const std::shared_ptr<FlightInfo> &flight_info)
    : closed_(false) {
  const std::vector<FlightEndpoint> &endpoints = flight_info->endpoints();

  stream_readers_.reserve(endpoints.size());
  for (int i = 0; i < endpoints.size(); ++i) {
    auto result = flight_sql_client.DoGet(call_options, endpoints[i].ticket);
    ThrowIfNotOK(result.status());
    stream_readers_.push_back(std::move(result.ValueOrDie()));
  }

  stream_readers_it_ = stream_readers_.begin();
}

FlightStreamChunkIterator::~FlightStreamChunkIterator() { Close(); }

bool FlightStreamChunkIterator::GetNext(FlightStreamChunk *chunk) {
  if (closed_) return false;

  chunk->data = nullptr;
  while (stream_readers_it_ != stream_readers_.end()) {
    const auto &chunk_result = (*stream_readers_it_)->Next();
    ThrowIfNotOK(chunk_result.status());
    chunk->data = chunk_result.ValueOrDie().data;
    if (chunk->data != nullptr) {
      return true;
    }
    ++stream_readers_it_;
  }
  return false;
}

void FlightStreamChunkIterator::Close() {
  if (closed_) {
    return;
  }
  for (const auto &item : stream_readers_) {
    item->Cancel();
  }
  closed_ = true;
}

} // namespace flight_sql
} // namespace driver

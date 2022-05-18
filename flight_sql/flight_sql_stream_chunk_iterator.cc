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
    stream_readers_it_++;
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

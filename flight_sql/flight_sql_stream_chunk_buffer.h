/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#pragma once

#include <mutex>
#include <condition_variable>
#include <thread>
#include <arrow/flight/types.h>

#include "flight_sql_stream_chunk_iterator.h"


namespace driver {
namespace flight_sql {

using arrow::flight::FlightStreamChunk;

#define BUFFER_CAPACITY 2

class FlightStreamChunkBuffer {
  std::vector<std::thread> threads_;
  FlightStreamChunkIterator iterator_;

  // Buffer fields
  FlightStreamChunk buffer[BUFFER_CAPACITY];
  int buffer_size{0};
  int left{0}; // index where variables are put inside of buffer (produced)
  int right{0}; // index where variables are removed from buffer (consumed)

  // Fields for concurrency
  std::mutex mtx;
  std::condition_variable not_empty;
  std::condition_variable not_full;

public:
  explicit FlightStreamChunkBuffer(FlightSqlClient &flight_sql_client,
                                   const arrow::flight::FlightCallOptions &call_options,
                                   const std::shared_ptr<FlightInfo> &flight_info);
  ~FlightStreamChunkBuffer();

  void Produce(FlightStreamChunk num);

  void Close();

  bool GetNext(FlightStreamChunk* chunk);

};

}
}

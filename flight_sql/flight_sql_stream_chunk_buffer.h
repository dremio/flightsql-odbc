/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#pragma once

#include <arrow/flight/types.h>
#include "flight_sql_stream_chunk_iterator.h"
#include "blocking_queue.h"


namespace driver {
namespace flight_sql {

using arrow::flight::FlightStreamChunk;

class FlightStreamChunkBuffer {
  BlockingQueue<FlightStreamChunk> queue_{10};
  std::vector<std::shared_ptr<FlightStreamChunkIterator>> iterators_;

public:
  explicit FlightStreamChunkBuffer(FlightSqlClient &flight_sql_client,
                                   const arrow::flight::FlightCallOptions &call_options,
                                   const std::shared_ptr<FlightInfo> &flight_info);
  ~FlightStreamChunkBuffer();

  void Close();

  bool GetNext(FlightStreamChunk* chunk);

};

}
}

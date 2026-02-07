/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#pragma once

#include <arrow/flight/client.h>
#include <arrow/flight/sql/client.h>
#include <odbcabstraction/blocking_queue.h>


namespace driver {
namespace flight_sql {

using arrow::Result;
using arrow::flight::FlightInfo;
using arrow::flight::FlightStreamChunk;
using arrow::flight::FlightStreamReader;
using arrow::flight::sql::FlightSqlClient;
using driver::odbcabstraction::BlockingQueue;

class FlightStreamChunkBuffer {
  BlockingQueue<Result<FlightStreamChunk>> queue_;

public:
  FlightStreamChunkBuffer(FlightSqlClient &flight_sql_client,
                          const arrow::flight::FlightCallOptions &call_options,
                          const std::shared_ptr<FlightInfo> &flight_info,
                          size_t queue_capacity = 5,
                          bool use_extended_flightsql_buffer = false);

  ~FlightStreamChunkBuffer();

  void Close();

  bool GetNext(FlightStreamChunk* chunk);

};

}
}

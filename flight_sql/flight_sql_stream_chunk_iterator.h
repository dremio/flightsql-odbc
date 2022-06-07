/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#pragma once

#include <arrow/flight/client.h>
#include <arrow/flight/sql/client.h>

namespace driver {
namespace flight_sql {

using arrow::flight::FlightInfo;
using arrow::flight::FlightStreamChunk;
using arrow::flight::FlightStreamReader;
using arrow::flight::sql::FlightSqlClient;

class FlightStreamChunkIterator {
private:
  std::vector<std::unique_ptr<FlightStreamReader>> stream_readers_;
  std::vector<std::unique_ptr<FlightStreamReader>>::iterator stream_readers_it_;
  bool closed_;

public:
  FlightStreamChunkIterator(
      FlightSqlClient &flight_sql_client,
      const arrow::flight::FlightCallOptions &call_options,
      const std::shared_ptr<FlightInfo> &flight_info);

  ~FlightStreamChunkIterator();

  bool GetNext(FlightStreamChunk *chunk);

  void Close();
};

} // namespace flight_sql
} // namespace driver

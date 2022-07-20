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
  std::unique_ptr<FlightStreamReader> stream_reader_;
  bool closed_;

public:
  explicit FlightStreamChunkIterator(std::unique_ptr<FlightStreamReader> stream_reader);

  ~FlightStreamChunkIterator();

  bool GetNext(FlightStreamChunk *chunk);

  void Close();
};

} // namespace flight_sql
} // namespace driver

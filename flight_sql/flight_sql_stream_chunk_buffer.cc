/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include "flight_sql_stream_chunk_buffer.h"
#include "utils.h"


namespace driver {
namespace flight_sql {

using arrow::flight::FlightEndpoint;

FlightStreamChunkBuffer::FlightStreamChunkBuffer(FlightSqlClient &flight_sql_client,
                                                 const arrow::flight::FlightCallOptions &call_options,
                                                 const std::shared_ptr<FlightInfo> &flight_info,
                                                 size_t queue_capacity,
                                                 bool use_extended_flightsql_buffer): queue_(queue_capacity, use_extended_flightsql_buffer) {

  // FIXME: Endpoint iteration should consider endpoints may be at different hosts
  for (const auto & endpoint : flight_info->endpoints()) {
    const arrow::flight::Ticket &ticket = endpoint.ticket;

    auto result = flight_sql_client.DoGet(call_options, ticket);
    ThrowIfNotOK(result.status());
    std::shared_ptr<FlightStreamReader> stream_reader_ptr(std::move(result.ValueOrDie()));

    BlockingQueue<Result<FlightStreamChunk>>::Supplier supplier = [=] {
      auto result = stream_reader_ptr->Next();
      bool isNotOk = !result.ok();
      bool isNotEmpty = result.ok() && (result.ValueOrDie().data != nullptr);

      return boost::make_optional(isNotOk || isNotEmpty, std::move(result));
    };
    queue_.AddProducer(std::move(supplier));
  }
}

bool FlightStreamChunkBuffer::GetNext(FlightStreamChunk *chunk) {
  Result<FlightStreamChunk> result;
  if (!queue_.Pop(&result)) {
    return false;
  }

  if (!result.status().ok()) {
    Close();
    throw odbcabstraction::DriverException(result.status().message());
  }
  *chunk = std::move(result.ValueOrDie());
  return chunk->data != nullptr;
}

void FlightStreamChunkBuffer::Close() {
  queue_.Close();
}

FlightStreamChunkBuffer::~FlightStreamChunkBuffer() {
  Close();
}

}
}

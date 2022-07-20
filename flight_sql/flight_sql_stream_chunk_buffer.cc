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
                                                 const std::shared_ptr<FlightInfo> &flight_info) {

  // FIXME: Endpoint iteration should consider endpoints may be at different hosts
  for (const auto & endpoint : flight_info->endpoints()) {
    const arrow::flight::Ticket &ticket = endpoint.ticket;

    auto result = flight_sql_client.DoGet(call_options, ticket);
    ThrowIfNotOK(result.status());
    auto stream_reader = std::move(result.ValueOrDie());
    std::shared_ptr<FlightStreamReader> stream_reader_ptr(std::move(stream_reader));
    iterators_.push_back(stream_reader_ptr);

    BlockingQueue<Result<FlightStreamChunk>>::Supplier supplier = [=] {
      return stream_reader_ptr->Next();
    };
    queue_.AddProducer(std::move(supplier));
  }
}

bool FlightStreamChunkBuffer::GetNext(FlightStreamChunk *chunk) {
  Result<FlightStreamChunk> result;
  if(!queue_.Consume(&result)) {
    return false;
  }

  ThrowIfNotOK(result.status());
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

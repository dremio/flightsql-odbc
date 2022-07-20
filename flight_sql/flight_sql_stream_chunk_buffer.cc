/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include "flight_sql_stream_chunk_buffer.h"
#include "utils.h"

#include <iostream>


namespace driver {
namespace flight_sql {

using arrow::flight::FlightEndpoint;

void FlightStreamChunkBuffer::Produce(FlightStreamChunk num) {

  // Acquire a unique lock on the mutex
  std::unique_lock<std::mutex> unique_lock(mtx);

  // Wait if the buffer is full
  not_full.wait(unique_lock, [this]() {
    return buffer_size != BUFFER_CAPACITY;
  });

  // Add input to buffer
  buffer[right] = num;

  // Update appropriate fields
  right = (right + 1) % BUFFER_CAPACITY;
  buffer_size++;

  // Unlock unique lock
  unique_lock.unlock();

  // Notify a single thread that buffer isn't empty
  not_empty.notify_one();
}

FlightStreamChunkBuffer::FlightStreamChunkBuffer(FlightSqlClient &flight_sql_client,
                                                 const arrow::flight::FlightCallOptions &call_options,
                                                 const std::shared_ptr<FlightInfo> &flight_info) {

  const std::vector<FlightEndpoint> &endpoints = flight_info->endpoints();

  for (int i = 0; i < endpoints.size(); ++i) {
    const arrow::flight::Ticket &ticket = endpoints[i].ticket;

    active_threads_++;
    threads_.emplace_back([&] {
      auto result = flight_sql_client.DoGet(call_options, ticket);
      ThrowIfNotOK(result.status());
      auto stream_reader = std::move(result.ValueOrDie());
      FlightStreamChunkIterator iterator(std::move(stream_reader));

      while (true) {
        FlightStreamChunk chunk;
        if (iterator.GetNext(&chunk)) {
          Produce(chunk);
        } else {
          break;
        }
      }
      active_threads_--;
      not_empty.notify_all();
    });
  }
}

bool FlightStreamChunkBuffer::GetNext(FlightStreamChunk *result) {

  // Acquire a unique lock on the mutex
  std::unique_lock<std::mutex> unique_lock(mtx);

  // Wait if buffer is empty
  not_empty.wait(unique_lock, [this]() {
    return buffer_size != 0 || closed_ || active_threads_ == 0;
  });

  if (closed_) {
    return false;
  }
  if (buffer_size == 0 && active_threads_ == 0) {
    return false;
  }

  // Get value from position to remove in buffer
  *result = buffer[left];

  // Update appropriate fields
  left = (left + 1) % BUFFER_CAPACITY;
  buffer_size--;

  // Unlock unique lock
  unique_lock.unlock();

  // Notify a single thread that the buffer isn't full
  not_full.notify_one();

  // Return result
  return true;
}

void FlightStreamChunkBuffer::Close() {
  if (closed_) return;
  closed_ = true;
  not_empty.notify_all();

  for (auto &item: threads_) {
    item.join();
  }
}

FlightStreamChunkBuffer::~FlightStreamChunkBuffer() {
  Close();
}

}
}

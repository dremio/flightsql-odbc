/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include "flight_sql_stream_chunk_buffer.h"

#include <iostream>


namespace driver {
namespace flight_sql {

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
  std::cout << "produced, size: " << buffer_size << std::endl;


  // Unlock unique lock
  unique_lock.unlock();

  // Notify a single thread that buffer isn't empty
  not_empty.notify_one();
}

FlightStreamChunkBuffer::FlightStreamChunkBuffer(FlightSqlClient &flight_sql_client,
                                                 const arrow::flight::FlightCallOptions &call_options,
                                                 const std::shared_ptr<FlightInfo> &flight_info) : iterator_(
        flight_sql_client, call_options, flight_info) {
  threads_.emplace_back([=] {
    while (true) {
      FlightStreamChunk chunk;
      if (iterator_.GetNext(&chunk)) {
        Produce(chunk);
      } else {
        break;
      }
    }
  });
}

bool FlightStreamChunkBuffer::GetNext(FlightStreamChunk *result) {

  // Acquire a unique lock on the mutex
  std::unique_lock<std::mutex> unique_lock(mtx);

  // Wait if buffer is empty
  not_empty.wait(unique_lock, [this]() {
    return buffer_size != 0;
  });

  // Get value from position to remove in buffer
  *result = buffer[left];

  // Update appropriate fields
  left = (left + 1) % BUFFER_CAPACITY;
  buffer_size--;
  std::cout << "consumed, size: " << buffer_size << std::endl;

  // Unlock unique lock
  unique_lock.unlock();

  // Notify a single thread that the buffer isn't full
  not_full.notify_one();

  // Return result
  return true;
}

void FlightStreamChunkBuffer::Close() {
  iterator_.Close();
}

FlightStreamChunkBuffer::~FlightStreamChunkBuffer() {
  for (auto &item: threads_) {
    item.join();
  }
}

}
}

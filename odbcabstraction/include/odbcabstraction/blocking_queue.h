/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#pragma once

#include <atomic>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <vector>
#include <chrono>
#include <boost/optional.hpp>

namespace driver {
namespace odbcabstraction {


template<typename T>
class BlockingQueue {

  size_t capacity_;
  size_t extended_capacity_;
  std::vector<T> buffer_;
  size_t buffer_size_{0};
  size_t left_{0}; // index where variables are put inside of buffer (produced)
  size_t right_{0}; // index where variables are removed from buffer (consumed)

  std::mutex mtx_;
  std::condition_variable not_empty_;
  std::condition_variable not_full_;

  std::vector<std::thread> threads_;
  std::atomic<size_t> active_threads_{0};
  std::atomic<bool> closed_{false};

public:
  typedef std::function<boost::optional<T>(void)> Supplier;

  BlockingQueue(size_t capacity, bool use_extended_buffer):
    capacity_(capacity),
    extended_capacity_(0) {
      if (use_extended_buffer) {
        extended_capacity_ = 1000 * capacity_;
        buffer_.resize(extended_capacity_, T());
      }
      else buffer_.resize(capacity_);
    }

  void AddProducer(Supplier supplier) {
    active_threads_++;
    threads_.emplace_back([=] {
      while (!closed_) {
        // Block while queue is full
        std::unique_lock<std::mutex> unique_lock(mtx_);
        if (!WaitUntilCanPushOrClosed(unique_lock)) break;

        // Only one thread at a time be notified and call supplier
        auto item = supplier();
        if (!item) break;

        Push(*item);
        not_empty_.notify_one();
      }

      std::unique_lock<std::mutex> unique_lock(mtx_);
      active_threads_--;
      not_empty_.notify_all();
    });
  }

  bool Pop(T *result) {
    std::unique_lock<std::mutex> unique_lock(mtx_);
    if (!WaitUntilCanPopOrClosed(unique_lock)) return false;

    *result = std::move(buffer_[left_]);

    left_ = (left_ + 1) % (extended_capacity_ ? extended_capacity_ : capacity_);
    buffer_size_--;

    not_full_.notify_one();

    return true;
  }

  void Close() {
    std::unique_lock<std::mutex> unique_lock(mtx_);

    if (closed_) return;
    closed_ = true;
    not_empty_.notify_all();
    not_full_.notify_all();

    unique_lock.unlock();

    for (auto &item: threads_) {
      item.join();
    }
  }

private:

  void Push(T item) {
    buffer_[right_] = std::move(item);

    right_ = (right_ + 1) % (extended_capacity_ ? extended_capacity_ : capacity_);
    buffer_size_++;

  }

  bool WaitUntilCanPushOrClosed(std::unique_lock<std::mutex> &unique_lock) {
    if (extended_capacity_ > 0 && buffer_size_ >= extended_capacity_ ) {
      not_full_.wait(unique_lock, [this]() {
        return closed_ || buffer_size_ < extended_capacity_;
      });
    }
    else if (extended_capacity_ > 0 && buffer_size_ >= capacity_) {
      not_full_.wait_for(unique_lock, std::chrono::milliseconds(500));
    }
    else {
      not_full_.wait(unique_lock, [this]() {
        return closed_ || buffer_size_ < capacity_;
      });
    }

    return !closed_;
  }

  bool WaitUntilCanPopOrClosed(std::unique_lock<std::mutex> &unique_lock) {
    not_empty_.wait(unique_lock, [this]() {
      return closed_ || buffer_size_ != 0 || active_threads_ == 0;
    });

    return !closed_ && buffer_size_ > 0;
  }
};

}
}
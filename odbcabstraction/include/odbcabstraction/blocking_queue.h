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
#include <boost/optional.hpp>

namespace driver {
namespace odbcabstraction {


template<typename T>
class BlockingQueue {

  size_t capacity_;
  std::vector<T> buffer_;
  size_t buffer_size_{0};
  size_t left_{0}; // index where variables are put inside of buffer (produced)
  size_t right_{0}; // index where variables are removed from buffer (consumed)

  std::mutex mtx_;
  std::condition_variable not_empty_;
  std::condition_variable not_full_;

  std::vector<std::thread> threads_;
  std::atomic<size_t> active_threads_{0};
  bool closed_{false};

public:
  typedef std::function<boost::optional<T>(void)> Supplier;

  BlockingQueue(size_t capacity): capacity_(capacity), buffer_(capacity) {}

  void AddProducer(Supplier supplier) {
    active_threads_++;
    threads_.emplace_back([=] {
      while (!closed_) {
        if (!Produce(supplier)) {
          break;
        }
      }

      active_threads_--;
      not_empty_.notify_all();
    });
  }

  bool Produce(Supplier supplier) {
    std::unique_lock<std::mutex> unique_lock(mtx_);

    not_full_.wait(unique_lock, [this]() {
      return buffer_size_ != capacity_ || closed_;
    });
    if (closed_) {
      return false;
    }

    bool produced = false;
    auto item = supplier();
    if (item) {
      buffer_[right_] = std::move(*item);

      right_ = (right_ + 1) % capacity_;
      buffer_size_++;

      produced = true;
    }

    unique_lock.unlock();

    not_empty_.notify_one();

    return produced;
  }

  bool Consume(T *result) {
    std::unique_lock<std::mutex> unique_lock(mtx_);

    not_empty_.wait(unique_lock, [this]() {
      return buffer_size_ != 0 || closed_ || active_threads_ == 0;
    });

    if (closed_) {
      return false;
    }
    if (buffer_size_ == 0 && active_threads_ == 0) {
      return false;
    }

    *result = std::move(buffer_[left_]);

    left_ = (left_ + 1) % capacity_;
    buffer_size_--;

    unique_lock.unlock();
    not_full_.notify_one();
    return true;
  }

  void Close() {
    if (closed_) return;
    closed_ = true;
    not_empty_.notify_all();
    not_full_.notify_all();

    for (auto &item: threads_) {
      item.join();
    }
  }
};

}
}

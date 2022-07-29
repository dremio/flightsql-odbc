/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#pragma once

#include <functional>
#include <string>

#include <spdlog/fmt/bundled/format.h>

#define __LAZY_LOG(LEVEL, ...) do {                                                           \
  driver::odbcabstraction::Logger *logger = driver::odbcabstraction::Logger::GetInstance();   \
  if (logger) {                                                                               \
    logger->log(driver::odbcabstraction::LogLevel::LogLevel_##LEVEL, [&]() {                  \
      return fmt::format(__VA_ARGS__);                                                        \
    });                                                                                       \
  }                                                                                           \
} while(0)
#define LOG_DEBUG(...) __LAZY_LOG(DEBUG, __VA_ARGS__)
#define LOG_INFO(...) __LAZY_LOG(INFO, __VA_ARGS__)
#define LOG_ERROR(...) __LAZY_LOG(ERROR, __VA_ARGS__)
#define LOG_TRACE(...) __LAZY_LOG(TRACE, __VA_ARGS__)
#define LOG_WARN(...) __LAZY_LOG(WARN, __VA_ARGS__)

namespace driver {
namespace odbcabstraction {

enum LogLevel {
  LogLevel_TRACE,
  LogLevel_DEBUG,
  LogLevel_INFO,
  LogLevel_WARN,
  LogLevel_ERROR,
  LogLevel_OFF
};

class Logger {
protected:
  Logger() = default;

public:
  static Logger *GetInstance();
  static void SetInstance(std::unique_ptr<Logger> logger);

  virtual ~Logger() = default;

  virtual void log(LogLevel level, const std::function<std::string(void)> &build_message) = 0;
};

} // namespace odbcabstraction
} // namespace driver

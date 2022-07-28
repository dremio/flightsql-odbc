/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#pragma once

#include <cstdint>
#include <map>
#include <string>

#include <boost/algorithm/string.hpp>
#include <spdlog/fmt/bundled/format.h>

#define LOG_DEBUG(...) driver::odbcabstraction::Logger::GetInstance()->log(driver::odbcabstraction::LogLevel::LogLevel_DEBUG, __VA_ARGS__);
#define LOG_INFO(...) driver::odbcabstraction::Logger::GetInstance()->log(driver::odbcabstraction::LogLevel::LogLevel_INFO, __VA_ARGS__);
#define LOG_ERROR(...) driver::odbcabstraction::Logger::GetInstance()->log(driver::odbcabstraction::LogLevel::LogLevel_ERROR, __VA_ARGS__);
#define LOG_TRACE(...) driver::odbcabstraction::Logger::GetInstance()->log(driver::odbcabstraction::LogLevel::LogLevel_TRACE, __VA_ARGS__);
#define LOG_WARN(...) driver::odbcabstraction::Logger::GetInstance()->log(driver::odbcabstraction::LogLevel::LogLevel_WARN, __VA_ARGS__);

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

  virtual void log(LogLevel level, const std::string &message) = 0;

  virtual bool checkLogLevel(LogLevel called) = 0;

  template <typename... Args>
  void log(LogLevel level, fmt::format_string<Args...> fmt, Args &&... args) {
    if(checkLogLevel(level)) {
      log(level, fmt::format(fmt, args...));
    }
  };
};

} // namespace odbcabstraction
} // namespace driver

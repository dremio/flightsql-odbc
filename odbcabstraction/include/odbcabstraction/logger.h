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
#include <fmt/core.h>

#include <odbcabstraction/spi/connection.h>

#define LOG_DEBUG(...) driver::odbcabstraction::Logger::GetInstance()->log(driver::odbcabstraction::LogLevel::DEBUG, __VA_ARGS__);
#define LOG_INFO(...) driver::odbcabstraction::Logger::GetInstance()->log(driver::odbcabstraction::LogLevel::INFO, __VA_ARGS__);
#define LOG_ERROR(...) driver::odbcabstraction::Logger::GetInstance()->log(driver::odbcabstraction::LogLevel::ERROR, __VA_ARGS__);
#define LOG_TRACE(...) driver::odbcabstraction::Logger::GetInstance()->log(driver::odbcabstraction::LogLevel::TRACE, __VA_ARGS__);
#define LOG_WARN(...) driver::odbcabstraction::Logger::GetInstance()->log(driver::odbcabstraction::LogLevel::WARN, __VA_ARGS__);

namespace driver {
namespace odbcabstraction {

enum LogLevel { TRACE, DEBUG, INFO, WARN, ERROR, OFF };

typedef std::string ConfigProperty;
typedef std::map<std::string, ConfigProperty, Connection::CaseInsensitiveComparator>
    ConfigPropertyMap;

class Logger {
protected:
  Logger() = default;

public:
  static Logger *GetInstance();
  static void SetInstance(Logger * logger);

  virtual ~Logger() = default;

  virtual void log(LogLevel level, const std::string &message) = 0;

  template <typename... Args>
  void log(LogLevel level, fmt::format_string<Args...> fmt, Args &&... args) {
    log(level, fmt::format(fmt, args...));
  };
};

} // namespace odbcabstraction
} // namespace driver

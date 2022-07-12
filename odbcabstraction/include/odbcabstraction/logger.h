/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#pragma once

#include <cstdint>
#include <string>
#include <map>

#include <boost/algorithm/string.hpp>
#include <fmt/core.h>

#define LOG_DEBUG(...) Logger::GetInstance()->log(LogLevel::DEBUG, __VA_ARGS__);
#define LOG_INFO(...) Logger::GetInstance()->log(LogLevel::INFO, __VA_ARGS__);
#define LOG_ERROR(...) Logger::GetInstance()->log(LogLevel::ERROR, __VA_ARGS__);
#define LOG_TRACE(...) Logger::GetInstance()->log(LogLevel::TRACE, __VA_ARGS__);
#define LOG_WARN(...) Logger::GetInstance()->log(LogLevel::WARN, __VA_ARGS__);

enum LogLevel {
  TRACE, DEBUG, INFO, WARN, ERROR
};

/// \brief Case insensitive comparator
struct CaseInsensitiveComparator
  : std::binary_function<std::string, std::string, bool> {
  bool operator()(const std::string &s1, const std::string &s2) const {
    return boost::lexicographical_compare(s1, s2, boost::is_iless());
  }
};

typedef std::string ConfigProperty;
typedef std::map<std::string, ConfigProperty, CaseInsensitiveComparator>
  ConfigPropertyMap;

const static std::string LOG_LEVEL = "LogLevel";
const static std::string ASYNC_DISABLE = "AsyncDisable";
const static std::string LOG_PATH = "LogPath";
const static std::string MAXIMUM_FILE_SIZE = "MaximumFileSize";
const static std::string FILE_QUANTITY = "FileQuantity";
const static std::string LOG_ENABLED = "LogEnabled";

class Logger {
protected:
  Logger() = default;

public:
  static Logger * GetInstance();
  Logger(Logger &other) = delete;
  void operator=(const Logger &) = delete;
  virtual ~Logger() = default;
  virtual void log(LogLevel level, const std::string & message) = 0;

  template<typename... Args>
  void log(LogLevel level, fmt::format_string<Args...> fmt, Args &&...args) {
      log(level, fmt::format(fmt, args...));
  };
};

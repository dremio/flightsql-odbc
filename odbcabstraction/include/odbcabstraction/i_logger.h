#pragma once

#include <cstdint>
#include <string>

#define LOG_DEBUG(MESSAGE, ARGS) ILogger::GetInstance()->log(LogLevel::DEBUG, MESSAGE);
#define LOG_INFO(MESSAGE, ARGS) ILogger::GetInstance()->log(LogLevel::INFO, MESSAGE);
#define LOG_ERROR(MESSAGE, ARGS) ILogger::GetInstance()->log(LogLevel::ERROR, MESSAGE);
#define LOG_TRACE(MESSAGE, ARGS) ILogger::GetInstance()->log(LogLevel::TRACE, MESSAGE);
#define LOG_WARN(MESSAGE, ARGS) ILogger::GetInstance()->log(LogLevel::WARN, MESSAGE);

enum LogLevel {
  TRACE, DEBUG, INFO, WARN, ERROR
};


class ILogger {
protected:
  ILogger() = default;

public:
  static ILogger * GetInstance();
  ILogger(ILogger &other) = delete;
  void operator=(const ILogger &) = delete;
  virtual ~ILogger() = default;
  virtual void log(LogLevel level, const std::string & message) = 0;
  virtual void init(int64_t fileQuantity, int64_t maxFileSize,
                    const std::string& fileNamePrefix, LogLevel level) = 0;
};

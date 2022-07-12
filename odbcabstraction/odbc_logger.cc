/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include "odbc_logger.h"

#include "odbcabstraction/logger.h"

#include <cstdint>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/async.h>

namespace {
  spdlog::level::level_enum ToSpdLogLevel(LogLevel level) {
    switch (level) {
      case TRACE:
        return spdlog::level::trace;
      case DEBUG:
        return spdlog::level::debug;
      case INFO:
        return spdlog::level::info;
      case WARN:
        return spdlog::level::warn;
      case ERROR:
        return spdlog::level::err;
    }
  }
}

static std::unique_ptr<Logger> odbc_logger_;

Logger * Logger::GetInstance() {
  if (odbc_logger_ == nullptr) {
    ODBCLogger::MakeLogger();
  }
  return odbc_logger_.get();
}

void ODBCLogger::init(int64_t fileQuantity, int64_t maxFileSize,
                       const std::string &fileNamePrefix, LogLevel level) {
  logger_ = spdlog::rotating_logger_mt<spdlog::async_factory>("ODBC Logger",
                                                              fileNamePrefix,
                                                              maxFileSize,
                                                              fileQuantity);
  logger_->set_level(ToSpdLogLevel(level));
}

void ODBCLogger::log(LogLevel level, const std::string &message) {
  switch (level) {
    case DEBUG:
      logger_->debug(message);
      break;
    case TRACE:
      logger_->trace(message);
      break;
    case INFO:
      logger_->info(message);
      break;
    case WARN:
      logger_->warn(message);
      break;
    case ERROR:
      logger_->error(message);
      break;
  }
}

void ODBCLogger::MakeLogger() {
  odbc_logger_.reset(new ODBCLogger());
}

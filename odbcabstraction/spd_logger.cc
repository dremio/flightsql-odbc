/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include "odbcabstraction/spd_logger.h"

#include "odbcabstraction/logger.h"

#include <cstdint>
#include <spdlog/async.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/spdlog.h>

namespace driver {
namespace odbcabstraction {

const std::string SPDLogger::LOG_LEVEL = "LogLevel";
const std::string SPDLogger::ASYNC_DISABLE= "AsyncDisable";
const std::string SPDLogger::LOG_PATH= "LogPath";
const std::string SPDLogger::MAXIMUM_FILE_SIZE= "MaximumFileSize";
const std::string SPDLogger::FILE_QUANTITY= "FileQuantity";
const std::string SPDLogger::LOG_ENABLED= "LogEnabled";

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
  default:
    return spdlog::level::off;
  }
}
} // namespace

void SPDLogger::init(int64_t fileQuantity, int64_t maxFileSize,
                     const std::string &fileNamePrefix, LogLevel level) {
  logger_ = spdlog::rotating_logger_mt<spdlog::async_factory>(
      "ODBC Logger", fileNamePrefix, maxFileSize, fileQuantity);

  logger_->set_level(ToSpdLogLevel(level));
}

void SPDLogger::log(LogLevel level, const std::string &message) {
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

} // namespace odbcabstraction
} // namespace driver

/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include "odbcabstraction/spd_logger.h"

#include "odbcabstraction/logger.h"

#include <spdlog/async.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/spdlog.h>

#include <cstdint>

namespace driver {
namespace odbcabstraction {

const std::string SPDLogger::LOG_LEVEL = "LogLevel";
const std::string SPDLogger::LOG_PATH= "LogPath";
const std::string SPDLogger::MAXIMUM_FILE_SIZE= "MaximumFileSize";
const std::string SPDLogger::FILE_QUANTITY= "FileQuantity";
const std::string SPDLogger::LOG_ENABLED= "LogEnabled";

namespace {
inline spdlog::level::level_enum ToSpdLogLevel(LogLevel level) {
  switch (level) {
  case LogLevel_TRACE:
    return spdlog::level::trace;
  case LogLevel_DEBUG:
    return spdlog::level::debug;
  case LogLevel_INFO:
    return spdlog::level::info;
  case LogLevel_WARN:
    return spdlog::level::warn;
  case LogLevel_ERROR:
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

void SPDLogger::log(LogLevel level, const std::function<std::string(void)> &build_message) {
  auto level_set = logger_->level();
  spdlog::level::level_enum spdlog_level = ToSpdLogLevel(level);
  if (level_set == spdlog::level::off || level_set > spdlog_level) {
    return;
  }

  const std::string &message = build_message();
  logger_->log(spdlog_level, message);
}

} // namespace odbcabstraction
} // namespace driver

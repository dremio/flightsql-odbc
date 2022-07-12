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
#include <csignal>

namespace driver {
namespace odbcabstraction {

const std::string SPDLogger::LOG_LEVEL = "LogLevel";
const std::string SPDLogger::LOG_PATH= "LogPath";
const std::string SPDLogger::MAXIMUM_FILE_SIZE= "MaximumFileSize";
const std::string SPDLogger::FILE_QUANTITY= "FileQuantity";
const std::string SPDLogger::LOG_ENABLED= "LogEnabled";

namespace {
std::function<void(int)> shutdown_handler;
void signal_handler(int signal) {
  shutdown_handler(signal);
}

typedef void (*Handler)(int signum);

Handler old_sigint_handler = SIG_IGN;
Handler old_sigsegv_handler = SIG_IGN;
Handler old_sigabrt_handler = SIG_IGN;
Handler old_sigkill_handler = SIG_IGN;

void set_int_handler(int signum, Handler& old_handler) {
  Handler old = signal(signum, SIG_IGN);
  if (old != SIG_IGN) {
    old_handler = old;
  }
  signal(signum, signal_handler);
}

void rst_int_handler(int signum, Handler& handler) {
  Handler actual_handler = signal(signum, SIG_IGN);
  if (actual_handler == signal_handler) {
    signal(signum, handler);
  }
}


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

  if (level != LogLevel::OFF) {
    set_int_handler(SIGINT, old_sigint_handler);
    set_int_handler(SIGSEGV, old_sigsegv_handler);
    set_int_handler(SIGABRT, old_sigabrt_handler);
    set_int_handler(SIGKILL, old_sigkill_handler);
    shutdown_handler = [&](int signal) {
      logger_->flush();
      spdlog::shutdown();
    };
  }
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

SPDLogger::~SPDLogger() {
  rst_int_handler(SIGINT, old_sigint_handler);
  rst_int_handler(SIGSEGV, old_sigsegv_handler);
  rst_int_handler(SIGABRT, old_sigabrt_handler);
  rst_int_handler(SIGKILL, old_sigkill_handler);
}

bool SPDLogger::checkLogLevel(LogLevel called) {
  auto set = logger_->level();
  if (set == spdlog::level::off) {
    return false;
  } else {
    return set <= ToSpdLogLevel(called);
  }
}

} // namespace odbcabstraction
} // namespace driver

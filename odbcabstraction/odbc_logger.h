/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#pragma once

#include "odbcabstraction/logger.h"

#include <cstdint>
#include <string>

#include <spdlog/spdlog.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/async.h>

class ODBCLogger : public Logger {
protected:
  std::shared_ptr<spdlog::logger> logger_;
  ODBCLogger() = default;

public:
  ODBCLogger(ODBCLogger &other) = delete;
  void operator=(const ODBCLogger &) = delete;
  static void MakeLogger();
  void init(int64_t fileQuantity, int64_t maxFileSize,
            const std::string& fileNamePrefix, LogLevel level);
  void log(LogLevel level, const std::string &message) override;
  template<typename... Args>
  void log(LogLevel level, fmt::format_string<Args...> fmt, Args &&...args) {
  };
};

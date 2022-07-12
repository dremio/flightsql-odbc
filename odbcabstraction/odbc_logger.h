/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#pragma once

#include "odbcabstraction/i_logger.h"

#include <cstdint>
#include <string>

#include <spdlog/spdlog.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/async.h>

class ODBCLogger : public ILogger {
protected:
  std::shared_ptr<spdlog::logger> logger_;
  ODBCLogger() = default;

public:
  static void MakeLogger();
  void init(int64_t fileQuantity, int64_t maxFileSize,
            const std::string& fileNamePrefix, LogLevel level) override;

  void log(LogLevel level, const std::string &message) override;
};

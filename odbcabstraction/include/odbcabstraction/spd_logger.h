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

namespace driver {
namespace odbcabstraction {

class SPDLogger : public Logger {
protected:
  std::shared_ptr<spdlog::logger> logger_;

public:
  static const std::string LOG_LEVEL;
  static const std::string LOG_PATH;
  static const std::string MAXIMUM_FILE_SIZE;
  static const std::string FILE_QUANTITY;
  static const std::string LOG_ENABLED;

  SPDLogger() = default;
  ~SPDLogger() = default;
  SPDLogger(SPDLogger &other) = delete;

  void operator=(const SPDLogger &) = delete;
  void init(int64_t fileQuantity, int64_t maxFileSize,
            const std::string &fileNamePrefix, LogLevel level);

  void log(LogLevel level, const std::function<std::string(void)> &build_message) override;
};

} // namespace odbcabstraction
} // namespace driver

/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */


#include <odbcabstraction/logger.h>

namespace driver {
namespace odbcabstraction {

static std::unique_ptr<Logger> odbc_logger_ = nullptr;

Logger *Logger::GetInstance() {
  return odbc_logger_.get();
}

void Logger::SetInstance(std::unique_ptr<Logger>logger) {
  odbc_logger_ = std::move(logger);
}

}
}

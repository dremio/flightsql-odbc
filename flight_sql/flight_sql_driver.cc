/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include "flight_sql_connection.h"
#include "../odbcabstraction/spd_logger.h"
#include <odbcabstraction/platform.h>
#include <odbcabstraction/utils.h>
#include <flight_sql/flight_sql_driver.h>

namespace driver {
namespace flight_sql {

using odbcabstraction::Connection;
using odbcabstraction::OdbcVersion;
using odbcabstraction::LogLevel;
using odbcabstraction::SPDLogger;

namespace {
  LogLevel ToLogLevel(int64_t level) {
    switch (level) {
      case 0:
        return LogLevel::TRACE;
      case 1:
        return LogLevel::DEBUG;
      case 2:
        return LogLevel::INFO;
      case 3:
        return LogLevel::WARN;
      case 4:
        return LogLevel::ERROR;
      default:
        return LogLevel::OFF;
    }
  }
}

FlightSqlDriver::FlightSqlDriver()
    : diagnostics_("Apache Arrow", "Flight SQL", OdbcVersion::V_3),
      version_("0.9.0.0")
{}

std::shared_ptr<Connection>
FlightSqlDriver::CreateConnection(OdbcVersion odbc_version) {
  return std::make_shared<FlightSqlConnection>(odbc_version, version_);
}

odbcabstraction::Diagnostics &FlightSqlDriver::GetDiagnostics() {
  return diagnostics_;
}

void FlightSqlDriver::SetVersion(std::string version) {
  version_ = std::move(version);
}

void FlightSqlDriver::RegisterLog() {
  odbcabstraction::ConfigPropertyMap propertyMap;
  
  driver::odbcabstraction::ReadConfigLogFile(propertyMap);

  auto log_enable_iterator = propertyMap.find(SPDLogger::LOG_ENABLED);

  auto log_enabled = log_enable_iterator != propertyMap.end() ?
    odbcabstraction::AsBool(log_enable_iterator->second) : false;

  if (log_enabled.get()) {
    auto log_level_iterator = propertyMap.find(SPDLogger::LOG_LEVEL);

    auto log_level =
      log_level_iterator != propertyMap.end() ? std::stoi(log_level_iterator->second) : 1;

    auto log_path_iterator = propertyMap.find(SPDLogger::LOG_PATH);

    auto log_path =
      log_path_iterator != propertyMap.end() ? log_path_iterator->second : "/tmp";

    auto maximum_file_size_iterator = propertyMap.find(SPDLogger::MAXIMUM_FILE_SIZE);

    auto maximum_file_size = maximum_file_size_iterator != propertyMap.end() ? std::stoi(maximum_file_size_iterator->second) : 0;

    auto maximum_file_quantity_iterator = propertyMap.
      find(SPDLogger::FILE_QUANTITY);

    auto maximum_file_quantity =
      maximum_file_quantity_iterator != propertyMap.end() ? std::stoi(
        maximum_file_quantity_iterator->second) : 0;

    auto async_disable_iterator = propertyMap.find(SPDLogger::ASYNC_DISABLE);

    auto async_disable = async_disable_iterator != propertyMap.end() ? odbcabstraction::AsBool(async_disable_iterator->second) : false;

    auto *logger = new odbcabstraction::SPDLogger();

    logger->init(maximum_file_quantity, maximum_file_size,
                                      log_path, ToLogLevel(log_level));
    odbcabstraction::Logger::SetInstance(logger);
  } else {
    auto *logger = new odbcabstraction::SPDLogger();

    logger->init(1, 1,
                 "/temp", LogLevel::OFF);
    odbcabstraction::Logger::SetInstance(logger);
  }
}

} // namespace flight_sql
} // namespace driver

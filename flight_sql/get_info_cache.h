/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#pragma once

#include <arrow/flight/api.h>
#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <odbcabstraction/spi/connection.h>

namespace arrow {
namespace flight {
namespace sql {
class FlightSqlClient;
}
} // namespace flight
} // namespace arrow

namespace driver {
namespace flight_sql {

class GetInfoCache {

private:
  std::map<uint16_t, driver::odbcabstraction::Connection::Info> info_;
  arrow::flight::FlightCallOptions &call_options_;
  std::unique_ptr<arrow::flight::sql::FlightSqlClient> &sql_client_;
  std::mutex mutex_;
  std::atomic<bool> has_server_info_;

public:
  GetInfoCache(arrow::flight::FlightCallOptions &call_options,
               std::unique_ptr<arrow::flight::sql::FlightSqlClient> &client);
  void SetProperty(uint16_t property,
                   driver::odbcabstraction::Connection::Info value);
  driver::odbcabstraction::Connection::Info GetInfo(uint16_t info_type);

private:
  bool LoadInfoFromServer();
};
} // namespace flight_sql
} // namespace driver

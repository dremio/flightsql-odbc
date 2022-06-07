/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#pragma once

#include <cstdint>
#include <ctime>

namespace driver {
namespace odbcabstraction {
  void GetTimeForMillisSinceEpoch(tm& date, int64_t value);
} // namespace flight_sql
} // namespace driver

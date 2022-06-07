/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include <cstdint>
#include <ctime>

namespace driver {
namespace odbcabstraction {
void GetTimeForMillisSinceEpoch(tm& date, int64_t value) {
  #if defined(_WIN32)
    gmtime_s(&date, &value);
  #else
    time_t time_value = static_cast<time_t>(value);
    gmtime_r(&time_value, &date);
  #endif
  }
} // namespace odbcabstraction
} // namespace driver

/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include "odbcabstraction/calendar_utils.h"

#include <cstdint>
#include <ctime>

namespace driver {
namespace odbcabstraction {
int64_t GetTodayTimeFromEpoch() {
  tm date{};
  int64_t t = std::time(0);

  GetTimeForSecondsSinceEpoch(date, t);

  date.tm_hour = 0;
  date.tm_min = 0;
  date.tm_sec = 0;

  #if defined(_WIN32)
    return _mkgmtime(&date);
  #else
    return timegm(&date);
  #endif
}

void GetTimeForSecondsSinceEpoch(tm& date, int64_t value) {
  #if defined(_WIN32)
    gmtime_s(&date, &value);
  #else
    time_t time_value = static_cast<time_t>(value);
    gmtime_r(&time_value, &date);
  #endif
  }
} // namespace odbcabstraction
} // namespace driver

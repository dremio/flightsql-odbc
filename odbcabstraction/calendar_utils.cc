/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include "odbcabstraction/calendar_utils.h"

#include <boost/date_time/posix_time/posix_time.hpp>
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
  date = boost::posix_time::to_tm(boost::posix_time::from_time_t(value));
}
} // namespace odbcabstraction
} // namespace driver

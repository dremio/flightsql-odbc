/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#pragma once

#include <string>

#include <odbcabstraction/platform.h>
#include <sys/types.h>
#if !_WIN32
#include <netdb.h>
#endif

namespace driver {

class AddressInfo {
private:
  struct addrinfo * addrinfo_result_;

public:
  AddressInfo();

  ~AddressInfo();

  bool GetAddressInfo(const std::string &host, char *host_name_info, int64_t max_host);
};
}

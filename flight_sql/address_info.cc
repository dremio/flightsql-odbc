/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include "address_info.h"

namespace driver {

bool AddressInfo::GetAddressInfo(const std::string &host, char *host_name_info, int64_t max_host) {
  int error;
  error = getaddrinfo(host.c_str(), NULL, NULL, &result);

  if (error != 0) {
    return false;
  }

  error = getnameinfo(result->ai_addr, result->ai_addrlen, host_name_info,
                          max_host, NULL, 0, 0);
  return error == 0;
}

AddressInfo::~AddressInfo() {
  if (result) {
    freeaddrinfo(result);
  }
}

AddressInfo::AddressInfo() : result(nullptr) {}
} // namespace driver

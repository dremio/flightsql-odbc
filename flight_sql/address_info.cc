/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include "address_info.h"

namespace driver {

bool AddressInfo::GetAddressInfo(const std::string &host, char *host_name_info, int64_t max_host) {
  if (addrinfo_result_) {
    freeaddrinfo(addrinfo_result_);
    addrinfo_result_ = nullptr;
  }

  int error;
  error = getaddrinfo(host.c_str(), NULL, NULL, &addrinfo_result_);

  if (error != 0) {
    return false;
  }

  error = getnameinfo(addrinfo_result_->ai_addr, addrinfo_result_->ai_addrlen, host_name_info,
                          max_host, NULL, 0, 0);
  return error == 0;
}

AddressInfo::~AddressInfo() {
  if (addrinfo_result_) {
    freeaddrinfo(addrinfo_result_);
    addrinfo_result_ = nullptr;
  }
}

AddressInfo::AddressInfo() : addrinfo_result_(nullptr) {}
} // namespace driver

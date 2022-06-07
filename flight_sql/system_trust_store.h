/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#pragma once

#if defined _WIN32 || defined _WIN64

#include <windows.h>
#include <wincrypt.h>
#include <bcrypt.h>
#include <cryptuiapi.h>
#include <iostream>
#include <tchar.h>
#include <vector>

namespace driver {
namespace flight_sql {

/// Load the certificates from the windows system trust store. Part of the logic
/// was based in the drill connector
/// https://github.com/apache/drill/blob/master/contrib/native/client/src/clientlib/wincert.ipp.
class SystemTrustStore {
private:
  const char* stores_;
  HCERTSTORE h_store_;
  PCCERT_CONTEXT p_context_;

public:
  explicit SystemTrustStore(const char* store);

  ~SystemTrustStore();

  /// Check if there is a certificate inside the system trust store to be extracted
  /// \return   If there is a valid cert in the store.
  bool HasNext();

  /// Get the next certificate from the store.
  /// \return   the certificate.
  std::string GetNext() const;

  /// Check if the system has the specify store.
  /// \return  If the specific store exist in the system.
  bool SystemHasStore();
};
} // namespace flight_sql
} // namespace driver

#else // Not Windows
namespace driver {
namespace flight_sql {
class SystemTrustStore;
} // namespace flight_sql
} // namespace driver

#endif

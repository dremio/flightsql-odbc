/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include "flight_sql_ssl_config.h"
#include "odbcabstraction/logger.h"

#include <odbcabstraction/exceptions.h>
#include <fstream>
#include <sstream>

namespace driver {
namespace flight_sql {


FlightSqlSslConfig::FlightSqlSslConfig(
  bool disableCertificateVerification, const std::string& trustedCerts,
  bool systemTrustStore, bool useEncryption)
  : trustedCerts_(trustedCerts), useEncryption_(useEncryption),
  disableCertificateVerification_(disableCertificateVerification),
  systemTrustStore_(systemTrustStore) {}

bool FlightSqlSslConfig::useEncryption() const {
  return useEncryption_;
}

bool FlightSqlSslConfig::shouldDisableCertificateVerification() const {
  return disableCertificateVerification_;
}

const std::string& FlightSqlSslConfig::getTrustedCerts() const {
  return trustedCerts_;
}

bool FlightSqlSslConfig::useSystemTrustStore() const {
  return systemTrustStore_;
}

void FlightSqlSslConfig::populateOptionsWithCerts(arrow::flight::CertKeyPair* out) {
  LOG_TRACE("[{}] Entering function", __FUNCTION__)

  try {
    std::ifstream cert_file(trustedCerts_);
    if (!cert_file) {
      const std::string message = std::string("Could not open certificate: " + trustedCerts_);
      LOG_ERROR(("[{}] " + message), __FUNCTION__)
      throw odbcabstraction::DriverException(message);
    }
    std::stringstream cert;
    cert << cert_file.rdbuf();
    out->pem_cert = cert.str();
  }
  catch (const std::ifstream::failure& e) {
    const char *message = e.what();
    LOG_ERROR("[{}] Failed to read certificate file {}", __FUNCTION__, message)
    throw odbcabstraction::DriverException(message);
  }
  LOG_TRACE("[{}] Exiting successfully with no return value", __FUNCTION__)
}
} // namespace flight_sql
} // namespace driver

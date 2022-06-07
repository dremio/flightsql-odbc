/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include "flight_sql_ssl_config.h"

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
  try {
    std::ifstream cert_file(trustedCerts_);
    if (!cert_file) {
      throw odbcabstraction::DriverException("Could not open certificate: " + trustedCerts_);
    }
    std::stringstream cert;
    cert << cert_file.rdbuf();
    out->pem_cert = cert.str();
  }
  catch (const std::ifstream::failure& e) {
    throw odbcabstraction::DriverException(e.what());
  }
}
} // namespace flight_sql
} // namespace driver

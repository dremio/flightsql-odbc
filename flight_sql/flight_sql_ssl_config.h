/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#pragma once

#include <arrow/flight/types.h>
#include <arrow/status.h>
#include <string>

namespace driver {
namespace flight_sql {

/// \brief An Auxiliary class that holds all the information to perform
///        a SSL connection.
class FlightSqlSslConfig {
public:
  FlightSqlSslConfig(bool disableCertificateVerification,
                     const std::string &trustedCerts, bool systemTrustStore,
                     bool useEncryption);

  /// \brief  Tells if ssl is enabled. By default it will be true.
  /// \return Whether ssl is enabled.
  bool useEncryption() const;

  /// \brief  Tells if disable certificate verification is enabled.
  /// \return Whether disable certificate verification is enabled.
  bool shouldDisableCertificateVerification() const;

  /// \brief  The path to the trusted certificate.
  /// \return Certificate path.
  const std::string &getTrustedCerts() const;

  /// \brief  Tells if we need to check if the certificate is in the system trust store.
  /// \return Whether to use the system trust store.
  bool useSystemTrustStore() const;

  /// \brief Loads the certificate file and extract the certificate file from it
  ///        and create the object CertKeyPair with it on.
  /// \param out A CertKeyPair with the cert on it.
  /// \return    The cert key pair object
  void populateOptionsWithCerts(arrow::flight::CertKeyPair *out);

private:
  const std::string trustedCerts_;
  const bool useEncryption_;
  const bool disableCertificateVerification_;
  const bool systemTrustStore_;
};
} // namespace flight_sql
} // namespace driver

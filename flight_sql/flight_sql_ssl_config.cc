// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "flight_sql_ssl_config.h"
#include "sstream"
#include <iostream>
#include <fstream>

driver::flight_sql::FlightSqlSslConfig::FlightSqlSslConfig(
    bool disableCertificateVerification, const std::string &trustedCerts,
    bool systemTrustStore, bool useEncryption)
    : useEncryption_(useEncryption), systemTrustStore_(systemTrustStore),
      trustedCerts_(trustedCerts),
      disableCertificateVerification_(disableCertificateVerification) {}

bool driver::flight_sql::FlightSqlSslConfig::isUseEncryption() const {
  return useEncryption_;
}

bool driver::flight_sql::FlightSqlSslConfig::isDisableCertificateVerification() const {
  return disableCertificateVerification_;
}

const std::string &driver::flight_sql::FlightSqlSslConfig::getTrustedCerts() const {
  return trustedCerts_;
}

bool driver::flight_sql::FlightSqlSslConfig::isSystemTrustStore() const {
  return systemTrustStore_;
}

arrow::Status driver::flight_sql::FlightSqlSslConfig::readCerts(arrow::flight::CertKeyPair* out) {
  try {
    std::ifstream cert_file(trustedCerts_);
    if (!cert_file) {
      return arrow::Status::IOError("Could not open certificate: " + trustedCerts_);
    }
    std::stringstream cert;
    cert << cert_file.rdbuf();
    out->pem_cert = cert.str();
    out->pem_key = "";
    return arrow::Status::OK();
  } catch (const std::ifstream::failure& e) {
    return arrow::Status::IOError(e.what());
  }
}

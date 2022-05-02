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

#include "flight_sql_connection.h"

#include <odbcabstraction/platform.h>
#include <odbcabstraction/utils.h>

#include <arrow/flight/types.h>
#include <arrow/flight/client_cookie_middleware.h>
#include <flight_sql_auth_method.h>
#include <flight_sql_statement.h>
#include <flight_sql_ssl_config.h>
#include <utils.h>

#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/optional.hpp>
#include <odbcabstraction/exceptions.h>

#include <sql.h>
#include <sqlext.h>
#include <iostream>

#include <openssl/ssl.h>
#include <openssl/x509.h>

#if defined _WIN32  || defined _WIN64

#include <cryptuiapi.h>
#include <iostream>
#include <tchar.h>
#include <wincrypt.h>
#include <windows.h>

inline
int loadSystemTrustStore(std::string& certs) {
  HCERTSTORE hStore;
  PCCERT_CONTEXT pContext = NULL;
  X509* x509;
  char* stores[] = {
    "CA",
    "MY",
    "ROOT",
    "SPC"
  };
  int certCount = 0;

  for (int i = 0; i < 4; i++) {
  hStore = CertOpenSystemStore(NULL, stores[i]);

  while (pContext = CertEnumCertificatesInStore(hStore, pContext)) {
    DWORD size = 0;
    CryptBinaryToString(pContext->pbCertEncoded, pContext->cbCertEncoded,
                        CRYPT_STRING_BASE64HEADER, nullptr, &size);
    std::vector<char> pem(size);
    CryptBinaryToString(pContext->pbCertEncoded,
                        pContext->cbCertEncoded, CRYPT_STRING_BASE64HEADER,
                        pem.data(), &size);
    certs += pem.data();
  }

  CertFreeCertificateContext(pContext);
  CertCloseStore(hStore, 0);
}

return 0;
}
#else
int loadSystemTrustStore(const SSL* ssl, std::string& msg) {
return 0;
}

#endif

namespace driver {
namespace flight_sql {

using arrow::Result;
using arrow::Status;
using arrow::flight::FlightCallOptions;
using arrow::flight::FlightClient;
using arrow::flight::FlightClientOptions;
using arrow::flight::Location;
using arrow::flight::TimeoutDuration;
using arrow::flight::sql::FlightSqlClient;
using driver::odbcabstraction::AsBool;
using driver::odbcabstraction::Connection;
using driver::odbcabstraction::DriverException;
using driver::odbcabstraction::OdbcVersion;
using driver::odbcabstraction::Statement;

const std::string FlightSqlConnection::HOST = "host";
const std::string FlightSqlConnection::PORT = "port";
const std::string FlightSqlConnection::USER = "user";
const std::string FlightSqlConnection::UID = "uid";
const std::string FlightSqlConnection::PASSWORD = "password";
const std::string FlightSqlConnection::PWD = "pwd";
const std::string FlightSqlConnection::TOKEN = "token";
const std::string FlightSqlConnection::USE_ENCRYPTION = "useEncryption";
const std::string FlightSqlConnection::DISABLE_CERTIFICATE_VERIFICATION = "disableCertificateVerification";
const std::string FlightSqlConnection::TRUSTED_CERTS = "trustedCerts";
const std::string FlightSqlConnection::USE_SYSTEM_TRUST_STORE = "useSystemTrustStore";

namespace {
const std::set<std::string, Connection::CaseInsensitiveComparator> BUILT_IN_PROPERTIES = {
    FlightSqlConnection::HOST,
    FlightSqlConnection::PORT,
    FlightSqlConnection::USER,
    FlightSqlConnection::UID,
    FlightSqlConnection::PASSWORD,
    FlightSqlConnection::PWD,
    FlightSqlConnection::TOKEN,
    FlightSqlConnection::USE_ENCRYPTION,
    FlightSqlConnection::DISABLE_CERTIFICATE_VERIFICATION,
    FlightSqlConnection::TRUSTED_CERTS,
    FlightSqlConnection::USE_SYSTEM_TRUST_STORE
};

Connection::ConnPropertyMap::const_iterator
TrackMissingRequiredProperty(const std::string &property,
                             const Connection::ConnPropertyMap &properties,
                             std::vector<std::string> &missing_attr) {
  Connection::ConnPropertyMap::const_iterator prop_iter =
      properties.find(property);
  if (properties.end() == prop_iter) {
    missing_attr.push_back(property);
  }
  return prop_iter;
}
} // namespace

std::shared_ptr<FlightSqlSslConfig> LoadFlightSslConfigs(const Connection::ConnPropertyMap &connPropertyMap) {
  auto use_encryption_iterator = connPropertyMap.find(
    FlightSqlConnection::USE_ENCRYPTION);
  bool use_encryption =
    use_encryption_iterator != connPropertyMap.end() ? AsBool(
      use_encryption_iterator->second) : true;

  auto disable_cert_iterator = connPropertyMap.find(
    FlightSqlConnection::DISABLE_CERTIFICATE_VERIFICATION);
  bool disable_cert =
    disable_cert_iterator != connPropertyMap.end() ? AsBool(
      disable_cert_iterator->second) : false;

  auto trusted_certs_iterator = connPropertyMap.find(
    FlightSqlConnection::TRUSTED_CERTS);
  auto trusted_certs =
    trusted_certs_iterator != connPropertyMap.end() ? trusted_certs_iterator->second : "";

  auto system_trust_iterator = connPropertyMap.find(
    FlightSqlConnection::USE_SYSTEM_TRUST_STORE);
  auto system_trusted =
    system_trust_iterator != connPropertyMap.end() ? AsBool(
      system_trust_iterator->second) : true;

  return std::make_shared<FlightSqlSslConfig>(disable_cert, trusted_certs,
                                              system_trusted, use_encryption);
}

void FlightSqlConnection::Connect(const ConnPropertyMap &properties,
                                  std::vector<std::string> &missing_attr) {
  try {
    auto flight_ssl_configs = LoadFlightSslConfigs(properties);

    Location location = BuildLocation(properties, missing_attr, flight_ssl_configs);
    FlightClientOptions client_options =
      BuildFlightClientOptions(properties, missing_attr,
                               flight_ssl_configs);

    const std::shared_ptr<arrow::flight::ClientMiddlewareFactory>
        &cookie_factory = arrow::flight::GetCookieFactory();
    client_options.middleware.push_back(cookie_factory);

    std::unique_ptr<FlightClient> flight_client;
    ThrowIfNotOK(
      FlightClient::Connect(location, client_options, &flight_client));

    std::unique_ptr<FlightSqlAuthMethod> auth_method =
      FlightSqlAuthMethod::FromProperties(flight_client, properties);
    auth_method->Authenticate(*this, call_options_);

    sql_client_.reset(new FlightSqlClient(std::move(flight_client)));
    closed_ = false;

    // Note: This should likely come from Flight instead of being from the
    // connection properties to allow reporting a user for other auth mechanisms
    // and also decouple the database user from user credentials.
    info_.SetProperty(SQL_USER_NAME, auth_method->GetUser());
    attribute_[CONNECTION_DEAD] = static_cast<uint32_t>(SQL_FALSE);

    PopulateCallOptions(properties);
  } catch (...) {
    attribute_[CONNECTION_DEAD] = static_cast<uint32_t>(SQL_TRUE);
    sql_client_.reset();

    throw;
  }
}

const FlightCallOptions &
FlightSqlConnection::PopulateCallOptions(const ConnPropertyMap &props) {
  // Set CONNECTION_TIMEOUT attribute or LOGIN_TIMEOUT depending on if this
  // is the first request.
  const boost::optional<Connection::Attribute> &connection_timeout = closed_ ?
      GetAttribute(LOGIN_TIMEOUT) : GetAttribute(CONNECTION_TIMEOUT);
  if (connection_timeout && boost::get<uint32_t>(*connection_timeout) > 0) {
    call_options_.timeout =
        TimeoutDuration{static_cast<double>(boost::get<uint32_t>(*connection_timeout))};
  }

  for (auto prop : props) {
    if (BUILT_IN_PROPERTIES.count(prop.first) != 0) {
      continue;
    }

    // Note: header names must be lower case for gRPC.
    std::string key_lc = boost::algorithm::to_lower_copy(prop.first);
    call_options_.headers.emplace_back(std::make_pair(key_lc, prop.second));
  }

  return call_options_;
}

FlightClientOptions
FlightSqlConnection::BuildFlightClientOptions(const ConnPropertyMap &properties,
                                              std::vector<std::string> &missing_attr,
                                              const std::shared_ptr<FlightSqlSslConfig>& ssl_config) {
  FlightClientOptions options;
  // Persist state information using cookies if the FlightProducer supports it.
  options.middleware.push_back(arrow::flight::GetCookieFactory());

  if (ssl_config->isUseEncryption()) {
    if (ssl_config->isDisableCertificateVerification()) {
      options.disable_server_verification = ssl_config->isDisableCertificateVerification();
    } else {
      if (ssl_config->isSystemTrustStore()) {
        X509_STORE* store = X509_STORE_new();
        std::string certs;
        loadSystemTrustStore(certs);
        options.tls_root_certs = certs;
      } else if (!ssl_config->getTrustedCerts().empty()) {
        flight::CertKeyPair cert_key_pair;
        ThrowIfNotOK(ssl_config->readCerts(&cert_key_pair));
        options.tls_root_certs = cert_key_pair.pem_cert;
      }
    }
  }

  return std::move(options);
}

Location
FlightSqlConnection::BuildLocation(const ConnPropertyMap &properties,
                                   std::vector<std::string> &missing_attr,
                                   const std::shared_ptr<FlightSqlSslConfig>& ssl_config) {
  const auto &host_iter =
      TrackMissingRequiredProperty(HOST, properties, missing_attr);

  const auto &port_iter =
      TrackMissingRequiredProperty(PORT, properties, missing_attr);

  if (!missing_attr.empty()) {
    std::string missing_attr_str =
        std::string("Missing required properties: ") +
        boost::algorithm::join(missing_attr, ", ");
    throw DriverException(missing_attr_str);
  }

  const std::string &host = host_iter->second;
  const int &port = boost::lexical_cast<int>(port_iter->second);

  Location location;
  if (ssl_config->isUseEncryption()) {
    ThrowIfNotOK(Location::ForGrpcTls(host, port, &location));
  } else {
    ThrowIfNotOK(Location::ForGrpcTcp(host, port, &location));
  }
  return location;
}

void FlightSqlConnection::Close() {
  if (closed_) {
    throw DriverException("Connection already closed.");
  }

  sql_client_.reset();
  closed_ = true;
  attribute_[CONNECTION_DEAD] = static_cast<uint32_t>(SQL_TRUE);
}

std::shared_ptr<Statement> FlightSqlConnection::CreateStatement() {
  return std::shared_ptr<Statement>(
      new FlightSqlStatement(diagnostics_, *sql_client_, call_options_));
}

bool FlightSqlConnection::SetAttribute(Connection::AttributeId attribute,
                                       const Connection::Attribute &value) {
  switch (attribute) {
  case ACCESS_MODE:
    // We will always return read-write.
    return CheckIfSetToOnlyValidValue(value, static_cast<uint32_t>(SQL_MODE_READ_WRITE));
  case PACKET_SIZE:
    return CheckIfSetToOnlyValidValue(value, static_cast<uint32_t>(0));
  default:
    attribute_[attribute] = value;
    return true;
  }
}

boost::optional<Connection::Attribute>
FlightSqlConnection::GetAttribute(Connection::AttributeId attribute) {
  switch (attribute) {
  case ACCESS_MODE:
    // FlightSQL does not provide this metadata.
    return boost::make_optional(Attribute(static_cast<uint32_t>(SQL_MODE_READ_WRITE)));
  case PACKET_SIZE:
    return boost::make_optional(Attribute(static_cast<uint32_t>(0)));
  default:
    const auto &it = attribute_.find(attribute);
    return boost::make_optional(it != attribute_.end(), it->second);
  }
}

Connection::Info FlightSqlConnection::GetInfo(uint16_t info_type) {
  auto result = info_.GetInfo(info_type);
  if (info_type == SQL_DBMS_NAME) {
    // Update the database component reported in error messages.
    // We do this lazily for performance reasons.
    diagnostics_.SetDataSourceComponent(boost::get<std::string>(result));
  }
  return result;
}

FlightSqlConnection::FlightSqlConnection(OdbcVersion odbc_version)
    : diagnostics_("Apache Arrow", "Flight SQL", odbc_version),
      odbc_version_(odbc_version), info_(call_options_, sql_client_),
      closed_(true) {
  attribute_[CONNECTION_DEAD] = static_cast<uint32_t>(SQL_TRUE);
  attribute_[LOGIN_TIMEOUT] = static_cast<uint32_t>(0);
  attribute_[CONNECTION_TIMEOUT] = static_cast<uint32_t>(0);
  attribute_[CURRENT_CATALOG] = "";
}
odbcabstraction::Diagnostics &FlightSqlConnection::GetDiagnostics() {
  return diagnostics_;
}
} // namespace flight_sql
} // namespace driver

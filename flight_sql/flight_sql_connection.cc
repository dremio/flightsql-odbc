/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include "flight_sql_connection.h"

#include <odbcabstraction/platform.h>
#include <odbcabstraction/utils.h>

#include <arrow/flight/types.h>
#include <arrow/flight/client_cookie_middleware.h>
#include "address_info.h"
#include "flight_sql_auth_method.h"
#include "flight_sql_statement.h"
#include "flight_sql_ssl_config.h"
#include "utils.h"

#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/asio/ip/address.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/optional.hpp>
#include <odbcabstraction/exceptions.h>

#include <sql.h>
#include <sqlext.h>

#include "system_trust_store.h"

#ifndef NI_MAXHOST
#define NI_MAXHOST 1025
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
using driver::odbcabstraction::CommunicationException;
using driver::odbcabstraction::OdbcVersion;
using driver::odbcabstraction::Statement;

const std::string FlightSqlConnection::DSN = "dsn";
const std::string FlightSqlConnection::DRIVER = "driver";
const std::string FlightSqlConnection::HOST = "host";
const std::string FlightSqlConnection::PORT = "port";
const std::string FlightSqlConnection::USER = "user";
const std::string FlightSqlConnection::USER_ID = "user id";
const std::string FlightSqlConnection::UID = "uid";
const std::string FlightSqlConnection::PASSWORD = "password";
const std::string FlightSqlConnection::PWD = "pwd";
const std::string FlightSqlConnection::TOKEN = "token";
const std::string FlightSqlConnection::USE_ENCRYPTION = "useEncryption";
const std::string FlightSqlConnection::DISABLE_CERTIFICATE_VERIFICATION = "disableCertificateVerification";
const std::string FlightSqlConnection::TRUSTED_CERTS = "trustedCerts";
const std::string FlightSqlConnection::USE_SYSTEM_TRUST_STORE = "useSystemTrustStore";
const std::string FlightSqlConnection::STRING_COLUMN_LENGTH = "StringColumnLength";
const std::string FlightSqlConnection::USE_WIDE_CHAR = "UseWideChar";
const std::string FlightSqlConnection::CHUNK_BUFFER_CAPACITY = "ChunkBufferCapacity";

const std::vector<std::string> FlightSqlConnection::ALL_KEYS = {
    FlightSqlConnection::DSN, FlightSqlConnection::DRIVER, FlightSqlConnection::HOST, FlightSqlConnection::PORT,
    FlightSqlConnection::TOKEN, FlightSqlConnection::UID, FlightSqlConnection::USER_ID, FlightSqlConnection::PWD,
    FlightSqlConnection::USE_ENCRYPTION, FlightSqlConnection::TRUSTED_CERTS, FlightSqlConnection::USE_SYSTEM_TRUST_STORE,
    FlightSqlConnection::DISABLE_CERTIFICATE_VERIFICATION, FlightSqlConnection::STRING_COLUMN_LENGTH,
    FlightSqlConnection::USE_WIDE_CHAR, FlightSqlConnection::CHUNK_BUFFER_CAPACITY};

namespace {

#if _WIN32 || _WIN64
constexpr auto SYSTEM_TRUST_STORE_DEFAULT = true;
constexpr auto STORES = {
    "CA",
    "MY",
    "ROOT",
    "SPC"
};

inline std::string GetCerts() {
  std::string certs;

  for (auto store : STORES) {
    std::shared_ptr<SystemTrustStore> cert_iterator = std::make_shared<SystemTrustStore>(store);

    if (!cert_iterator->SystemHasStore()) {
      // If the system does not have the specific store, we skip it using the continue.
      continue;
    }
    while (cert_iterator->HasNext()) {
      certs += cert_iterator->GetNext();
    }
  }

  return certs;
}

#else

constexpr auto SYSTEM_TRUST_STORE_DEFAULT = false;
inline std::string GetCerts() {
  return "";
}

#endif

const std::set<std::string, odbcabstraction::CaseInsensitiveComparator> BUILT_IN_PROPERTIES = {
    FlightSqlConnection::HOST,
    FlightSqlConnection::PORT,
    FlightSqlConnection::USER,
    FlightSqlConnection::USER_ID,
    FlightSqlConnection::UID,
    FlightSqlConnection::PASSWORD,
    FlightSqlConnection::PWD,
    FlightSqlConnection::TOKEN,
    FlightSqlConnection::USE_ENCRYPTION,
    FlightSqlConnection::DISABLE_CERTIFICATE_VERIFICATION,
    FlightSqlConnection::TRUSTED_CERTS,
    FlightSqlConnection::USE_SYSTEM_TRUST_STORE,
    FlightSqlConnection::STRING_COLUMN_LENGTH,
    FlightSqlConnection::USE_WIDE_CHAR
};

Connection::ConnPropertyMap::const_iterator
TrackMissingRequiredProperty(const std::string &property,
                             const Connection::ConnPropertyMap &properties,
                             std::vector<std::string> &missing_attr) {
  auto prop_iter =
      properties.find(property);
  if (properties.end() == prop_iter) {
    missing_attr.push_back(property);
  }
  return prop_iter;
}
} // namespace

std::shared_ptr<FlightSqlSslConfig> LoadFlightSslConfigs(const Connection::ConnPropertyMap &connPropertyMap) {
  LOG_TRACE("[{}] Entering function", __FUNCTION__);
  bool use_encryption = AsBool(connPropertyMap, FlightSqlConnection::USE_ENCRYPTION).value_or(true);
  bool disable_cert = AsBool(connPropertyMap, FlightSqlConnection::DISABLE_CERTIFICATE_VERIFICATION).value_or(false);
  bool use_system_trusted = AsBool(connPropertyMap, FlightSqlConnection::USE_SYSTEM_TRUST_STORE).value_or(SYSTEM_TRUST_STORE_DEFAULT);

  auto trusted_certs_iterator = connPropertyMap.find(
    FlightSqlConnection::TRUSTED_CERTS);
  auto trusted_certs =
    trusted_certs_iterator != connPropertyMap.end() ? trusted_certs_iterator->second : "";

  const std::shared_ptr<FlightSqlSslConfig> &return_ptr = std::make_shared<FlightSqlSslConfig>(disable_cert,
                                                                                               trusted_certs,
                                                                                               use_system_trusted,
                                                                                               use_encryption);

  LOG_TRACE("[{}] Exiting successfully with FlightSqlSslConfig: disableCertificateVerification '{}', trustedCerts '{}', systemTrustStore '{}', useEncryption '{}'",             __FUNCTION__, disable_cert, trusted_certs, use_system_trusted, use_encryption);
  return return_ptr;
}
void FlightSqlConnection::Connect(const ConnPropertyMap &properties,
                                  std::vector<std::string> &missing_attr) {
  LOG_TRACE("[{}] Entering function", __FUNCTION__);
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
    LOG_TRACE("[{}] Attempting to connect using the FlightClient", __FUNCTION__);
    ThrowIfNotOK(
      FlightClient::Connect(location, client_options, &flight_client));

    std::unique_ptr<FlightSqlAuthMethod> auth_method =
      FlightSqlAuthMethod::FromProperties(flight_client, properties);
    LOG_TRACE("[{}] Attempting to authenticate", __FUNCTION__);
    auth_method->Authenticate(*this, call_options_);

    sql_client_.reset(new FlightSqlClient(std::move(flight_client)));
    closed_ = false;
    LOG_TRACE("[{}] Connected successfully and reset to a new FlightSqlClient", __FUNCTION__);

    // Note: This should likely come from Flight instead of being from the
    // connection properties to allow reporting a user for other auth mechanisms
    // and also decouple the database user from user credentials.
    info_.SetProperty(SQL_USER_NAME, auth_method->GetUser());
    attribute_[CONNECTION_DEAD] = static_cast<uint32_t>(SQL_FALSE);

    PopulateMetadataSettings(properties);
    PopulateCallOptions(properties);
  } catch (...) {
    attribute_[CONNECTION_DEAD] = static_cast<uint32_t>(SQL_TRUE);
    sql_client_.reset();

    throw;
  }
}

void FlightSqlConnection::PopulateMetadataSettings(const Connection::ConnPropertyMap &conn_property_map) {
  LOG_TRACE("[{}] Entering function", __FUNCTION__);
  metadata_settings_.string_column_length_ = GetStringColumnLength(conn_property_map);
  metadata_settings_.use_wide_char_ = GetUseWideChar(conn_property_map);
  metadata_settings_.chunk_buffer_capacity_ = GetChunkBufferCapacity(conn_property_map);
  LOG_TRACE("[{}] Exiting successfully with no return value but defined: StringColumnLength '{}', UseWideChar '{}'", __FUNCTION__, metadata_settings_.string_column_length_.value(), metadata_settings_.use_wide_char_);
}

boost::optional<int32_t> FlightSqlConnection::GetStringColumnLength(const Connection::ConnPropertyMap &conn_property_map) {
  LOG_TRACE("[{}] Entering function", __FUNCTION__);
  const int32_t min_string_column_length = 1;
  boost::optional<int32_t> return_optional = boost::none;

  try {
    return_optional = AsInt32(min_string_column_length, conn_property_map, FlightSqlConnection::STRING_COLUMN_LENGTH);
  } catch (const std::exception& e) {
    const std::string message = std::string(
        "Invalid value for connection property " + FlightSqlConnection::STRING_COLUMN_LENGTH +
        ". Please ensure it has a valid numeric value. Message: " + e.what());
    LOG_WARN(("[{}] " + message), __FUNCTION__)
    diagnostics_.AddWarning(message, "01000", odbcabstraction::ODBCErrorCodes_GENERAL_WARNING);
  }

  LOG_TRACE("[{}] Exiting successfully with Optional", __FUNCTION__);
  return return_optional;
}

bool FlightSqlConnection::GetUseWideChar(const ConnPropertyMap &connPropertyMap) {
  LOG_TRACE("[{}] Entering function", __FUNCTION__);
#if defined _WIN32 || defined _WIN64
  // Windows should use wide chars by default
  bool default_value = true;
#else
  // Mac and Linux should not use wide chars by default
  bool default_value = false;
#endif
  bool value = AsBool(connPropertyMap, FlightSqlConnection::USE_WIDE_CHAR).value_or(default_value);
  LOG_TRACE("[{}] Exiting successfully with bool {}", __FUNCTION__, value);
  return value;
}

size_t FlightSqlConnection::GetChunkBufferCapacity(const ConnPropertyMap &connPropertyMap) {
  size_t default_value = 5;
  try {
    return AsInt32(1, connPropertyMap, FlightSqlConnection::CHUNK_BUFFER_CAPACITY).value_or(default_value);
  } catch (const std::exception& e) {
    diagnostics_.AddWarning(
            std::string("Invalid value for connection property " + FlightSqlConnection::CHUNK_BUFFER_CAPACITY +
                        ". Please ensure it has a valid numeric value. Message: " + e.what()),
            "01000", odbcabstraction::ODBCErrorCodes_GENERAL_WARNING);
  }

  return default_value;
}

const FlightCallOptions &
FlightSqlConnection::PopulateCallOptions(const ConnPropertyMap &props) {
  LOG_TRACE("[{}] Entering function", __FUNCTION__);
  // Set CONNECTION_TIMEOUT attribute or LOGIN_TIMEOUT depending on if this
  // is the first request.
  const boost::optional<Connection::Attribute> &connection_timeout = closed_ ?
      GetAttribute(LOGIN_TIMEOUT) : GetAttribute(CONNECTION_TIMEOUT);
  if (connection_timeout && boost::get<uint32_t>(*connection_timeout) > 0) {
    call_options_.timeout =
        TimeoutDuration{static_cast<double>(boost::get<uint32_t>(*connection_timeout))};
  }

  for (const auto& prop : props) {
    if (BUILT_IN_PROPERTIES.count(prop.first) != 0) {
      continue;
    }

    if (prop.first.find(' ') != std::string::npos) {
      // Connection properties containing spaces will crash gRPC, but some tools
      // such as the OLE DB to ODBC bridge generate unused properties containing spaces.
      const std::string message = std::string(
          "Ignoring connection option " + prop.first +
          ". Server-specific options must be valid HTTP header names and cannot contain spaces.");
      LOG_WARN(("[{}] " + message), __FUNCTION__)
      diagnostics_.AddWarning(message, "01000", odbcabstraction::ODBCErrorCodes_GENERAL_WARNING);
      continue;
    }

    // Note: header names must be lower-case for gRPC.
    // gRPC will crash if they are not lower-case.
    std::string key_lc = boost::algorithm::to_lower_copy(prop.first);
    call_options_.headers.emplace_back(std::make_pair(key_lc, prop.second));
  }

  LOG_TRACE("[{}] Exiting successfully with FlightCallOptions", __FUNCTION__);
  return call_options_;
}

FlightClientOptions
FlightSqlConnection::BuildFlightClientOptions(const ConnPropertyMap &properties,
                                              std::vector<std::string> &missing_attr,
                                              const std::shared_ptr<FlightSqlSslConfig>& ssl_config) {
  LOG_TRACE("[{}] Entering function", __FUNCTION__);

  FlightClientOptions options;
  // Persist state information using cookies if the FlightProducer supports it.
  options.middleware.push_back(arrow::flight::GetCookieFactory());

  if (ssl_config->useEncryption()) {
    if (ssl_config->shouldDisableCertificateVerification()) {
      LOG_TRACE("[{}] Disabling server certificate verification", __FUNCTION__);
      options.disable_server_verification = ssl_config->shouldDisableCertificateVerification();
    } else {
      if (ssl_config->useSystemTrustStore()) {
        LOG_TRACE("[{}] Using certificates from the system", __FUNCTION__);
        const std::string certs = GetCerts();

        options.tls_root_certs = certs;
      } else if (!ssl_config->getTrustedCerts().empty()) {
        LOG_TRACE("[{}] Using certificates from user input", __FUNCTION__);
        flight::CertKeyPair cert_key_pair;
        ssl_config->populateOptionsWithCerts(&cert_key_pair);
        options.tls_root_certs = cert_key_pair.pem_cert;
      }
    }
  }

  LOG_TRACE("[{}] Exiting successfully with FlightClientOptions", __FUNCTION__);
  return std::move(options);
}

Location
FlightSqlConnection::BuildLocation(const ConnPropertyMap &properties,
                                   std::vector<std::string> &missing_attr,
                                   const std::shared_ptr<FlightSqlSslConfig>& ssl_config) {
  LOG_TRACE("[{}] Entering function", __FUNCTION__);

  const auto &host_iter =
      TrackMissingRequiredProperty(HOST, properties, missing_attr);

  const auto &port_iter =
      TrackMissingRequiredProperty(PORT, properties, missing_attr);

  if (!missing_attr.empty()) {
    std::string missing_attr_str =
        std::string("Missing required properties: ") +
        boost::algorithm::join(missing_attr, ", ");
    LOG_ERROR(("[{}] " + missing_attr_str), __FUNCTION__);
    throw DriverException(missing_attr_str);
  }

  const std::string &host = host_iter->second;
  const int &port = boost::lexical_cast<int>(port_iter->second);

  Location location;
  if (ssl_config->useEncryption()) {
    AddressInfo address_info;
    char host_name_info[NI_MAXHOST] = "";
    bool operation_result = false;

    try {
      auto ip_address = boost::asio::ip::make_address(host);
      // We should only attempt to resolve the hostname from the IP if the given
      // HOST input is an IP address.
      if (ip_address.is_v4() || ip_address.is_v6()) {
        operation_result = address_info.GetAddressInfo(host, host_name_info,
                                                           NI_MAXHOST);
        if (operation_result) {
          ThrowIfNotOK(Location::ForGrpcTls(host_name_info, port, &location));
          LOG_TRACE("[{}] Exiting successfully with a GRPC_TLS Location from an IP address", __FUNCTION__);
          return location;
        }
        LOG_WARN("[{}] Couldn't resolve the IP address to a hostname", __FUNCTION__)
      }
    }
    catch (...) {
      // This is expected. The Host attribute can be an IP or name, but make_address will throw
      // if it is not an IP.
    }

    ThrowIfNotOK(Location::ForGrpcTls(host, port, &location));
    LOG_TRACE("[{}] Exiting successfully with a GRPC_TLS Location from a hostname", __FUNCTION__);
    return location;
  }

  ThrowIfNotOK(Location::ForGrpcTcp(host, port, &location));
  LOG_TRACE("[{}] Exiting successfully with a GRPC_TCP Location from a hostname", __FUNCTION__);
  return location;
}

void FlightSqlConnection::Close() {
  LOG_TRACE("[{}] Entering function", __FUNCTION__);
  if (closed_) {
    const std::string message = "Connection already closed.";
    LOG_ERROR(("[{}] " + message), __FUNCTION__);
    throw DriverException(message);
  }

  sql_client_.reset();
  closed_ = true;
  attribute_[CONNECTION_DEAD] = static_cast<uint32_t>(SQL_TRUE);
  LOG_TRACE("[{}] Exiting successfully with no return value", __FUNCTION__);
}

std::shared_ptr<Statement> FlightSqlConnection::CreateStatement() {
  LOG_TRACE("[{}] Entering function", __FUNCTION__);
  const std::shared_ptr<Statement> &return_ptr = std::shared_ptr<Statement>(
      new FlightSqlStatement(
          diagnostics_,
          *sql_client_,
          call_options_,
          metadata_settings_
      )
  );

  LOG_TRACE("[{}] Exiting successfully with Statement", __FUNCTION__);
  return return_ptr;
}

bool FlightSqlConnection::SetAttribute(Connection::AttributeId attribute,
                                       const Connection::Attribute &value) {
  LOG_TRACE("[{}] Entry with parameters: attribute '{}'", __FUNCTION__, attribute);
  bool return_bool = false;
  switch (attribute) {
    case ACCESS_MODE:
      // We will always return read-write.
      return_bool = CheckIfSetToOnlyValidValue(value, static_cast<uint32_t>(SQL_MODE_READ_WRITE));
      break;
    case PACKET_SIZE:
      return_bool = CheckIfSetToOnlyValidValue(value, static_cast<uint32_t>(0));
      break;
    default:
      attribute_[attribute] = value;
      return_bool = true;
      break;
  }

  LOG_TRACE("[{}] Exiting successfully with bool {}", __FUNCTION__, return_bool);
  return return_bool;
}

boost::optional<Connection::Attribute>
FlightSqlConnection::GetAttribute(Connection::AttributeId attribute) {
  LOG_TRACE("[{}] Entry with parameters: attribute '{}'", __FUNCTION__, attribute);

  boost::optional<Connection::Attribute> return_optional = boost::none;
  switch (attribute) {
  case ACCESS_MODE:
    // FlightSQL does not provide this metadata.
    return_optional = boost::make_optional(Attribute(static_cast<uint32_t>(SQL_MODE_READ_WRITE)));
    break;
  case PACKET_SIZE:
    return_optional = boost::make_optional(Attribute(static_cast<uint32_t>(0)));
    break;
  default:
    const auto &it = attribute_.find(attribute);
    return_optional = boost::make_optional(it != attribute_.end(), it->second);
    break;
  }

  LOG_TRACE("[{}] Exiting successfully with Optional", __FUNCTION__);
  return return_optional;
}

Connection::Info FlightSqlConnection::GetInfo(uint16_t info_type) {
  LOG_TRACE("[{}] Entry with parameters: info_type '{}'", __FUNCTION__, info_type);

  auto result = info_.GetInfo(info_type);
  if (info_type == SQL_DBMS_NAME || info_type == SQL_SERVER_NAME) {
    // Update the database component reported in error messages.
    // We do this lazily for performance reasons.
    LOG_TRACE("[{}] Lazy updating DataSourceComponent with the result", __FUNCTION__);
    diagnostics_.SetDataSourceComponent(boost::get<std::string>(result));
  }

  LOG_TRACE("[{}] Exiting successfully with Info", __FUNCTION__);
  return result;
}

FlightSqlConnection::FlightSqlConnection(OdbcVersion odbc_version, const std::string &driver_version)
    : diagnostics_("Apache Arrow", "Flight SQL", odbc_version),
      odbc_version_(odbc_version), info_(call_options_, sql_client_, driver_version),
      closed_(true) {
  attribute_[CONNECTION_DEAD] = static_cast<uint32_t>(SQL_TRUE);
  attribute_[LOGIN_TIMEOUT] = static_cast<uint32_t>(0);
  attribute_[CONNECTION_TIMEOUT] = static_cast<uint32_t>(0);
  attribute_[CURRENT_CATALOG] = "";
}
odbcabstraction::Diagnostics &FlightSqlConnection::GetDiagnostics() {
  return diagnostics_;
}

void FlightSqlConnection::SetClosed(bool is_closed) {
  LOG_TRACE("[{}] Entry with parameters: is_closed '{}'", __FUNCTION__, is_closed);
  closed_ = is_closed;
  LOG_TRACE("[{}] Exiting successfully with no return value", __FUNCTION__);
}

} // namespace flight_sql
} // namespace driver

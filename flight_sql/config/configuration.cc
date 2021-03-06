/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include "config/configuration.h"

#include "flight_sql_connection.h"
#include <sstream>
#include <iterator>
#include <odbcinst.h>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/algorithm/copy.hpp>

namespace driver {
namespace flight_sql {
namespace config {

static const std::string DEFAULT_DSN = "Apache Arrow Flight SQL";
static const std::string DEFAULT_ENABLE_ENCRYPTION = TRUE_STR;
static const std::string DEFAULT_USE_CERT_STORE = TRUE_STR;
static const std::string DEFAULT_DISABLE_CERT_VERIFICATION = FALSE_STR;

namespace {
std::string ReadDsnString(const std::string& dsn, const std::string& key, const std::string& dflt = "")
{
    #define BUFFER_SIZE (1024)
    std::vector<char> buf(BUFFER_SIZE);

    int ret = SQLGetPrivateProfileString(dsn.c_str(), key.c_str(), dflt.c_str(), buf.data(), static_cast<int>(buf.size()), "ODBC.INI");

    if (ret > BUFFER_SIZE)
    {
        // If there wasn't enough space, try again with the right size buffer.
        buf.resize(ret + 1);
        ret = SQLGetPrivateProfileString(dsn.c_str(), key.c_str(), dflt.c_str(), buf.data(), static_cast<int>(buf.size()), "ODBC.INI");
    }

    return std::string(buf.data(), ret);
}

void RemoveAllKnownKeys(std::vector<std::string>& keys) {
    // Remove all known DSN keys from the passed in set of keys, case insensitively.
    keys.erase(std::remove_if(keys.begin(), keys.end(), [&](auto& x) {
        return std::find_if(FlightSqlConnection::ALL_KEYS.begin(), FlightSqlConnection::ALL_KEYS.end(), [&](auto& s) {
            return boost::iequals(x, s);}) != FlightSqlConnection::ALL_KEYS.end();
        }), keys.end());
}

std::vector<std::string> ReadAllKeys(const std::string& dsn)
{
    std::vector<char> buf(BUFFER_SIZE);

    int ret = SQLGetPrivateProfileString(dsn.c_str(), NULL, "", buf.data(), static_cast<int>(buf.size()), "ODBC.INI");

    if (ret > BUFFER_SIZE)
    {
        // If there wasn't enough space, try again with the right size buffer.
        buf.resize(ret + 1);
        ret = SQLGetPrivateProfileString(dsn.c_str(), NULL, "", buf.data(), static_cast<int>(buf.size()), "ODBC.INI");
    }

    // When you pass NULL to SQLGetPrivateProfileString it gives back a \0 delimited list of all the keys.
    // The below loop simply tokenizes all the keys and places them into a vector.
    std::vector<std::string> keys;
    char* begin = buf.data();
    while (begin && *begin != '\0') {
        char* cur;
        for (cur = begin; *cur != '\0'; ++cur);
        keys.emplace_back(begin, cur);
        begin = ++cur;
    }
    return keys;
}
}

Configuration::Configuration()
{
    // No-op.
}

Configuration::~Configuration()
{
    // No-op.
}

void Configuration::LoadDefaults()
{
    Set(FlightSqlConnection::DSN, DEFAULT_DSN);
    Set(FlightSqlConnection::USE_ENCRYPTION, DEFAULT_ENABLE_ENCRYPTION);
    Set(FlightSqlConnection::USE_SYSTEM_TRUST_STORE, DEFAULT_USE_CERT_STORE);
    Set(FlightSqlConnection::DISABLE_CERTIFICATE_VERIFICATION, DEFAULT_DISABLE_CERT_VERIFICATION);
}

void Configuration::LoadDsn(const std::string& dsn)
{
    Set(FlightSqlConnection::DSN, dsn);
    Set(FlightSqlConnection::HOST, ReadDsnString(dsn, FlightSqlConnection::HOST));
    Set(FlightSqlConnection::PORT, ReadDsnString(dsn, FlightSqlConnection::PORT));
    Set(FlightSqlConnection::TOKEN, ReadDsnString(dsn, FlightSqlConnection::TOKEN));
    Set(FlightSqlConnection::UID, ReadDsnString(dsn, FlightSqlConnection::UID));
    Set(FlightSqlConnection::PWD, ReadDsnString(dsn, FlightSqlConnection::PWD));
    Set(FlightSqlConnection::USE_ENCRYPTION, 
        ReadDsnString(dsn, FlightSqlConnection::USE_ENCRYPTION, DEFAULT_ENABLE_ENCRYPTION));
    Set(FlightSqlConnection::TRUSTED_CERTS, ReadDsnString(dsn, FlightSqlConnection::TRUSTED_CERTS));
    Set(FlightSqlConnection::USE_SYSTEM_TRUST_STORE, 
        ReadDsnString(dsn, FlightSqlConnection::USE_SYSTEM_TRUST_STORE, DEFAULT_USE_CERT_STORE));
    Set(FlightSqlConnection::DISABLE_CERTIFICATE_VERIFICATION, 
        ReadDsnString(dsn, FlightSqlConnection::DISABLE_CERTIFICATE_VERIFICATION, DEFAULT_DISABLE_CERT_VERIFICATION));

    auto customKeys = ReadAllKeys(dsn);
    RemoveAllKnownKeys(customKeys);
    for (auto key : customKeys) {
        Set(key, ReadDsnString(dsn, key));
    }
}

void Configuration::Clear()
{
    this->properties.clear();
}

bool Configuration::IsSet(const std::string& key) const
{
    return 0 != this->properties.count(key);
}

const std::string& Configuration::Get(const std::string& key) const
{
    const auto itr = this->properties.find(key);
    if (itr == this->properties.cend()) {
        static const std::string empty("");
        return empty;
    }
    return itr->second;
}

void Configuration::Set(const std::string& key, const std::string& value)
{
    const std::string copy = boost::trim_copy(value);
    if (!copy.empty()) {
        this->properties[key] = value;
    }
}

const driver::odbcabstraction::Connection::ConnPropertyMap& Configuration::GetProperties() const
{
    return this->properties;
}

std::vector<std::string> Configuration::GetCustomKeys() const
{
    driver::odbcabstraction::Connection::ConnPropertyMap copyProps(properties);
    for (auto& key : FlightSqlConnection::ALL_KEYS) {
        copyProps.erase(key);
    }
    std::vector<std::string> keys;
    boost::copy(copyProps | boost::adaptors::map_keys, std::back_inserter(keys));
    return keys;
}

} // namespace config
} // namespace flight_sql
} // namespace driver

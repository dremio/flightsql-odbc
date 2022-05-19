/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "include/flight_sql/config/configuration.h"

#include <string>
#include <sstream>
#include <iterator>
#include <odbcinst.h>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/algorithm/copy.hpp>

namespace driver {
namespace config {

const std::string Key::DSN = "dsn";
const std::string Key::DRIVER = "driver";
const std::string Key::HOST = "host";
const std::string Key::PORT = "port";
const std::string Key::AUTH_TOKEN = "token";
const std::string Key::UID = "uid";
const std::string Key::PWD = "pwd";
const std::string Key::USE_TLS = "UseEncryption";
const std::string Key::ENCRYPTION_CERT = "TrustedCerts";
const std::string Key::USE_CERT_STORE = "UseSystemTrustStore";
const std::string Key::DISABLE_CERT_VERIFICATION = "DisableCertificateVerification";
static const std::vector<std::string> ALL_KEYS = { 
    Key::DSN, Key::DRIVER, Key::HOST, Key::PORT, Key::AUTH_TOKEN, Key::UID, Key::PWD, Key::USE_TLS, 
    Key::ENCRYPTION_CERT, Key::USE_CERT_STORE, Key::DISABLE_CERT_VERIFICATION };

static const std::string defaultDsn = "Apache Arrow Flight SQL";
static const std::string defaultPort = "32010";
static const std::string defaultEnableEncryption = TRUE_STR;
static const std::string defaultUseCertStore = TRUE_STR;
static const std::string defaultDisableCertificationVerification = FALSE_STR;

namespace {
std::string ReadDsnString(const std::string& dsn, const std::string& key, const std::string& dflt = "")
{
    #define BUFFER_SIZE (1024)
    std::vector<char> buf(BUFFER_SIZE);

    int ret = SQLGetPrivateProfileString(dsn.c_str(), key.c_str(), dflt.c_str(), buf.data(), static_cast<int>(buf.size()), "ODBC.INI");

    if (ret > BUFFER_SIZE)
    {
        buf.resize(ret + 1);
        ret = SQLGetPrivateProfileString(dsn.c_str(), key.c_str(), dflt.c_str(), buf.data(), static_cast<int>(buf.size()), "ODBC.INI");
    }

    return std::string(buf.data(), ret);
}

void RemoveAllKnownKeys(std::vector<std::string>& keys) {
    keys.erase(std::remove_if(keys.begin(), keys.end(), [&](auto& x) {
        return std::find_if(ALL_KEYS.begin(), ALL_KEYS.end(), [&](auto& s) {return boost::iequals(x, s);}) != ALL_KEYS.end();
        }), keys.end());
}

std::vector<std::string> ReadAllKeys(const std::string& dsn)
{
    std::vector<char> buf(BUFFER_SIZE);

    int ret = SQLGetPrivateProfileString(dsn.c_str(), NULL, "", buf.data(), static_cast<int>(buf.size()), "ODBC.INI");

    if (ret > BUFFER_SIZE)
    {
        buf.resize(ret + 1);
        ret = SQLGetPrivateProfileString(dsn.c_str(), NULL, "", buf.data(), static_cast<int>(buf.size()), "ODBC.INI");
    }

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
    Set(Key::DSN, defaultDsn);
    Set(Key::PORT, defaultPort);
    Set(Key::USE_TLS, defaultEnableEncryption);
    Set(Key::USE_CERT_STORE, defaultUseCertStore);
    Set(Key::DISABLE_CERT_VERIFICATION, defaultDisableCertificationVerification);
}

void Configuration::LoadDsn(const std::string& dsn)
{
    Set(Key::DSN, dsn);
    Set(Key::HOST, ReadDsnString(dsn, Key::HOST));
    Set(Key::PORT, ReadDsnString(dsn, Key::PORT, defaultPort));
    Set(Key::AUTH_TOKEN, ReadDsnString(dsn, Key::AUTH_TOKEN));
    Set(Key::UID, ReadDsnString(dsn, Key::UID));
    Set(Key::PWD, ReadDsnString(dsn, Key::PWD));
    Set(Key::USE_TLS, ReadDsnString(dsn, Key::USE_TLS, defaultEnableEncryption));
    Set(Key::ENCRYPTION_CERT, ReadDsnString(dsn, Key::ENCRYPTION_CERT));
    Set(Key::USE_CERT_STORE, ReadDsnString(dsn, Key::USE_CERT_STORE, defaultUseCertStore));
    Set(Key::DISABLE_CERT_VERIFICATION, ReadDsnString(dsn, Key::DISABLE_CERT_VERIFICATION, defaultDisableCertificationVerification));

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
    for (auto& key : ALL_KEYS) {
        copyProps.erase(key);
    }
    std::vector<std::string> keys;
    boost::copy(copyProps | boost::adaptors::map_keys, std::back_inserter(keys));
    return keys;
}

}
}

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

namespace driver {
namespace config {

const std::string Key::dsn = "dsn";
const std::string Key::driver = "driver";
const std::string Key::server = "server";
const std::string Key::port = "port";
const std::string Key::auth_token = "token";
const std::string Key::uid = "uid";
const std::string Key::pwd = "pwd";
const std::string Key::enable_encryption = "enable_encryption";
const std::string Key::encryption_cert = "cert";
const std::string Key::use_cert_store = "use_cert_store";

static const std::string defaultDsn = "Apache Arrow Flight SQL";
static const std::string defaultPort = "32010";
static const std::string defaultEnableEncryption = TRUE_STR;
static const std::string defaultUseCertStore = TRUE_STR;

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

Configuration::Configuration()
{
    // No-op.
}

Configuration::~Configuration()
{
    // No-op.
}

std::string Configuration::ToConnectString() const
{
    std::stringstream connect_string_buffer;

    for (auto it = this->properties.cbegin(); it != this->properties.cend(); ++it)
    {
        const std::string& key = it->first;
        auto& value = it->second;

        if (value.empty())
            continue;

        if (value.find(' ') == std::string::npos)
            connect_string_buffer << key << '=' << value << ';';
        else
            connect_string_buffer << key << "={" << value << "};";
    }

    return connect_string_buffer.str();
}

void Configuration::LoadDefaults()
{
    Set(Key::dsn, defaultDsn);
    Set(Key::port, defaultPort);
    Set(Key::enable_encryption, defaultEnableEncryption);
    Set(Key::use_cert_store, defaultUseCertStore);
}

void Configuration::LoadDsn(const std::string& dsn)
{
    Set(Key::dsn, dsn);
    Set(Key::server, ReadDsnString(dsn, Key::server));
    Set(Key::port, ReadDsnString(dsn, Key::port, defaultPort));
    Set(Key::auth_token, ReadDsnString(dsn, Key::auth_token));
    Set(Key::uid, ReadDsnString(dsn, Key::uid));
    Set(Key::pwd, ReadDsnString(dsn, Key::pwd));
    Set(Key::enable_encryption, ReadDsnString(dsn, Key::enable_encryption, defaultEnableEncryption));
    Set(Key::encryption_cert, ReadDsnString(dsn, Key::encryption_cert));
    Set(Key::use_cert_store, ReadDsnString(dsn, Key::use_cert_store, defaultUseCertStore));
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

}
}

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

#pragma once

#include "windows.h"
#include "winuser.h"
#include <stdint.h>
#include <string>
#include <set>
#include "../odbcabstraction/include/odbcabstraction/connection.h"

namespace driver {
namespace config {

#define TRUE_STR "true"
#define FALSE_STR "false"

/** Connection attribute keywords. */
struct Key
{
    /** Connection attribute keyword for DSN attribute. */
    static const std::string DSN;

    /** Connection attribute keyword for Driver attribute. */
    static const std::string DRIVER;

    /** Connection attribute keyword for host attribute. */
    static const std::string HOST;

    /** Connection attribute keyword for port attribute. */
    static const std::string PORT;

    /** Connection attribute keyword for username attribute. */
    static const std::string UID;

    /** Connection attribute keyword for password attribute. */
    static const std::string PWD;

    /** Connection attribute keyword for authentication token attribute. */
    static const std::string AUTH_TOKEN;

    /** Connection attribute keyword for enabling encryption attribute. */
    static const std::string USE_TLS;

    /** Connection attribute keyword for the encryption certificate attribute. */
    static const std::string ENCRYPTION_CERT;

    /** Connection attribute keyword for using the system certificate store attribute. */
    static const std::string USE_CERT_STORE;

    /** Connection attribute keyword for disabling certificate verification attribute. */
    static const std::string DISABLE_CERT_VERIFICATION;
};

/**
    * ODBC configuration abstraction.
    */
class Configuration
{
public:
    /** Default values for configuration. */
    struct DefaultValue
    {
        /** Default value for DSN attribute. */
        static const std::string dsn;

        /** Default value for Driver attribute. */
        static const std::string driver;

        /** Default value for server attribute. */
        static const std::string server;

        /** Default value for port attribute. */
        static const uint16_t port;

        /** Default value for user attribute. */
        static const std::string user;

        /** Default value for password attribute. */
        static const std::string password;
    };

    /**
        * Default constructor.
        */
    Configuration();

    /**
        * Destructor.
        */
    ~Configuration();

    /**
        * Convert configure to connect string.
        *
        * @return Connect string.
        */
    std::string ToConnectString() const;

    void LoadDefaults();
    void LoadDsn(const std::string& dsn);

    void Clear();
    bool IsSet(const std::string& key) const;
    const std::string& Get(const std::string& key) const;
    void Set(const std::string& key, const std::string& value);

    /**
        * Get properties map.
        */
    const driver::odbcabstraction::Connection::ConnPropertyMap& GetProperties() const;

    std::vector<std::string> GetCustomKeys() const;

private:
    driver::odbcabstraction::Connection::ConnPropertyMap properties;
};

} // namespace config
} // namespace driver

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

#ifndef _IGNITE_ODBC_CONFIG_CONFIGURATION
#define _IGNITE_ODBC_CONFIG_CONFIGURATION

#include <stdint.h>
#include <string>
#include <map>
#include "include/flight_sql/config/settable_value.h"


//#include "ignite/odbc/protocol_version.h"
//#include "ignite/odbc/ssl_mode.h"
//#include "ignite/odbc/end_point.h"
//#include "ignite/odbc/engine_mode.h"
//#include "ignite/odbc/nested_tx_mode.h"

namespace ignite
{
    namespace odbc
    {
        namespace config
        {
            /**
             * ODBC configuration abstraction.
             */
            class Configuration
            {
            public:
                /** Argument map type. */
                typedef std::map<std::string, std::string> ArgumentMap;

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

                /**
                 * Get server port.
                 *
                 * @return Server port.
                 */
                uint16_t GetTcpPort() const;

                /**
                 * Set server port.
                 *
                 * @param port Server port.
                 */
                void SetTcpPort(uint16_t port);

                /**
                 * Check if the value set.
                 *
                 * @return @true if the value set.
                 */
                bool IsTcpPortSet() const;

                /**
                 * Get DSN.
                 *
                 * @return Data Source Name.
                 */
                const std::string& GetDsn(const std::string& dflt = DefaultValue::dsn) const;

                /**
                 * Check if the value set.
                 *
                 * @return @true if the value set.
                 */
                bool IsDsnSet() const;

                /**
                 * Set DSN.
                 *
                 * @param dsn Data Source Name.
                 */
                void SetDsn(const std::string& dsn);

                /**
                 * Get Driver.
                 *
                 * @return Driver name.
                 */
                const std::string& GetDriver() const;

                /**
                 * Set driver.
                 *
                 * @param driver Driver.
                 */
                void SetDriver(const std::string& driver);

                /**
                 * Get server host.
                 *
                 * @return Server host.
                 */
                const std::string& GetHost() const;

                /**
                 * Set server host.
                 *
                 * @param server Server host.
                 */
                void SetHost(const std::string& server);

                /**
                 * Check if the value set.
                 *
                 * @return @true if the value set.
                 */
                bool IsHostSet() const;

                /**
                 * Get user.
                 *
                 * @return User.
                 */
                const std::string& GetUser() const;

                /**
                 * Set user.
                 *
                 * @param user User.
                 */
                void SetUser(const std::string& user);

                /**
                 * Check if the value set.
                 *
                 * @return @true if the value set.
                 */
                bool IsUserSet() const;

                /**
                 * Get password.
                 *
                 * @return Password.
                 */
                const std::string& GetPassword() const;

                /**
                 * Set password.
                 *
                 * @param pass Password.
                 */
                void SetPassword(const std::string& pass);

                /**
                 * Check if the value set.
                 *
                 * @return @true if the value set.
                 */
                bool IsPasswordSet() const;

                /**
                 * Get argument map.
                 *
                 * @param res Resulting argument map.
                 */
                void ToMap(ArgumentMap& res) const;

            private:
                /**
                 * Add key and value to the argument map.
                 *
                 * @param map Map.
                 * @param key Key.
                 * @param value Value.
                 */
                template<typename T>
                static void AddToMap(ArgumentMap& map, const std::string& key, const SettableValue<T>& value);

                /** DSN. */
                SettableValue<std::string> dsn;

                /** Driver name. */
                SettableValue<std::string> driver;

                /** Server. Deprecated. */
                SettableValue<std::string> server;

                /** TCP port. Deprecated. */
                SettableValue<uint16_t> port;

                /** User. */
                SettableValue<std::string> user;

                /** Password. */
                SettableValue<std::string> password;

            };

            template<>
            void Configuration::AddToMap<std::string>(ArgumentMap& map, const std::string& key,
                const SettableValue<std::string>& value);

            template<>
            void Configuration::AddToMap<uint16_t>(ArgumentMap& map, const std::string& key,
                const SettableValue<uint16_t>& value);

            template<>
            void Configuration::AddToMap<int32_t>(ArgumentMap& map, const std::string& key,
                const SettableValue<int32_t>& value);

            template<>
            void Configuration::AddToMap<bool>(ArgumentMap& map, const std::string& key,
                const SettableValue<bool>& value);
        }
    }
}

#endif //_IGNITE_ODBC_CONFIG_CONFIGURATION

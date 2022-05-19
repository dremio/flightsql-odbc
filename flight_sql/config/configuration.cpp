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

#include "windows.h"
#include "winuser.h"
#include <string>
#include <sstream>
#include <iterator>

#include "include/flight_sql/config/configuration.h"
#include "include/flight_sql/config/settable_value.h"
#include "include/flight_sql/common/common.h"
#include "include/flight_sql/config/connection_string_parser.h"
//#include "ignite/odbc/config/config_tools.h"

namespace ignite
{
    namespace odbc
    {
        namespace config
        {
            const std::string Configuration::DefaultValue::dsn = "MyNewDSN";
            const std::string Configuration::DefaultValue::driver = "JoysDriver";
            const std::string Configuration::DefaultValue::server = "";
            const uint16_t Configuration::DefaultValue::port = 10800;
            const std::string Configuration::DefaultValue::user = "";
            const std::string Configuration::DefaultValue::password = "";
           // const ssl::SslMode::Type Configuration::DefaultValue::sslMode = ssl::SslMode::DISABLE;

            Configuration::Configuration() :
                dsn(DefaultValue::dsn),
                driver(DefaultValue::driver),
                server(DefaultValue::server),
                port(DefaultValue::port),
                user(DefaultValue::user),
                password(DefaultValue::password)
            {
                // No-op.
            }


            Configuration::~Configuration()
            {
                // No-op.
            }

            std::string Configuration::ToConnectString() const
            {
                ArgumentMap arguments;

                ToMap(arguments);

                std::stringstream connect_string_buffer;

                for (ArgumentMap::const_iterator it = arguments.begin(); it != arguments.end(); ++it)
                {
                    const std::string& key = it->first;
                    const std::string& value = it->second;

                    if (value.empty())
                        continue;

                    if (value.find(' ') == std::string::npos)
                        connect_string_buffer << key << '=' << value << ';';
                    else
                        connect_string_buffer << key << "={" << value << "};";
                }

                return connect_string_buffer.str();
            }

            uint16_t Configuration::GetTcpPort() const
            {
                return port.GetValue();
            }

            void Configuration::SetTcpPort(uint16_t port)
            {
                this->port.SetValue(port);
            }

            bool Configuration::IsTcpPortSet() const
            {
                return port.IsSet();
            }

            const std::string& Configuration::GetDsn(const std::string& dflt) const
            {
                if (!dsn.IsSet())
                    return dflt;

                return dsn.GetValue();
            }

            bool Configuration::IsDsnSet() const
            {
                return dsn.IsSet();
            }

            void Configuration::SetDsn(const std::string& dsn)
            {
                this->dsn.SetValue(dsn);
            }

            const std::string& Configuration::GetDriver() const
            {
                return driver.GetValue();
            }

            void Configuration::SetDriver(const std::string& driver)
            {
                this->driver.SetValue(driver);
            }

            const std::string& Configuration::GetHost() const
            {
                return server.GetValue();
            }

            void Configuration::SetHost(const std::string& server)
            {
                this->server.SetValue(server);
            }

            bool Configuration::IsHostSet() const
            {
                return server.IsSet();
            }

            const std::string& Configuration::GetUser() const
            {
                return user.GetValue();
            }

            void Configuration::SetUser(const std::string& user)
            {
                this->user.SetValue(user);
            }

            bool Configuration::IsUserSet() const
            {
                return user.IsSet();
            }

            const std::string& Configuration::GetPassword() const
            {
                return password.GetValue();
            }

            void Configuration::SetPassword(const std::string& pass)
            {
                this->password.SetValue(pass);
            }

            bool Configuration::IsPasswordSet() const
            {
                return password.IsSet();
            }
        


            void Configuration::ToMap(ArgumentMap& res) const
            {
                AddToMap(res, ConnectionStringParser::Key::dsn, dsn);
                AddToMap(res, ConnectionStringParser::Key::driver, driver);
                AddToMap(res, ConnectionStringParser::Key::port, port);
                AddToMap(res, ConnectionStringParser::Key::server, server);
                AddToMap(res, ConnectionStringParser::Key::user, user);
                AddToMap(res, ConnectionStringParser::Key::password, password);
            }

            template<>
            void Configuration::AddToMap(ArgumentMap& map, const std::string& key, const SettableValue<uint16_t>& value)
            {
                if (value.IsSet())
                    map[key] = (value.GetValue());
            }

            template<>
            void Configuration::AddToMap(ArgumentMap& map, const std::string& key, const SettableValue<int32_t>& value)
            {
                if (value.IsSet())
                    map[key] = (value.GetValue());
            }

            template<>
            void Configuration::AddToMap(ArgumentMap& map, const std::string& key,
                const SettableValue<std::string>& value)
            {
                if (value.IsSet())
                    map[key] = value.GetValue();
            }

            template<>
            void Configuration::AddToMap(ArgumentMap& map, const std::string& key,
                const SettableValue<bool>& value)
            {
                if (value.IsSet())
                    map[key] = value.GetValue() ? "true" : "false";
            }
        }
    }
}


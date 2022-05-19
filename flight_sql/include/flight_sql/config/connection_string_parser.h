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

#ifndef _IGNITE_ODBC_CONFIG_CONNECTION_STRING_PARSER
#define _IGNITE_ODBC_CONFIG_CONNECTION_STRING_PARSER

#include <string>

#include <flight_sql/config/configuration.h>

#include "flight_sql/config/diagnostic_record_storage.h"

namespace ignite
{
    namespace odbc
    {
        namespace config
        {
            /**
             * ODBC configuration parser abstraction.
             */
            class ConnectionStringParser
            {
            public:
                /** Connection attribute keywords. */
                struct Key
                {
                    /** Connection attribute keyword for DSN attribute. */
                    static const std::string dsn;

                    /** Connection attribute keyword for Driver attribute. */
                    static const std::string driver;


                    /** Connection attribute keyword for server attribute. */
                    static const std::string server;

                    /** Connection attribute keyword for port attribute. */
                    static const std::string port;

                    /** Connection attribute keyword for username attribute. */
                    static const std::string user;

                    /** Connection attribute keyword for password attribute. */
                    static const std::string password;

                    /** Connection attribute keyword for username attribute. */
                    static const std::string uid;

                    /** Connection attribute keyword for password attribute. */
                    static const std::string pwd;
                };

                /**
                 * Constructor.
                 *
                 * @param cfg Configuration.
                 */
                ConnectionStringParser(Configuration& cfg);

                /**
                 * Destructor.
                 */
                ~ConnectionStringParser();

                /**
                 * Parse connect string.
                 *
                 * @param str String to parse.
                 * @param len String length.
                 * @param delimiter delimiter.
                 * @param diag Diagnostics collector.
                 */
                void ParseConnectionString(const char* str, size_t len, char delimiter,
                    diagnostic::DiagnosticRecordStorage* diag);

                /**
                 * Parse connect string.
                 *
                 * @param str String to parse.
                 * @param diag Diagnostics collector.
                 */
                void ParseConnectionString(const std::string& str, diagnostic::DiagnosticRecordStorage* diag);

                /**
                 * Parse config attributes.
                 *
                 * @param str String to parse.
                 * @param diag Diagnostics collector.
                 */
                void ParseConfigAttributes(const char* str, diagnostic::DiagnosticRecordStorage* diag);

            private:
                /**
                 * Result of parsing string value to bool.
                 */
                struct BoolParseResult
                {
                    enum Type
                    {
                        AI_FALSE,

                        AI_TRUE,

                        AI_UNRECOGNIZED
                    };
                };

                /**
                 * Handle new attribute pair callback.
                 *
                 * @param key Key.
                 * @param value Value.
                 * @param diag Diagnostics collector.
                 */
                void HandleAttributePair(const std::string& key, const std::string& value,
                    diagnostic::DiagnosticRecordStorage* diag);

                /**
                 * Convert string to boolean value.
                 *
                 * @param value Value to convert to bool.
                 * @return Result.
                 */
                static BoolParseResult::Type StringToBool(const std::string& value);

                /**
                 * Convert string to boolean value.
                 *
                 * @param msg Error message.
                 * @param key Key.
                 * @param value Value.
                 * @return Resulting error message.
                 */
                static std::string MakeErrorMessage(const std::string& msg, const std::string& key,
                    const std::string& value);

                /** Configuration. */
                Configuration& cfg;
            };
        }

    }
}

#endif //_IGNITE_ODBC_CONFIG_CONNECTION_STRING_PARSER

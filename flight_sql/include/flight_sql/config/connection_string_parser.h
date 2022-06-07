/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#pragma once

#include <string>

#include "config/configuration.h"

namespace driver {
namespace flight_sql {
namespace config {

/**
    * ODBC configuration parser abstraction.
    */
class ConnectionStringParser
{
public:
    /**
        * Constructor.
        *
        * @param cfg Configuration.
        */
    explicit ConnectionStringParser(Configuration& cfg);

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
        */
    void ParseConnectionString(const char* str, size_t len, char delimiter);

    /**
        * Parse connect string.
        *
        * @param str String to parse.
        */
    void ParseConnectionString(const std::string& str);

    /**
        * Parse config attributes.
        *
        * @param str String to parse.
        */
    void ParseConfigAttributes(const char* str);

private:
    ConnectionStringParser(const ConnectionStringParser& parser) = delete;
    ConnectionStringParser& operator=(const ConnectionStringParser&) = delete;

    /** Configuration. */
    Configuration& cfg;
};

} // namespace config
} // namespace flight_sql
} // namespace driver

/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#pragma once

#include <odbcabstraction/platform.h>
#include "winuser.h"
#include <stdint.h>
#include <string>
#include <odbcabstraction/spi/connection.h>

namespace driver {
namespace flight_sql {
namespace config {

#define TRUE_STR "true"
#define FALSE_STR "false"

/**
    * ODBC configuration abstraction.
    */
class Configuration
{
public:
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
} // namespace flight_sql
} // namespace driver

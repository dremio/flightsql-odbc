/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#pragma once

#include <string>
#include <odbcabstraction/spi/connection.h>

namespace driver {
namespace odbcabstraction {

using driver::odbcabstraction::Connection;

/// Parse a string value to a boolean.
/// \param value            the value to be parsed.
/// \param default_value    the default value in case the parse fails.
/// \return                 the parsed valued.
bool AsBool(const std::string& value, bool default_value);

/// Looks up for a value inside the ConnPropertyMap and then try to parse it.
/// In case it does not find or it cannot parse, the default value will be returned.
/// \param default_value      the default value to be parsed.
/// \param connPropertyMap    the map with the connection properties.
/// \param property_name      the name of the property that will be looked up.
/// \return                   the parsed valued.
bool AsBool(bool default_value, const Connection::ConnPropertyMap& connPropertyMap,
            const std::string& property_name);

/// Parse a string value to an int32_t.
/// \param value            the value to be parsed.
/// \param default_value    the default value in case the parse fails.
/// \param min_value        the minimum value to be parsed, else the default value is returned.
/// \return                 the parsed valued.
/// \exception std::invalid_argument    exception from \link std::stoi \endlink
/// \exception std::out_of_range        exception from \link std::stoi \endlink
int32_t AsInt32(const std::string& value, int32_t default_value, int32_t min_value);

/// Looks up for a value inside the ConnPropertyMap and then try to parse it.
/// In case it does not find or it cannot parse, the default value will be returned.
/// \param default_value                the default value to be parsed.
/// \param min_value                    the minimum value to be parsed, else the default value is returned.
/// \param connPropertyMap              the map with the connection properties.
/// \param property_name                the name of the property that will be looked up.
/// \return                             the parsed valued.
/// \exception std::invalid_argument    exception from \link std::stoi \endlink
/// \exception std::out_of_range        exception from \link std::stoi \endlink
int32_t AsInt32(int32_t default_value, int32_t min_value, const Connection::ConnPropertyMap& connPropertyMap,
                const std::string& property_name);
} // namespace odbcabstraction
} // namespace driver

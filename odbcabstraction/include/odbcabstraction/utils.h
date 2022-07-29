/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#pragma once

#include <boost/algorithm/string.hpp>
#include <string>
#include <odbcabstraction/logger.h>
#include <odbcabstraction/spi/connection.h>

namespace driver {
namespace odbcabstraction {

using driver::odbcabstraction::Connection;

/// Parse a string value to a boolean.
/// \param value            the value to be parsed.
/// \return                 the parsed valued.
boost::optional<bool> AsBool(const std::string& value);

/// Looks up for a value inside the ConnPropertyMap and then try to parse it.
/// In case it does not find or it cannot parse, the default value will be returned.
/// \param connPropertyMap    the map with the connection properties.
/// \param property_name      the name of the property that will be looked up.
/// \return                   the parsed valued.
boost::optional<bool> AsBool(const Connection::ConnPropertyMap& connPropertyMap, const std::string& property_name);

/// Looks up for a value inside the ConnPropertyMap and then try to parse it.
/// In case it does not find or it cannot parse, the default value will be returned.
/// \param min_value                    the minimum value to be parsed, else the default value is returned.
/// \param connPropertyMap              the map with the connection properties.
/// \param property_name                the name of the property that will be looked up.
/// \return                             the parsed valued.
/// \exception std::invalid_argument    exception from \link std::stoi \endlink
/// \exception std::out_of_range        exception from \link std::stoi \endlink
boost::optional<int32_t> AsInt32(int32_t min_value, const Connection::ConnPropertyMap& connPropertyMap,
                const std::string& property_name);


void ReadConfigFile(PropertyMap &properties, const std::string &configFileName);

} // namespace odbcabstraction
} // namespace driver

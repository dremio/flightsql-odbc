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
} // namespace odbcabstraction
} // namespace driver

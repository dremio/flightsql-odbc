/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include <odbcabstraction/utils.h>

#include <boost/algorithm/string/predicate.hpp>

namespace driver {
namespace odbcabstraction {

boost::optional<bool> AsBool(const Connection::ConnPropertyMap& connPropertyMap,
            const std::string& property_name) {
  auto extracted_property = connPropertyMap.find(property_name);

  if (extracted_property != connPropertyMap.end()) {
    if (boost::iequals(extracted_property->second, "true") || boost::iequals(extracted_property->second, "1")) {
      return true;
    } else if (boost::iequals(extracted_property->second, "false") || boost::iequals(extracted_property->second, "0")) {
      return false;
    }
  }

  return boost::none;
}

boost::optional<int32_t> AsInt32(int32_t min_value, const Connection::ConnPropertyMap& connPropertyMap, const std::string& property_name) {
  auto extracted_property = connPropertyMap.find(property_name);

  if (extracted_property != connPropertyMap.end()) {
    const int32_t stringColumnLength = std::stoi(extracted_property->second);

    if (stringColumnLength >= min_value && stringColumnLength <= INT32_MAX) {
      return stringColumnLength;
    }
  }
  return boost::none;
}

} // namespace odbcabstraction
} // namespace driver

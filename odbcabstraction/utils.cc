/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include <odbcabstraction/utils.h>

#include <boost/algorithm/string/predicate.hpp>

namespace driver {
namespace odbcabstraction {

bool AsBool(const std::string& value, bool default_value) {
  if (boost::iequals(value, "true") || boost::iequals(value, "1")) {
    return true;
  } else if (boost::iequals(value, "false") || boost::iequals(value, "0")) {
    return false;
  } else {
    return default_value;
  }
}

bool AsBool(bool default_value, const Connection::ConnPropertyMap& connPropertyMap,
            const std::string& property_name) {

  auto extracted_property = connPropertyMap.find(
    property_name);

  if (extracted_property != connPropertyMap.end()) {
    return AsBool(extracted_property->second, default_value);
  }
  else {
    return default_value;
  }
}

int32_t AsInt32(const std::string& value, int32_t default_value, int32_t min_value) {
  const int32_t string_column_length = std::stoi(value);

  if (string_column_length >= min_value && string_column_length <= INT32_MAX) {
    return string_column_length;
  } else {
    return default_value;
  }
}

int32_t AsInt32(int32_t default_value, int32_t min_value, const Connection::ConnPropertyMap& connPropertyMap,
            const std::string& property_name) {

  auto extracted_property = connPropertyMap.find(
          property_name);

  if (extracted_property != connPropertyMap.end()) {
    return AsInt32(extracted_property->second, default_value, min_value);
  }
  else {
    return default_value;
  }
}
} // namespace odbcabstraction
} // namespace driver

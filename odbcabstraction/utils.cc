/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include <odbcabstraction/utils.h>
#include "whereami.h"

#include <fstream>
#include <sstream>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/tokenizer.hpp>
#include <boost/token_functions.hpp>

namespace driver {
namespace odbcabstraction {

boost::optional<bool> AsBool(const std::string& value) {
  if (boost::iequals(value, "true") || boost::iequals(value, "1")) {
    return true;
  } else if (boost::iequals(value, "false") || boost::iequals(value, "0")) {
    return false;
  } else {
    return boost::none;
  }
}

boost::optional<bool> AsBool(const Connection::ConnPropertyMap& connPropertyMap,
            const std::string& property_name) {
  auto extracted_property = connPropertyMap.find(property_name);

  if (extracted_property != connPropertyMap.end()) {
    return AsBool(extracted_property->second);
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

void ReadConfigLogFile(ConfigPropertyMap properties) {
  std::vector<char> path;
  int length, dirname_length;
  length = wai_getExecutablePath(NULL, 0, &dirname_length);

  if (length != 0) {
    path.reserve(length);
    wai_getExecutablePath(path.data(), length, &dirname_length);
  } else {
    throw DriverException("Could not read the driver config file.");
  }

  std::ifstream myfile;

  std::string config_path(path.data()) ;
  myfile.open(config_path + "/arrow-odbc.ini");

  if (myfile.fail()) {
    throw DriverException("Could not read the driver config file.");
  }

  std::string temp_config;
  std::stringstream configString;

  boost::char_separator<char> separator("=");
  while(myfile.good()) {
    myfile >> temp_config;
    boost::tokenizer< boost::char_separator<char>> tokenizer(temp_config, separator);

    auto iterator = tokenizer.begin();

    std::string key = *iterator;
    std::string value = *++iterator;

    properties[key] = std::move(value);
  }
}

} // namespace odbcabstraction
} // namespace driver

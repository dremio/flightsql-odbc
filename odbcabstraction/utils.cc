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

#define CONFIG_LOG_NAME "/arrow-odbc.ini"

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

std::string GetModulePath() {
  std::vector<char> path;
  int length, dirname_length;
  length = wai_getModulePath(NULL, 0, &dirname_length);

  if (length != 0) {
    path.resize(length);
    wai_getModulePath(path.data(), length, &dirname_length);
  } else {
    throw DriverException("Could not find module path.");
  }

  return std::string(path.begin(), path.begin() + dirname_length);
}

void ReadConfigFile(PropertyMap &properties, const std::string &configFileName) {
  auto config_path = GetModulePath();

  std::ifstream myfile;
  myfile.open(config_path + "/" + configFileName);

  if (myfile.fail()) {
    throw DriverException("Could not read the driver config file.");
  }

  std::string temp_config;

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

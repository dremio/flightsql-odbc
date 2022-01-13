// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <boost/optional.hpp>
#include <boost/variant.hpp>
#include <map>
#include <vector>

#include "types.h"

#pragma once

namespace driver {
namespace odbcabstraction {

class Statement;

/// \brief High-level representation of an ODBC connection.
class Connection {
public:
  virtual ~Connection() = default;

  /// \brief Connection attributes
  enum AttributeId {
    ACCESS_MODE,        // Tells if it should support write operations
    AUTO_IPD,           // Relevant to parameter binding on statements
    AUTOCOMMIT,         // Do not support transactions yet
    CONNECTION_DEAD,    // Tells if connection is still alive
    CONNECTION_TIMEOUT, // Matters to Connect()
    DBC_INFO_TOKEN,     // Lookup
    LOGIN_TIMEOUT,      // Matters to Connect()
    METADATA_ID,        // Pass to statement
    PACKET_SIZE,        // Lookup if there is a packet size on Flight
    QUIET_MODE,         // Lookup
  };

  typedef boost::variant<std::string, int, double, bool> Attribute;
  typedef boost::variant<std::string, int, bool> Property;
  typedef boost::variant<std::string, int, bool> Info;

  static const std::string HOST;
  static const std::string PORT;
  static const std::string USER;
  static const std::string PASSWORD;
  static const std::string USE_TLS;
  // TODO: Add properties for getting the certificates
  // TODO: Check if gRPC can use the system truststore, if not copy from Drill

  /// \brief Establish the connection.
  /// \param properties[in] properties used to establish the connection.
  /// \param missing_properties[out] vector of missing properties (if any).
  virtual void Connect(const std::map<std::string, Property> &properties,
                       std::vector<std::string> &missing_properties) = 0;

  /// \brief Close the connection.
  virtual void Close() = 0;

  /// \brief Create a statement.
  virtual std::shared_ptr<Statement> CreateStatement() = 0;

  /// \brief Set a connection attribute (may be called at any time).
  /// \param attribute[in] Which attribute to set.
  /// \param value The value to be set.
  virtual void SetAttribute(AttributeId attribute, const Attribute &value) = 0;

  /// \brief Retrieve a connection attribute
  /// \param attribute[in] Attribute to be retrieved.
  virtual boost::optional<Connection::Attribute>
  GetAttribute(Connection::AttributeId attribute) = 0;

  /// \brief Retrieves info from the database (see ODBC's SQLGetInfo).
  virtual Info GetInfo(uint16_t info_type) = 0;

protected:
  Connection() = default;
};

} // namespace odbcabstraction
} // namespace driver

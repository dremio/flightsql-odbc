/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#pragma once

#include <boost/algorithm/string.hpp>
#include <boost/optional.hpp>
#include <boost/variant.hpp>
#include <map>
#include <vector>

#include <odbcabstraction/types.h>
#include <odbcabstraction/diagnostics.h>

namespace driver {
namespace odbcabstraction {

/// \brief Case insensitive comparator
struct CaseInsensitiveComparator
        : std::binary_function<std::string, std::string, bool> {
  bool operator()(const std::string &s1, const std::string &s2) const {
    return boost::lexicographical_compare(s1, s2, boost::is_iless());
  }
};

// PropertyMap is case-insensitive for keys.
typedef std::map<std::string, std::string, CaseInsensitiveComparator> PropertyMap;

class Statement;

/// \brief High-level representation of an ODBC connection.
class Connection {
protected:
  Connection() = default;

public:
  virtual ~Connection() = default;

  /// \brief Connection attributes
  enum AttributeId {
    ACCESS_MODE,        // uint32_t - Tells if it should support write operations
    CONNECTION_DEAD,    // uint32_t - Tells if connection is still alive
    CONNECTION_TIMEOUT, // uint32_t - The timeout for connection functions after connecting.
    CURRENT_CATALOG,    // std::string - The current catalog
    LOGIN_TIMEOUT,      // uint32_t - The timeout for the initial connection
    PACKET_SIZE,        // uint32_t - The Packet Size
  };

  typedef boost::variant<std::string, void*, uint64_t, uint32_t>  Attribute;
  typedef boost::variant<std::string, uint32_t, uint16_t> Info;
  typedef PropertyMap ConnPropertyMap;

  /// \brief Establish the connection.
  /// \param properties[in] properties used to establish the connection.
  /// \param missing_properties[out] vector of missing properties (if any).
  virtual void Connect(const ConnPropertyMap &properties,
                       std::vector<std::string> &missing_properties) = 0;

  /// \brief Close the connection.
  virtual void Close() = 0;

  /// \brief Create a statement.
  virtual std::shared_ptr<Statement> CreateStatement() = 0;

  /// \brief Set a connection attribute (may be called at any time).
  /// \param attribute[in] Which attribute to set.
  /// \param value The value to be set.
  /// \return true if the value was set successfully or false if it was substituted with
  /// a similar value.
  virtual bool SetAttribute(AttributeId attribute, const Attribute &value) = 0;

  /// \brief Retrieve a connection attribute
  /// \param attribute[in] Attribute to be retrieved.
  virtual boost::optional<Connection::Attribute>
  GetAttribute(Connection::AttributeId attribute) = 0;

  /// \brief Retrieves info from the database (see ODBC's SQLGetInfo).
  virtual Info GetInfo(uint16_t info_type) = 0;

  /// \brief Gets the diagnostics for this connection.
  /// \return the diagnostics
  virtual Diagnostics& GetDiagnostics() = 0;
};

} // namespace odbcabstraction
} // namespace driver

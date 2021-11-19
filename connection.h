#include "statement.h"
#include <boost/optional.hpp>
#include <boost/variant.hpp>
#include <map>
#include <vector>

#include "types.h"

#pragma once

class Connection {
public:
  enum AttributeId {
    ACCESS_MODE,     // Tells if it should support write operations
    AUTO_IPD,                   // Relevant to parameter binding on statements
    AUTOCOMMIT,                 // Do not support transactions yet
    CONNECTION_DEAD,            // Tells if connection is still alive
    CONNECTION_TIMEOUT,         // Matters to Connect()
    DBC_INFO_TOKEN,  // Lookup
    LOGIN_TIMEOUT,   // Matters to Connect()
    METADATA_ID,     // Pass to statement
    PACKET_SIZE,     // Lookup if there is a packet size on Flight
    QUIET_MODE,      // Lookup
  };

  typedef boost::variant<std::string, int, double, bool> Attribute;
  typedef boost::variant<std::string, int, bool> Property;
  typedef boost::variant<std::string, int, bool> Info;

  static const std::string HOST;
  static const std::string PORT;
  static const std::string USER;
  static const std::string PASSWORD;
  static const std::string USE_SSL;
  // Add properties for getting the certificates
  // Check if gRPC can use the system truststore, if not copy from Drill

  explicit Connection(OdbcVersion odbc_version);

  /**
   * Unified connect method
   *
   * @param properties[in]
   * @param missing_attr[out]
   */
  virtual void Connect(const std::map<std::string, Property> &properties,
                       std::vector<std::string> &missing_attr) = 0;

  /**
   * Close this connection
   */
  virtual void Close() = 0;

  /**
   * Allocates a statement
   */
  virtual std::shared_ptr<Statement> CreateStatement() = 0;

  /**
   * Set a connection attribute (may be called at any time)
   * @param attribute
   * @param value
   */
  virtual void SetAttribute(AttributeId attribute, const Attribute &value) = 0;

  /**
   * Retrieve a connection attribute
   *
   * @param attribute
   * @return
   */
  virtual boost::optional<Connection::Attribute>
  GetAttribute(Connection::AttributeId attribute) = 0;

  virtual Info GetInfo(uint16_t info_type) = 0;

private:
  OdbcVersion odbc_version_;
};

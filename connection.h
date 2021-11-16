#include <map>
#include <vector>
#include <boost/variant.hpp>
#include "statement.h"

#pragma once

class Connection {
public:
  enum AttributeId {
    ACCESS_MODE,
    ASYNC_DBC_EVENT,
    ASYNC_DBC_FUNCTIONS_ENABLE,
    ASYNC_DBC_PCALLBACK,
    ASYNC_DBC_PCONTEXT,
    ASYNC_ENABLE,
    AUTO_IPD,
    AUTOCOMMIT,
    CONNECTION_DEAD,
    CONNECTION_TIMEOUT,
    CURRENT_CATALOG,
    DBC_INFO_TOKEN,
    ENLIST_IN_DTC,
    LOGIN_TIMEOUT,
    METADATA_ID,
    ODBC_CURSORS,
    PACKET_SIZE,
    QUIET_MODE,
    TRACE,
    TRACEFILE,
    TRANSLATE_LIB,
    TRANSLATE_OPTION,
    TXN_ISOLATION,
  };
  typedef boost::variant<std::string, int, bool> Attribute;
  typedef boost::variant<std::string, int, bool> Property;
  typedef boost::variant<std::string, int, bool> Info;

  static const std::string CONNECTION_STRING;
  static const std::string HOST;
  static const std::string PORT;
  static const std::string USERNAME;
  static const std::string PASSWORD;

  /**
   * Unified connect method
   *
   * @param properties[in]
   * @param missing_attr[out]
   */
  virtual void Connect(
    const std::map<std::string, Property>& properties,
    std::vector<std::string>& missing_attr) = 0;

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
  virtual void SetAttribute(AttributeId attribute, const Attribute& value) = 0;

  /**
   * Retrieve a connection attribute
   *
   * @param attribute
   * @return
   */
  virtual Attribute GetAttribute(AttributeId attribute) = 0;

  virtual Info GetInfo(uint16_t info_type) = 0;
};

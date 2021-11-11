#include <map>
#include <vector>
#include <boost/variant.hpp>
#include "statement.h"

#pragma once

class Connection {
public:
  enum AttributeId {
    // TODO: Add attributes ids
  };
  typedef boost::variant<std::string> Attribute;
  typedef boost::variant<std::string> Property;

  // TODO: Constants for known properties (username, auth)

  /**
   * Unified connect method
   *
   * @param properties[in]
   * @param missing_attr[out]
   */
  virtual void Connect(
    const std::map<std::string, PropertiesType>& properties,
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
  virtual void SetAttribute(Attribute attribute, const AttributesType& value) = 0;

  /**
   * Retrieve a connection attribute
   *
   * @param attribute
   * @return
   */
  virtual AttributesType GetAttribute(Attribute attribute) = 0;

  // I feel this method should just return a variant (that can be a string, int32, etc)
  // and the wiring layer should be responsible for filling output ODBC buffers.
  // Let's not use short, int, etc and use int16_t, int32_t instead.
  virtual void GetInfo(uint16_t info_type, void* out, short out_capacity, short* out_length) = 0;
};

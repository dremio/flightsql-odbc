/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#pragma once

#include <odbcabstraction/odbc_impl/ODBCHandle.h>

#include <sql.h>
#include <memory>
#include <vector>
#include <map>
#include <odbcabstraction/spi/connection.h>

namespace ODBC
{
  class ODBCEnvironment;
  class ODBCDescriptor;
  class ODBCStatement;
}

/**
 * @brief An abstraction over an ODBC connection handle. This also wraps an SPI Connection.
 */
namespace ODBC
{
class ODBCConnection : public ODBCHandle<ODBCConnection> {
  public:
    ODBCConnection(const ODBCConnection&) = delete;
    ODBCConnection& operator=(const ODBCConnection&) = delete;

    ODBCConnection(ODBCEnvironment& environment, 
      std::shared_ptr<driver::odbcabstraction::Connection> spiConnection);
    
    driver::odbcabstraction::Diagnostics& GetDiagnostics_Impl();

    const std::string& GetDSN() const;
    bool isConnected() const;
    void connect(std::string dsn, const driver::odbcabstraction::Connection::ConnPropertyMap &properties,
                       std::vector<std::string> &missing_properties);

    void GetInfo(SQLUSMALLINT infoType, SQLPOINTER value, SQLSMALLINT bufferLength, SQLSMALLINT* outputLength, bool isUnicode);
    void SetConnectAttr(SQLINTEGER attribute, SQLPOINTER value, SQLINTEGER stringLength, bool isUnicode);
    void GetConnectAttr(SQLINTEGER attribute, SQLPOINTER value, SQLINTEGER bufferLength, SQLINTEGER* outputLength, bool isUnicode);

    ~ODBCConnection() = default;

    inline ODBCStatement& GetTrackingStatement() {
      return *m_attributeTrackingStatement;
    }

    void disconnect();

    void releaseConnection();

    std::shared_ptr<ODBCStatement> createStatement();
    void dropStatement(ODBCStatement* statement);

    std::shared_ptr<ODBCDescriptor> createDescriptor();
    void dropDescriptor(ODBCDescriptor* descriptor);

    inline bool IsOdbc2Connection() const {
      return m_is2xConnection;
    }

    /// @return the DSN or empty string if Driver was used.
    static std::string getPropertiesFromConnString(const std::string& connStr, 
      driver::odbcabstraction::Connection::ConnPropertyMap &properties);

  private:
    ODBCEnvironment& m_environment;
    std::shared_ptr<driver::odbcabstraction::Connection> m_spiConnection;
    // Extra ODBC statement that's used to track and validate when statement attributes are
    // set through the connection handle. These attributes get copied to new ODBC statements
    // when they are allocated.
    std::shared_ptr<ODBCStatement> m_attributeTrackingStatement;
    std::vector<std::shared_ptr<ODBCStatement> > m_statements;
    std::vector<std::shared_ptr<ODBCDescriptor> > m_descriptors;
    std::string m_dsn;
    const bool m_is2xConnection;
    bool m_isConnected;
};

}

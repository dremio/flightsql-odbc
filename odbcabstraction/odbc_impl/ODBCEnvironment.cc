/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include <odbcabstraction/odbc_impl/ODBCEnvironment.h>

#include <algorithm>
#include <utility>
#include <sqlext.h>
#include <odbcabstraction/spi/driver.h>
#include <odbcabstraction/spi/connection.h>
#include <odbcabstraction/types.h>
#include <odbcabstraction/odbc_impl/ODBCConnection.h>

using namespace ODBC;
using namespace driver::odbcabstraction;

// Public =========================================================================================
ODBCEnvironment::ODBCEnvironment(std::shared_ptr<Driver> driver) :
  m_driver(std::move(driver)),
  m_diagnostics(new Diagnostics(m_driver->GetDiagnostics().GetVendor(),
                                   m_driver->GetDiagnostics().GetDataSourceComponent(),
                                   V_2)),
  m_version(SQL_OV_ODBC2),
  m_connectionPooling(SQL_CP_OFF) {
}

Diagnostics &ODBCEnvironment::GetDiagnostics_Impl() {
  return *m_diagnostics;
}

SQLINTEGER ODBCEnvironment::getODBCVersion() const {
  return m_version;
}

void ODBCEnvironment::setODBCVersion(SQLINTEGER version) {
  if (version != m_version) {
    m_version = version;
    m_diagnostics.reset(
        new Diagnostics(m_diagnostics->GetVendor(),
                        m_diagnostics->GetDataSourceComponent(),
                        version == SQL_OV_ODBC2 ? V_2 : V_3));
  }
}

SQLINTEGER ODBCEnvironment::getConnectionPooling() const {
  return m_connectionPooling;
}

void ODBCEnvironment::setConnectionPooling(SQLINTEGER connectionPooling) {
  m_connectionPooling = connectionPooling;
}

std::shared_ptr<ODBCConnection> ODBCEnvironment::CreateConnection() {
  std::shared_ptr<Connection> spiConnection = m_driver->CreateConnection(m_version == SQL_OV_ODBC2 ? V_2 : V_3);
  std::shared_ptr<ODBCConnection> newConn = std::make_shared<ODBCConnection>(*this, spiConnection);
  m_connections.push_back(newConn);
  return newConn;
}

void ODBCEnvironment::DropConnection(ODBCConnection* conn) {
    auto it = std::find_if(m_connections.begin(), m_connections.end(), 
      [&conn] (const std::shared_ptr<ODBCConnection>& connection) { return connection.get() == conn; });
    if (m_connections.end() != it) {
        m_connections.erase(it);
    }
}

/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#pragma once

#include <odbcabstraction/odbc_impl/ODBCHandle.h>

#include <odbcabstraction/platform.h>
#include <sql.h>
#include <memory>
#include <string>

namespace driver {
namespace odbcabstraction {
  class Statement;
  class ResultSet;
}
}

namespace ODBC {
  class ODBCConnection;
  class ODBCDescriptor;
}

/**
 * @brief An abstraction over an ODBC connection handle. This also wraps an SPI Connection.
 */
namespace ODBC
{
class ODBCStatement : public ODBCHandle<ODBCStatement> {
  public:
    ODBCStatement(const ODBCStatement&) = delete;
    ODBCStatement& operator=(const ODBCStatement&) = delete;

    ODBCStatement(ODBCConnection& connection, 
      std::shared_ptr<driver::odbcabstraction::Statement> spiStatement);
    
    ~ODBCStatement() = default;

    driver::odbcabstraction::Diagnostics& GetDiagnostics_Impl();

    ODBCConnection &GetConnection();

    void CopyAttributesFromConnection(ODBCConnection& connection);
    void Prepare(const std::string& query);
    void ExecutePrepared();
    void ExecuteDirect(const std::string& query);

    /**
     * @brief Returns true if the number of rows fetch was greater than zero.
     */
    bool Fetch(size_t rows);
    bool isPrepared() const;

    void GetStmtAttr(SQLINTEGER statementAttribute, SQLPOINTER output,
                     SQLINTEGER bufferSize, SQLINTEGER *strLenPtr, bool isUnicode);
    void SetStmtAttr(SQLINTEGER statementAttribute, SQLPOINTER value,
                     SQLINTEGER bufferSize, bool isUnicode);

    void RevertAppDescriptor(bool isApd);

    inline ODBCDescriptor* GetIRD() {
      return m_ird.get();
    }

    inline ODBCDescriptor* GetARD() {
      return m_currentArd;
    }
    
    inline SQLULEN GetRowsetSize() {
      return m_rowsetSize;
    }

    bool GetData(SQLSMALLINT recordNumber, SQLSMALLINT cType, SQLPOINTER dataPtr, SQLLEN bufferLength, SQLLEN* indicatorPtr);

    /**
     * @brief Closes the cursor. This does _not_ un-prepare the statement or change
     * bindings.
     */
    void closeCursor(bool suppressErrors);

    /**
     * @brief Releases this statement from memory.
     */
    void releaseStatement();

    void GetTables(const std::string* catalog, const std::string* schema, const std::string* table, const std::string* tableType);
    void GetColumns(const std::string* catalog, const std::string* schema, const std::string* table, const std::string* column);
    void GetTypeInfo(SQLSMALLINT dataType);
    void Cancel();

  private:
    ODBCConnection& m_connection;
    std::shared_ptr<driver::odbcabstraction::Statement> m_spiStatement;
    std::shared_ptr<driver::odbcabstraction::ResultSet> m_currenResult;

    std::shared_ptr<ODBCDescriptor> m_builtInArd;
    std::shared_ptr<ODBCDescriptor> m_builtInApd;
    std::shared_ptr<ODBCDescriptor> m_ipd;
    std::shared_ptr<ODBCDescriptor> m_ird;
    ODBCDescriptor* m_currentArd;
    ODBCDescriptor* m_currentApd;
    SQLULEN m_rowNumber;
    SQLULEN m_maxRows;
    SQLULEN m_rowsetSize; // Used by SQLExtendedFetch instead of the ARD array size.
    bool m_isPrepared;
    bool m_hasReachedEndOfResult;
};
}

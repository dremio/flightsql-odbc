/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#pragma once

#include <boost/optional.hpp>
#include <boost/variant.hpp>
#include <map>
#include <vector>

namespace driver {
namespace odbcabstraction {

using boost::optional;

class ResultSet;

class ResultSetMetadata;

/// \brief High-level representation of an ODBC statement.
class Statement {
protected:
  Statement() = default;

public:
  virtual ~Statement() = default;

  /// \brief Statement attributes that can be called at anytime.
  ////TODO: Document attributes
  enum StatementAttributeId {
    MAX_LENGTH,     // size_t - The maximum length when retrieving variable length data. 0 means no limit.
    METADATA_ID,    // size_t - Modifies catalog function arguments to be identifiers. SQL_TRUE or SQL_FALSE.
    NOSCAN,         // size_t - Indicates that the driver does not scan for escape sequences. Default to SQL_NOSCAN_OFF
    QUERY_TIMEOUT,  // size_t - The time to wait in seconds for queries to execute. 0 to have no timeout.
  };

  typedef boost::variant<size_t> Attribute;

  /// \brief Set a statement attribute (may be called at any time)
  ///
  /// NOTE: Meant to be bound with SQLSetStmtAttr.
  ///
  /// \param attribute Attribute identifier to set.
  /// \param value Value to be associated with the attribute.
  /// \return true if the value was set successfully or false if it was substituted with
  /// a similar value.
  virtual bool SetAttribute(StatementAttributeId attribute,
                            const Attribute &value) = 0;

  /// \brief Retrieve a statement attribute.
  ///
  /// NOTE: Meant to be bound with SQLGetStmtAttr.
  ///
  /// \param attribute Attribute identifier to be retrieved.
  /// \return Value associated with the attribute.
  virtual optional<Statement::Attribute>
  GetAttribute(Statement::StatementAttributeId attribute) = 0;

  /// \brief Prepares the statement.
  /// Returns ResultSetMetadata if query returns a result set,
  /// otherwise it returns `boost::none`.
  /// \param query The SQL query to prepare.
  virtual boost::optional<std::shared_ptr<ResultSetMetadata>>
  Prepare(const std::string &query) = 0;

  /// \brief Execute the prepared statement.
  ///
  /// NOTE: Must call `Prepare(const std::string &query)` before, otherwise it
  /// will throw an exception.
  ///
  /// \returns true if the first result is a ResultSet object;
  ///         false if it is an update count or there are no results.
  virtual bool ExecutePrepared() = 0;

  /// \brief Execute the statement if it is prepared or not.
  /// \param query The SQL query to execute.
  /// \returns true if the first result is a ResultSet object;
  ///         false if it is an update count or there are no results.
  virtual bool Execute(const std::string &query) = 0;

  /// \brief Returns the current result as a ResultSet object.
  virtual std::shared_ptr<ResultSet> GetResultSet() = 0;

  /// \brief Retrieves the current result as an update count;
  /// if the result is a ResultSet object or there are no more results, -1 is
  /// returned.
  virtual long GetUpdateCount() = 0;

  /// \brief Returns the list of table, catalog, or schema names, and table
  /// types, stored in a specific data source. The driver returns the
  /// information as a result set.
  ///
  /// NOTE: This is meant to be used by ODBC 2.x binding.
  ///
  /// \param catalog_name The catalog name.
  /// \param schema_name The schema name.
  /// \param table_name The table name.
  /// \param table_type The table type.
  virtual std::shared_ptr<ResultSet>
  GetTables_V2(const std::string *catalog_name, const std::string *schema_name,
               const std::string *table_name,
               const std::string *table_type) = 0;

  /// \brief Returns the list of table, catalog, or schema names, and table
  /// types, stored in a specific data source. The driver returns the
  /// information as a result set.
  ///
  /// NOTE: This is meant to be used by ODBC 3.x binding.
  ///
  /// \param catalog_name The catalog name.
  /// \param schema_name The schema name.
  /// \param table_name The table name.
  /// \param table_type The table type.
  virtual std::shared_ptr<ResultSet>
  GetTables_V3(const std::string *catalog_name, const std::string *schema_name,
               const std::string *table_name,
               const std::string *table_type) = 0;

  /// \brief Returns the list of column names in specified tables. The driver
  /// returns this information as a result set..
  ///
  /// NOTE: This is meant to be used by ODBC 2.x binding.
  ///
  /// \param catalog_name The catalog name.
  /// \param schema_name The schema name.
  /// \param table_name The table name.
  /// \param column_name The column name.
  virtual std::shared_ptr<ResultSet>
  GetColumns_V2(const std::string *catalog_name, const std::string *schema_name,
                const std::string *table_name,
                const std::string *column_name) = 0;

  /// \brief Returns the list of column names in specified tables. The driver
  /// returns this information as a result set..
  ///
  /// NOTE: This is meant to be used by ODBC 3.x binding.
  ///
  /// \param catalog_name The catalog name.
  /// \param schema_name The schema name.
  /// \param table_name The table name.
  /// \param column_name The column name.
  virtual std::shared_ptr<ResultSet>
  GetColumns_V3(const std::string *catalog_name, const std::string *schema_name,
                const std::string *table_name,
                const std::string *column_name) = 0;

  /// \brief Returns information about data types supported by the data source.
  /// The driver returns the information in the form of an SQL result set. The
  /// data types are intended for use in Data Definition Language (DDL)
  /// statements.
  ///
  /// NOTE: This is meant to be used by ODBC 2.x binding.
  ///
  /// \param data_type The SQL data type.
  virtual std::shared_ptr<ResultSet> GetTypeInfo_V2(int16_t data_type) = 0;

  /// \brief Returns information about data types supported by the data source.
  /// The driver returns the information in the form of an SQL result set. The
  /// data types are intended for use in Data Definition Language (DDL)
  /// statements.
  ///
  /// NOTE: This is meant to be used by ODBC 3.x binding.
  ///
  /// \param data_type The SQL data type.
  virtual std::shared_ptr<ResultSet> GetTypeInfo_V3(int16_t data_type) = 0;

  /// \brief Returns the primary keys for a table.
  /// The driver returns this information as a result set.
  ///
  /// \param catalog_name The catalog name.
  /// \param schema_name The schema name.
  /// \param table_name The table name.
  virtual std::shared_ptr<ResultSet>
  GetPrimaryKeys(const std::string *catalog_name, const std::string *schema_name,
                 const std::string *table_name) = 0;

  /// \brief Returns the foreign keys for a table.
  /// The driver returns this information as a result set.
  ///
  /// \param pk_catalog_name The primary key catalog name.
  /// \param pk_schema_name The primary key schema name.
  /// \param pk_table_name The primary key table name.
  /// \param fk_catalog_name The foreign key catalog name.
  /// \param fk_schema_name The foreign key schema name.
  /// \param fk_table_name The foreign key table name.
  virtual std::shared_ptr<ResultSet>
  GetForeignKeys(const std::string *pk_catalog_name, const std::string *pk_schema_name,
                 const std::string *pk_table_name, const std::string *fk_catalog_name,
                 const std::string *fk_schema_name, const std::string *fk_table_name) = 0;

  /// \brief Gets the diagnostics for this statement.
  /// \return the diagnostics
  virtual Diagnostics& GetDiagnostics() = 0;

  /// \brief Cancels the processing of this statement.
  virtual void Cancel() = 0;
};

} // namespace odbcabstraction
} // namespace driver

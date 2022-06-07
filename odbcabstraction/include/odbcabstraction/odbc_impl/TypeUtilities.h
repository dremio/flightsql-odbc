/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#pragma once

#include <sql.h>
#include <sqlext.h>

namespace ODBC {
  inline SQLSMALLINT GetSqlTypeForODBCVersion(SQLSMALLINT type, bool isOdbc2x) {
    switch (type) {
      case SQL_DATE:
      case SQL_TYPE_DATE:
        return isOdbc2x ? SQL_DATE : SQL_TYPE_DATE;
      
      case SQL_TIME:
      case SQL_TYPE_TIME:
        return isOdbc2x ? SQL_TIME : SQL_TYPE_TIME;
    
      case SQL_TIMESTAMP:
      case SQL_TYPE_TIMESTAMP:
        return isOdbc2x ? SQL_TIMESTAMP : SQL_TYPE_TIMESTAMP;
    
      default:
        return type;
    }
  }
}

/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#pragma once

#include <cstdint>

namespace driver {
namespace odbcabstraction {

  enum ODBCErrorCodes : int32_t {
  ODBCErrorCodes_GENERAL_ERROR = 100,
    ODBCErrorCodes_AUTH = 200,
    ODBCErrorCodes_TLS = 300,
    ODBCErrorCodes_FRACTIONAL_TRUNCATION_ERROR = 400,
    ODBCErrorCodes_COMMUNICATION = 500,
    ODBCErrorCodes_GENERAL_WARNING = 1000000,
    ODBCErrorCodes_TRUNCATION_WARNING = 1000100,
    ODBCErrorCodes_FRACTIONAL_TRUNCATION_WARNING = 1000100,
    ODBCErrorCodes_INDICATOR_NEEDED = 1000200
  };
}
}

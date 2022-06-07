/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#pragma once

#include <exception>
#include <string>
#include <cstdint>
#include <odbcabstraction/error_codes.h>

namespace driver {
namespace odbcabstraction {

/// \brief Base for all driver specific exceptions
class DriverException : public std::exception {
public:
  explicit DriverException(std::string message, std::string sql_state = "HY000",
                  int32_t native_error = ODBCErrorCodes_GENERAL_ERROR);

  const char *what() const throw() override;

  const std::string &GetMessageText() const;
  const std::string &GetSqlState() const;
  int32_t GetNativeError() const;

private:
  const std::string msg_text_;
  const std::string sql_state_;
  const int32_t native_error_;
};

/// \brief Authentication specific exception
class AuthenticationException : public DriverException {
public:
  explicit AuthenticationException(std::string message, std::string sql_state = "28000",
                                   int32_t native_error = ODBCErrorCodes_AUTH);
};

/// \brief Error when null is retrieved from the database but no indicator was supplied.
/// (This means the driver has no way to report ot the application that there was a NULL value).
class NullWithoutIndicatorException : public DriverException {
public:
  explicit NullWithoutIndicatorException(
      std::string message = "Indicator variable required but not supplied", std::string sql_state = "22002",
      int32_t native_error = ODBCErrorCodes_INDICATOR_NEEDED);
};

} // namespace odbcabstraction
} // namespace driver

/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include <odbcabstraction/exceptions.h>
#include <odbcabstraction/platform.h>
#include <utility>

namespace driver {
namespace odbcabstraction {

DriverException::DriverException(std::string message, std::string sql_state,
                                 int32_t native_error)
    : msg_text_(std::move(message)),
      sql_state_(std::move(sql_state)),
      native_error_(native_error) {}

const char *DriverException::what() const throw() { return msg_text_.c_str(); }
const std::string &DriverException::GetMessageText() const { return msg_text_; }
const std::string &DriverException::GetSqlState() const { return sql_state_; }
int32_t DriverException::GetNativeError() const { return native_error_; }

AuthenticationException::AuthenticationException(std::string message, std::string sql_state,
                                                 int32_t native_error)
    : DriverException(message, sql_state, native_error) {}

NullWithoutIndicatorException::NullWithoutIndicatorException(
    std::string message, std::string sql_state, int32_t native_error)
    : DriverException(message, sql_state, native_error) {}
} // namespace odbcabstraction
} // namespace driver

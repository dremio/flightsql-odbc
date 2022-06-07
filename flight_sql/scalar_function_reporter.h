/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#pragma once

#include <arrow/type.h>

namespace driver {
namespace flight_sql {

void ReportSystemFunction(const std::string &function,
                          uint32_t &current_sys_functions,
                          uint32_t &current_convert_functions);
void ReportNumericFunction(const std::string &function,
                           uint32_t &current_functions);
void ReportStringFunction(const std::string &function,
                          uint32_t &current_functions);
void ReportDatetimeFunction(const std::string &function,
                            uint32_t &current_functions);

} // namespace flight_sql
} // namespace driver

/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#pragma once

#include <arrow/type_fwd.h>
#include <string>

namespace driver {
namespace flight_sql {

std::string ConvertToJson(const arrow::Scalar& scalar);

arrow::Result<std::shared_ptr<arrow::Array>> ConvertToJson(const std::shared_ptr<arrow::Array>& input);

} // namespace flight_sql
} // namespace driver

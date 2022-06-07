/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#pragma once

#include <arrow/type_fwd.h>
#include <memory>
#include <odbcabstraction/types.h>

namespace driver {
namespace flight_sql {

class Accessor;
class FlightSqlResultSet;

std::unique_ptr<Accessor>
CreateAccessor(arrow::Array *source_array,
               odbcabstraction::CDataType target_type);

} // namespace flight_sql
} // namespace driver

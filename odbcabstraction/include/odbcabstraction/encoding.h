/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#pragma once

#include <locale>
#include <codecvt>
#include <cstring>
#include <vector>

namespace driver {
namespace odbcabstraction {

size_t GetSqlWCharSize();

std::vector<uint8_t> Utf8ToWcs(const char* utf8_string, size_t length);

std::vector<uint8_t> Utf8ToWcs(const char* utf8_string);

std::vector<uint8_t> WcsToUtf8(const void* wcs_string);

}
}

/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#pragma once

#include <codecvt>
#include <vector>

namespace driver {
namespace odbcabstraction {

#if defined(__APPLE__)
inline size_t GetSqlWCharSize();
#else
constexpr inline size_t GetSqlWCharSize() {
  return sizeof(char16_t);
}
#endif

inline std::vector<uint8_t> Utf8ToWcs(const char* utf8_string, size_t length);

inline std::vector<uint8_t> Utf8ToWcs(const char* utf8_string);

inline std::vector<uint8_t> WcsToUtf8(const void* wcs_string);

}
}

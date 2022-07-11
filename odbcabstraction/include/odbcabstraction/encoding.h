/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#pragma once

#include <boost/algorithm/string/predicate.hpp>
#include <odbcabstraction/exceptions.h>
#include <cassert>
#include <cstring>
#include <locale>
#include <codecvt>
#include <vector>

namespace driver {
namespace odbcabstraction {

#if defined(__APPLE__)
namespace {
static size_t SqlWCharSize = 0;
}

// TODO: I think this should be set from warpdrive somehow
inline size_t GetSqlWCharSize() {
  if (SqlWCharSize == 0) {
    // TODO: Detect driver manager
    const char *env_p = std::getenv("WCHAR_ENCODING");
    if (env_p) {
      if (boost::iequals(env_p, "UTF-16")) {
        SqlWCharSize = sizeof(char16_t);
      } else if (boost::iequals(env_p, "UTF-32")) {
        SqlWCharSize = sizeof(char32_t);
      }
    } else {
      // Default to UTF32 on Mac
      SqlWCharSize = sizeof(char32_t);
    }
  }

  return SqlWCharSize;
}
#else
constexpr inline size_t GetSqlWCharSize() {
  return sizeof(char16_t);
}
#endif

namespace {

template<typename CHAR_TYPE>
inline size_t wcsstrlen(const void* wcs_string) {
  size_t len;
  for (len = 0; ((CHAR_TYPE *) wcs_string)[len]; len++);
  return len;
}

inline size_t wcsstrlen(const void* wcs_string) {
  switch (GetSqlWCharSize()) {
    case sizeof(char16_t):
      return wcsstrlen<char16_t>(wcs_string);
    case sizeof(char32_t):
      return wcsstrlen<char32_t>(wcs_string);
    default:
      assert(false);
      throw DriverException("Unknown SQLWCHAR size: " + std::to_string(GetSqlWCharSize()));
  }
}

}

template<typename CHAR_TYPE>
inline std::vector<uint8_t> Utf8ToWcs(const char *utf8_string, size_t length) {
  thread_local std::wstring_convert<std::codecvt_utf8<CHAR_TYPE>, CHAR_TYPE> converter;
  auto string = converter.from_bytes(utf8_string, utf8_string + length);

  unsigned long length_in_bytes = string.size() * GetSqlWCharSize();
  const uint8_t *data = (uint8_t*) string.data();
  std::vector<uint8_t> result(length_in_bytes);

  result.assign(data, data + length_in_bytes);
  return result;
}

inline std::vector<uint8_t> Utf8ToWcs(const char *utf8_string, size_t length) {
  switch (GetSqlWCharSize()) {
    case sizeof(char16_t):
      return Utf8ToWcs<char16_t>(utf8_string, length);
    case sizeof(char32_t):
      return Utf8ToWcs<char32_t>(utf8_string, length);
    default:
      assert(false);
      throw DriverException("Unknown SQLWCHAR size: " + std::to_string(GetSqlWCharSize()));
  }
}

inline std::vector<uint8_t> Utf8ToWcs(const char *utf8_string) {
  return Utf8ToWcs(utf8_string, strlen(utf8_string));
}

template<typename CHAR_TYPE>
inline std::vector<uint8_t> WcsToUtf8(const void *wcs_string, size_t length) {
  thread_local std::wstring_convert<std::codecvt_utf8<CHAR_TYPE>, CHAR_TYPE> converter;
  auto byte_string = converter.to_bytes((CHAR_TYPE*) wcs_string, (CHAR_TYPE*) wcs_string + length * GetSqlWCharSize());

  unsigned long length_in_bytes = byte_string.size();
  const uint8_t *data = (uint8_t*) byte_string.data();
  std::vector<uint8_t> result(length_in_bytes);

  result.assign(data, data + length_in_bytes);
  return result;
}

inline std::vector<uint8_t> WcsToUtf8(const void *wcs_string, size_t length) {
  switch (GetSqlWCharSize()) {
    case sizeof(char16_t):
      return WcsToUtf8<char16_t>(wcs_string, length);
    case sizeof(char32_t):
      return WcsToUtf8<char32_t>(wcs_string, length);
    default:
      assert(false);
      throw DriverException("Unknown SQLWCHAR size: " + std::to_string(GetSqlWCharSize()));
  }
}

inline std::vector<uint8_t> WcsToUtf8(const void* wcs_string) {
  return WcsToUtf8(wcs_string, wcsstrlen(wcs_string));
}

}
}

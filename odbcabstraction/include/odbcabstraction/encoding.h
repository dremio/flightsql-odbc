/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#pragma once

#include <odbcabstraction/exceptions.h>
#include <cassert>
#include <codecvt>
#include <cstring>
#include <locale>
#include <vector>

#if defined(__APPLE__)
#include <atomic>
#endif

namespace driver {
namespace odbcabstraction {

#if defined(__APPLE__)
extern std::atomic<size_t> SqlWCharSize;

void ComputeSqlWCharSize();

inline size_t GetSqlWCharSize() {
  if (SqlWCharSize == 0) {
    ComputeSqlWCharSize();
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
inline size_t wcsstrlen(const void *wcs_string) {
  size_t len;
  for (len = 0; ((CHAR_TYPE *) wcs_string)[len]; len++);
  return len;
}

inline size_t wcsstrlen(const void *wcs_string) {
  switch (GetSqlWCharSize()) {
    case sizeof(char16_t):
      return wcsstrlen<char16_t>(wcs_string);
    case sizeof(char32_t):
      return wcsstrlen<char32_t>(wcs_string);
    default:
      assert(false);
      throw DriverException("Encoding is unsupported, SQLWCHAR size: " + std::to_string(GetSqlWCharSize()));
  }
}

}

template<typename CHAR_TYPE>
inline void Utf8ToWcs(const char *utf8_string, size_t length, std::vector<uint8_t> *result) {
  thread_local std::wstring_convert<std::codecvt_utf8<CHAR_TYPE>, CHAR_TYPE> converter;
  auto string = converter.from_bytes(utf8_string, utf8_string + length);

  unsigned long length_in_bytes = string.size() * GetSqlWCharSize();
  const uint8_t *data = (uint8_t*) string.data();

  result->reserve(length_in_bytes);
  result->assign(data, data + length_in_bytes);
}

inline void Utf8ToWcs(const char *utf8_string, size_t length, std::vector<uint8_t> *result) {
  switch (GetSqlWCharSize()) {
    case sizeof(char16_t):
      return Utf8ToWcs<char16_t>(utf8_string, length, result);
    case sizeof(char32_t):
      return Utf8ToWcs<char32_t>(utf8_string, length, result);
    default:
      assert(false);
      throw DriverException("Encoding is unsupported, SQLWCHAR size: " + std::to_string(GetSqlWCharSize()));
  }
}

inline void Utf8ToWcs(const char *utf8_string, std::vector<uint8_t> *result) {
  return Utf8ToWcs(utf8_string, strlen(utf8_string), result);
}

template<typename CHAR_TYPE>
inline void WcsToUtf8(const void *wcs_string, size_t length_in_code_units, std::vector<uint8_t> *result) {
  thread_local std::wstring_convert<std::codecvt_utf8<CHAR_TYPE>, CHAR_TYPE> converter;
  auto byte_string = converter.to_bytes((CHAR_TYPE*) wcs_string, (CHAR_TYPE*) wcs_string + length_in_code_units);

  unsigned long length_in_bytes = byte_string.size();
  const uint8_t *data = (uint8_t*) byte_string.data();

  result->reserve(length_in_bytes);
  result->assign(data, data + length_in_bytes);
}

inline void WcsToUtf8(const void *wcs_string, size_t length_in_code_units, std::vector<uint8_t> *result) {
  switch (GetSqlWCharSize()) {
    case sizeof(char16_t):
      return WcsToUtf8<char16_t>(wcs_string, length_in_code_units, result);
    case sizeof(char32_t):
      return WcsToUtf8<char32_t>(wcs_string, length_in_code_units, result);
    default:
      assert(false);
      throw DriverException("Encoding is unsupported, SQLWCHAR size: " + std::to_string(GetSqlWCharSize()));
  }
}

inline void WcsToUtf8(const void *wcs_string, std::vector<uint8_t> *result) {
  return WcsToUtf8(wcs_string, wcsstrlen(wcs_string), result);
}

}
}

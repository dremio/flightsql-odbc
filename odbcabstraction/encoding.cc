/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include <odbcabstraction/encoding.h>

namespace driver {
namespace odbcabstraction {

static size_t	SqlWCharSize = -1;

size_t GetSqlWCharSize() {
  if (SqlWCharSize == -1) {
    // TODO: Detect driver manager
    const char *env_p = std::getenv("WCHAR_ENCODING");
    if (env_p) {
      if (strcmp(env_p, "UTF16_LE") == 0) {
        SqlWCharSize = 2;
      } else if (strcmp(env_p, "UTF32_LE") == 0) {
        SqlWCharSize = 4;
      }
    } else {
      // Default
      SqlWCharSize = 2;
    }
  }

  return SqlWCharSize;
}

template<typename CHAR_TYPE>
std::vector<uint8_t> Utf8ToWcs(const char *utf8_string, size_t length) {
  thread_local std::wstring_convert<std::codecvt_utf8<CHAR_TYPE>, CHAR_TYPE> converter;
  auto string = converter.from_bytes(utf8_string, utf8_string + length);

  unsigned long length_in_bytes = string.size() * GetSqlWCharSize();
  const uint8_t *data = (uint8_t*) string.data();
  std::vector<uint8_t> result(length_in_bytes);

  result.assign(data, data + length_in_bytes);
  return result;
}

std::vector<uint8_t> Utf8ToWcs(const char *utf8_string, size_t length) {
  switch (GetSqlWCharSize()) {
    case 2:
      return Utf8ToWcs<char16_t>(utf8_string, length);
    case 4:
      return Utf8ToWcs<char32_t>(utf8_string, length);
  }
}

std::vector<uint8_t> Utf8ToWcs(const char *utf8_string) {
  return Utf8ToWcs(utf8_string, strlen(utf8_string));
}

template<typename CHAR_TYPE>
std::vector<uint8_t> WcsToUtf8(const void *wcs_string, size_t length) {
  thread_local std::wstring_convert<std::codecvt_utf8<CHAR_TYPE>, CHAR_TYPE> converter;
  auto byte_string = converter.to_bytes((CHAR_TYPE*) wcs_string, (CHAR_TYPE*) wcs_string + length * GetSqlWCharSize());

  unsigned long length_in_bytes = byte_string.size();
  const uint8_t *data = (uint8_t*) byte_string.data();
  std::vector<uint8_t> result(length_in_bytes);

  result.assign(data, data + length_in_bytes);
  return result;
}

std::vector<uint8_t> WcsToUtf8(const void *wcs_string, size_t length) {
  switch (GetSqlWCharSize()) {
    case 2:
      return WcsToUtf8<char16_t>(wcs_string, length);
    case 4:
      return WcsToUtf8<char32_t>(wcs_string, length);
  }
}

template<typename CHAR_TYPE>
size_t wcsstrlen(const void* wcs_string) {
  size_t len;
  for (len = 0; ((CHAR_TYPE *) wcs_string)[len]; len++);
  return len;
}

size_t wcsstrlen(const void* wcs_string) {
  switch (GetSqlWCharSize()) {
    case 2:
      return wcsstrlen<char16_t>(wcs_string);
    case 4:
      return wcsstrlen<char32_t>(wcs_string);
  }
}

std::vector<uint8_t> WcsToUtf8(const void *wcs_string) {
  return WcsToUtf8(wcs_string, wcsstrlen(wcs_string));
}

} // namespace odbcabstraction
} // namespace driver

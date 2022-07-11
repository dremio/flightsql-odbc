/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include <odbcabstraction/encoding.h>

namespace driver {
namespace odbcabstraction {

typedef char32_t SqlWChar;
typedef std::basic_streambuf<SqlWChar> SqlWString;
typedef std::wstring_convert<std::codecvt_utf8<SqlWChar>, SqlWChar> CharToWStrConverter;

size_t GetSqlWCharSize() {
  return sizeof(SqlWChar);
}

std::vector<uint8_t> Utf8ToWcs(const char *utf8_string, size_t length) {
  auto string = CharToWStrConverter().from_bytes(utf8_string, utf8_string + length);

  unsigned long length_in_bytes = string.size() * GetSqlWCharSize();
  const uint8_t *data = (uint8_t*) string.data();
  std::vector<uint8_t> result(length_in_bytes);

  result.assign(data, data + length_in_bytes);
  return result;
}

std::vector<uint8_t> Utf8ToWcs(const char *utf8_string) {
  return Utf8ToWcs(utf8_string, strlen(utf8_string));
}

std::vector<uint8_t> WcsToUtf8(const void *wcs_string, size_t length) {
  auto byte_string = CharToWStrConverter().to_bytes((SqlWChar*) wcs_string, (SqlWChar*) wcs_string + length * GetSqlWCharSize());

  unsigned long length_in_bytes = byte_string.size();
  const uint8_t *data = (uint8_t*) byte_string.data();
  std::vector<uint8_t> result(length_in_bytes);

  result.assign(data, data + length_in_bytes);
  return result;
}

size_t wcsstrlen(const void* wcs_string) {
  size_t len;
  for (len = 0; ((SqlWChar *) wcs_string)[len]; len++);
  return len;
}

std::vector<uint8_t> WcsToUtf8(const void *wcs_string) {
  return WcsToUtf8(wcs_string, wcsstrlen(wcs_string));
}
} // namespace odbcabstraction
} // namespace driver

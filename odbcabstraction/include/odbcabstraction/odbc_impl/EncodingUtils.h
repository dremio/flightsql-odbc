/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#pragma once

#include <odbcabstraction/platform.h>
#include <odbcabstraction/encoding.h>
#include <sql.h>
#include <sqlext.h>
#include <algorithm>
#include <codecvt>
#include <locale>
#include <memory>
#include <string>
#include <cstring>

#define _SILENCE_CXX17_CODECVT_HEADER_DEPRECATION_WARNING

namespace ODBC {
  using namespace driver::odbcabstraction;

  // Return the number of bytes required for the conversion.
  template<typename CHAR_TYPE>
  inline size_t ConvertToSqlWChar(const std::string& str, SQLWCHAR* buffer, SQLLEN bufferSizeInBytes) {
    thread_local std::vector<uint8_t> wstr;
    Utf8ToWcs<CHAR_TYPE>(str.data(), str.size(), &wstr);
    SQLLEN valueLengthInBytes = wstr.size();

    if (buffer) {
      memcpy(buffer, wstr.data(), std::min(static_cast<SQLLEN>(wstr.size()), bufferSizeInBytes));

      // Write a NUL terminator
      if (bufferSizeInBytes >= valueLengthInBytes + GetSqlWCharSize()) {
        reinterpret_cast<CHAR_TYPE*>(buffer)[valueLengthInBytes / GetSqlWCharSize()] = '\0';
      } else {
        SQLLEN numCharsWritten = bufferSizeInBytes / GetSqlWCharSize();
        // If we failed to even write one char, the buffer is too small to hold a NUL-terminator.
        if (numCharsWritten > 0) {
          reinterpret_cast<CHAR_TYPE*>(buffer)[numCharsWritten-1] = '\0';
        }
      }
    }
    return valueLengthInBytes;
  }

  inline size_t ConvertToSqlWChar(const std::string& str, SQLWCHAR* buffer, SQLLEN bufferSizeInBytes) {
    switch (GetSqlWCharSize()) {
      case sizeof(char16_t):
        return ConvertToSqlWChar<char16_t>(str, buffer, bufferSizeInBytes);
      case sizeof(char32_t):
        return ConvertToSqlWChar<char32_t>(str, buffer, bufferSizeInBytes);
      default:
        assert(false);
        throw DriverException("Encoding is unsupported, SQLWCHAR size: " + std::to_string(GetSqlWCharSize()));
    }
  }
}

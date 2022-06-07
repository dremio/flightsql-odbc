/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#pragma once

#include <odbcabstraction/platform.h>
#include <sql.h>
#include <sqlext.h>
#include <algorithm>
#include <memory>
#include <cstring>
#include <odbcabstraction/exceptions.h>
#include <odbcabstraction/diagnostics.h>

#include <odbcabstraction/odbc_impl/EncodingUtils.h>

namespace ODBC {
template <typename T, typename O>
inline void GetAttribute(T attributeValue, SQLPOINTER output, O outputSize,
                         O *outputLenPtr) {
  if (output) {
    T *typedOutput = reinterpret_cast<T *>(output);
    *typedOutput = attributeValue;
  }

  if (outputLenPtr) {
    *outputLenPtr = sizeof(T);
  }
}

template <typename O>
inline SQLRETURN GetAttributeUTF8(const std::string &attributeValue,
                             SQLPOINTER output, O outputSize, O *outputLenPtr) {
  if (output) {
    size_t outputLenBeforeNul =
        std::min(static_cast<O>(attributeValue.size()), static_cast<O>(outputSize - 1));
    memcpy(output, attributeValue.c_str(), outputLenBeforeNul);
    reinterpret_cast<char *>(output)[outputLenBeforeNul] = '\0';
  }

  if (outputLenPtr) {
    *outputLenPtr = static_cast<O>(attributeValue.size());
  }

  if (output && outputSize < attributeValue.size() + 1) {
    return SQL_SUCCESS_WITH_INFO;
  }
  return SQL_SUCCESS;
}

template <typename O>
inline SQLRETURN GetAttributeUTF8(const std::string &attributeValue,
                                  SQLPOINTER output, O outputSize, O *outputLenPtr, driver::odbcabstraction::Diagnostics& diagnostics) {
  SQLRETURN result = GetAttributeUTF8(attributeValue, output, outputSize, outputLenPtr);
  if (SQL_SUCCESS_WITH_INFO == result) {
    diagnostics.AddTruncationWarning();
  }
  return result;
}

template <typename O>
inline SQLRETURN GetAttributeSQLWCHAR(const std::string &attributeValue, bool isLengthInBytes,
                                 SQLPOINTER output, O outputSize,
                                 O *outputLenPtr) {
  size_t result = ConvertToSqlWChar(
      attributeValue, reinterpret_cast<SQLWCHAR *>(output), isLengthInBytes ? outputSize : outputSize * sizeof(SQLWCHAR));

  if (outputLenPtr) {
    *outputLenPtr = static_cast<O>(isLengthInBytes ? result : result / sizeof(SQLWCHAR));
  }

  if (output && outputSize < result + (isLengthInBytes ? sizeof(SQLWCHAR) : 1)) {
    return SQL_SUCCESS_WITH_INFO;
  }
  return SQL_SUCCESS;
}

template <typename O>
inline SQLRETURN GetAttributeSQLWCHAR(const std::string &attributeValue, bool isLengthInBytes,
                                      SQLPOINTER output, O outputSize,
                                      O *outputLenPtr, driver::odbcabstraction::Diagnostics& diagnostics) {
  SQLRETURN result = GetAttributeSQLWCHAR(attributeValue, isLengthInBytes, output, outputSize, outputLenPtr);
  if (SQL_SUCCESS_WITH_INFO == result) {
    diagnostics.AddTruncationWarning();
  }
  return result;
}

template <typename O>
inline SQLRETURN
GetStringAttribute(bool isUnicode, const std::string &attributeValue, bool isLengthInBytes,
                   SQLPOINTER output, O outputSize, O *outputLenPtr, driver::odbcabstraction::Diagnostics& diagnostics) {
  SQLRETURN result = SQL_SUCCESS;
  if (isUnicode) {
    result = GetAttributeSQLWCHAR(attributeValue, isLengthInBytes, output, outputSize, outputLenPtr);
  } else {
    result = GetAttributeUTF8(attributeValue, output, outputSize, outputLenPtr);
  }

  if (SQL_SUCCESS_WITH_INFO == result) {
    diagnostics.AddTruncationWarning();
  }
  return result;
}

template <typename T>
inline void SetAttribute(SQLPOINTER newValue, T &attributeToWrite) {
  SQLLEN valueAsLen = reinterpret_cast<SQLLEN>(newValue);
  attributeToWrite = static_cast<T>(valueAsLen);
}

template <typename T>
inline void SetPointerAttribute(SQLPOINTER newValue, T &attributeToWrite) {
  attributeToWrite = static_cast<T>(newValue);
}

inline void SetAttributeUTF8(SQLPOINTER newValue, SQLINTEGER inputLength,
                             std::string &attributeToWrite) {
  const char *newValueAsChar = static_cast<const char *>(newValue);
  attributeToWrite.assign(newValueAsChar, inputLength == SQL_NTS
                                              ? strlen(newValueAsChar)
                                              : inputLength);
}

inline void SetAttributeSQLWCHAR(SQLPOINTER newValue,
                                 SQLINTEGER inputLengthInBytes,
                                 std::string &attributeToWrite) {
  SqlWString wstr;
  if (inputLengthInBytes == SQL_NTS) {
    wstr.assign(reinterpret_cast<SqlWChar *>(newValue));
  } else {
    wstr.assign(reinterpret_cast<SqlWChar *>(newValue),
                inputLengthInBytes / sizeof(SqlWChar));
  }
  attributeToWrite = CharToWStrConverter().to_bytes(wstr.c_str());
}

template <typename T>
void CheckIfAttributeIsSetToOnlyValidValue(SQLPOINTER value, T allowed_value) {
  if (static_cast<T>(reinterpret_cast<SQLULEN>(value)) != allowed_value) {
    throw driver::odbcabstraction::DriverException("Optional feature not implemented", "HYC00");
  }
}
}

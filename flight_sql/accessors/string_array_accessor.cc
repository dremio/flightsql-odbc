/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include "string_array_accessor.h"

#include <arrow/array.h>
#include <boost/locale.hpp>
#include <odbcabstraction/encoding.h>

namespace driver {
namespace flight_sql {

using namespace arrow;
using namespace odbcabstraction;

namespace {

#if defined _WIN32 || defined _WIN64
std::string utf8_to_clocale(const char *utf8str, int len)
{
  boost::locale::generator g;
  g.locale_cache_enabled(true);
  std::locale loc = g(boost::locale::util::get_system_locale());
  return boost::locale::conv::from_utf<char>(utf8str, utf8str + len, loc);
}
#endif

template <typename CHAR_TYPE>
inline RowStatus MoveSingleCellToCharBuffer(std::vector<uint8_t> &buffer,
                                            ColumnBinding *binding,
                                            StringArray *array, int64_t arrow_row, int64_t i,
                                            int64_t &value_offset,
                                            bool update_value_offset,
                                            odbcabstraction::Diagnostics &diagnostics) {
  RowStatus result = odbcabstraction::RowStatus_SUCCESS;

  // Arrow strings come as UTF-8
  const char *raw_value = array->Value(arrow_row).data();
  const size_t raw_value_length = array->value_length(arrow_row);
  const void *value;

  size_t size_in_bytes;
#if defined _WIN32 || defined _WIN64
  std::string clocale_str;
#endif
  if (sizeof(CHAR_TYPE) > sizeof(char)) {
    Utf8ToWcs(raw_value, raw_value_length, &buffer);
    value = buffer.data();
    size_in_bytes = buffer.size();
  } else {
#if defined _WIN32 || defined _WIN64
    // Convert to C locale string
    clocale_str = utf8_to_clocale(raw_value, raw_value_length);
    const char* clocale_data = clocale_str.data();
    size_t clocale_length = clocale_str.size();

    value = clocale_data;
    size_in_bytes = clocale_length;
#else
    value = raw_value;
    size_in_bytes = raw_value_length;
#endif
  }

  size_t remaining_length = static_cast<size_t>(size_in_bytes - value_offset);
  size_t value_length =
      std::min(remaining_length,
               binding->buffer_length);

  auto *byte_buffer =
      static_cast<char *>(binding->buffer) + i * binding->buffer_length;
  auto *char_buffer = (CHAR_TYPE *)byte_buffer;
  memcpy(char_buffer, ((char *)value) + value_offset, value_length);

  // Write a NUL terminator
  if (binding->buffer_length >= remaining_length + sizeof(CHAR_TYPE)) {
    char_buffer[remaining_length / sizeof(CHAR_TYPE)] = '\0';
    if (update_value_offset) {
      value_offset = -1;
    }
  } else {
    result = odbcabstraction::RowStatus_SUCCESS_WITH_INFO;
    diagnostics.AddTruncationWarning();
    size_t chars_written = binding->buffer_length / sizeof(CHAR_TYPE);
    // If we failed to even write one char, the buffer is too small to hold a
    // NUL-terminator.
    if (chars_written > 0) {
      char_buffer[(chars_written - 1)] = '\0';
      if (update_value_offset) {
        value_offset += binding->buffer_length - sizeof(CHAR_TYPE);
      }
    }
  }

  if (binding->strlen_buffer) {
    binding->strlen_buffer[i] = static_cast<ssize_t>(remaining_length);
  }

  return result;
}

} // namespace

template <CDataType TARGET_TYPE, typename CHAR_TYPE>
StringArrayFlightSqlAccessor<TARGET_TYPE, CHAR_TYPE>::StringArrayFlightSqlAccessor(
    Array *array)
    : FlightSqlAccessor<StringArray, TARGET_TYPE,
                        StringArrayFlightSqlAccessor<TARGET_TYPE, CHAR_TYPE>>(array) {}

template <CDataType TARGET_TYPE, typename CHAR_TYPE>
RowStatus StringArrayFlightSqlAccessor<TARGET_TYPE, CHAR_TYPE>::MoveSingleCell_impl(
        ColumnBinding *binding, int64_t arrow_row, int64_t i, int64_t &value_offset,
        bool update_value_offset, odbcabstraction::Diagnostics &diagnostics) {
    return MoveSingleCellToCharBuffer<CHAR_TYPE>(buffer_, binding, this->GetArray(), arrow_row, i, value_offset, update_value_offset, diagnostics);
}

template <CDataType TARGET_TYPE, typename CHAR_TYPE>
size_t StringArrayFlightSqlAccessor<TARGET_TYPE, CHAR_TYPE>::GetCellLength_impl(ColumnBinding *binding) const {
  return binding->buffer_length;
}

template class StringArrayFlightSqlAccessor<odbcabstraction::CDataType_CHAR, char>;
template class StringArrayFlightSqlAccessor<odbcabstraction::CDataType_WCHAR, char16_t>;
template class StringArrayFlightSqlAccessor<odbcabstraction::CDataType_WCHAR, char32_t>;

} // namespace flight_sql
} // namespace driver

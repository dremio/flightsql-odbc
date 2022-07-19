/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include <odbcabstraction/encoding.h>

#if defined(__APPLE__)
#include <boost/algorithm/string/predicate.hpp>
#include <dlfcn.h>
#include <mutex>
#endif

namespace driver {
namespace odbcabstraction {

#if defined(__APPLE__)
namespace {
std::mutex SqlWCharSizeMutex;

bool IsUsingIODBC() {
  // Detects iODBC by looking up by symbol iodbc_version
  void* handle = dlsym(RTLD_DEFAULT, "iodbc_version");
  bool using_iodbc = handle != nullptr;
  dlclose(handle);

  return using_iodbc;
}
}

void ComputeSqlWCharSize() {
  std::unique_lock<std::mutex> lock(SqlWCharSizeMutex);
  if (SqlWCharSize != 0) return;  // double-checked locking

  const char *env_p = std::getenv("WCHAR_ENCODING");
  if (env_p) {
    if (boost::iequals(env_p, "UTF-16")) {
      SqlWCharSize = sizeof(char16_t);
      return;
    } else if (boost::iequals(env_p, "UTF-32")) {
      SqlWCharSize = sizeof(char32_t);
      return;
    }
  }

  SqlWCharSize = IsUsingIODBC() ? sizeof(char32_t) : sizeof(char16_t);
}
#endif

} // namespace odbcabstraction
} // namespace driver

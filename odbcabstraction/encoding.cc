/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include <odbcabstraction/encoding.h>

#if defined(__APPLE__)
#include <dlfcn.h>
#endif

namespace driver {
namespace odbcabstraction {

#if defined(__APPLE__)
namespace {
bool IsUsingIODBC() {
  // Detects iODBC by looking up by symbol iodbc_version
  void* handle = dlsym(RTLD_DEFAULT, "iodbc_version");
  bool using_iodbc = handle != nullptr;
  dlclose(handle);

  return using_iodbc;
}
}

size_t ComputeSqlWCharSize() {
  const char *env_p = std::getenv("WCHAR_ENCODING");
  if (env_p) {
    if (boost::iequals(env_p, "UTF-16")) {
      return sizeof(char16_t);
    } else if (boost::iequals(env_p, "UTF-32")) {
      return sizeof(char32_t);
    }
  }

  return IsUsingIODBC() ? sizeof(char32_t) : sizeof(char16_t);
}
#endif

} // namespace odbcabstraction
} // namespace driver

#pragma once

#if defined(_WIN32)
  // NOMINMAX avoids std::min/max being defined as a c macro
  #define NOMINMAX
  #include <windows.h>
    
  #include <basetsd.h>
  typedef SSIZE_T ssize_t;
#endif
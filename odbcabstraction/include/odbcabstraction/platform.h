/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#pragma once

#if defined(_WIN32)
  // NOMINMAX avoids std::min/max being defined as a c macro
  #ifndef NOMINMAX
  #define NOMINMAX
  #endif

  // Avoid including extraneous Windows headers.
  #ifndef WIN32_LEAN_AND_MEAN
  #define WIN32_LEAN_AND_MEAN
  #endif

  #include <windows.h>
    
  #include <basetsd.h>
  typedef SSIZE_T ssize_t;

#endif

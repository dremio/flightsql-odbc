# FlightSQL ODBC

This repository contains Dremio's Flight SQL ODBC driver implementation, supporting libraries, and a small ODBC performance-testing tool.

## Layout

- `flight_sql/` - core Flight SQL ODBC driver sources, configuration handling, UI code, and unit tests.
- `odbcabstraction/` - ODBC abstraction layer used by the driver.
- `odbc_perf_testing/` - Python-based profiling and performance test helpers.
- `build_win32.bat` - Windows x86 build entrypoint.
- `build_win64.bat` - Windows x64 build entrypoint.
- `vcpkg.json` - native dependency manifest.

## Build

The repository is built with CMake and uses `vcpkg` for native dependencies.

Windows convenience scripts:

```bat
build_win64.bat
build_win32.bat
```

The Windows scripts expect:

- `VCPKG_ROOT` to point at a prepared `vcpkg` checkout
- `ARROW_GIT_REPOSITORY` optionally set to an Apache Arrow repository or fork

For non-Windows builds, use the top-level `CMakeLists.txt` directly and make sure Boost plus an ODBC implementation are available. On macOS, the project is set up for `libiodbc`.

## Testing

- `flight_sql/` includes GoogleTest-based unit tests that build with the CMake targets.
- Performance/profiling helpers live under [odbc_perf_testing/README.md](odbc_perf_testing/README.md).

## Notes

- This repo is separate from the packaging-oriented `odbc/` repository.
- The checked-in batch scripts are the clearest starting point for reproducing the intended Windows build flow.

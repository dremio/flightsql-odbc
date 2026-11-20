# REVIEW.md

Read this before reviewing a change to the Flight SQL ODBC driver.

## What failure looks like here

Rank findings by cost, most costly first:

1. **Wrong data, no error.** An Arrow-to-ODBC conversion silently changes a value, type, precision, scale or nullability. Check `flight_sql/accessors/*` (e.g. the rescale in `decimal_array_accessor.cc:35`), the type mapping in `flight_sql/utils.cc` (`GetDataTypeFromArrowField_V3`, `ConvertArrowTypeToC`) and the column metadata in `flight_sql_result_set_metadata.cc`.
2. **Auth/TLS weakened or secrets leaked.** A default flips (`useEncryption`/`disableCertificateVerification` at `flight_sql_connection.cc:162-163`), a cert is no longer verified, or a password/token ends up in a log, diagnostic or exception message (see the pattern at `flight_sql_auth_method.cc:124`).
3. **Connection string misroutes.** `config/connection_string_parser.cc` or the property lookups in `flight_sql_connection.cc` / `flight_sql_auth_method.cc` drop, rename or mis-split a key (braced values, `UID`/`PWD` aliases, case), so the driver connects to the wrong host/port or with the wrong auth.
4. **ODBC API conformance breaks.** A handle, descriptor or attribute in `odbcabstraction/odbc_impl/*` returns the wrong `SQLRETURN`, length/indicator or SQLSTATE; BI tools fail even when unit tests pass.
5. **Build breaks on a supported platform.** Windows x86/x64 (`build_win32.bat`, `build_win64.bat`), macOS with iODBC (`CMakeLists.txt:16-19`), or Linux; or the pinned Arrow (`flight_sql/CMakeLists.txt:113-119`) / `vcpkg.json` baseline moves without reason.

## Focus

- `flight_sql/accessors/`, `flight_sql/utils.cc`, `flight_sql/flight_sql_result_set*.cc`, `flight_sql/record_batch_transformer.cc`
- `flight_sql/flight_sql_connection.cc`, `flight_sql_auth_method.cc`, `flight_sql_ssl_config.cc`, `system_trust_store.cc`, `flight_sql/config/`
- `odbcabstraction/odbc_impl/`, `odbcabstraction/include/odbcabstraction/spi/`
- `CMakeLists.txt`, `flight_sql/CMakeLists.txt`, `odbcabstraction/CMakeLists.txt`, `build_win*.bat`, `vcpkg.json`
- A behavior change with no matching `*_test.cc` change is a finding.

## Skip

- `odbcabstraction/whereami.cc`, `odbcabstraction/whereami.h` (vendored third-party)
- Build output and fetched deps: `build/`, `_deps/`, `vcpkg_installed/`, the `ApacheArrow` ExternalProject tree, googletest from `FetchContent`
- `flight_sql/ui/` layout tweaks and `odbc_perf_testing/` (Python profiling helper) unless they touch credentials or connection properties

## Nits

At most 3. Report them only if you found nothing else, or label each `nit:`. Formatting, naming and include order are not findings.

## Evidence

Every finding cites `file:line` and states what the code there actually does. Nothing based only on a name, signature or comment. If you could not confirm it in the code, leave it out.

## Summary format

Start with a count line, e.g. `2 issues to fix, 1 nit`. If there is nothing, write `Nothing to fix.` No praise, no recap of the diff.

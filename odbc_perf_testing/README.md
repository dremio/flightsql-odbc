# ODBC Perf Testing Tool

---

## Examples:

### test_case=test-fetch-all

```
./main.py  --sql_query="SELECT * FROM table_name" py --driver /home/user/odbc_driver/_build/release/libarrow-odbc.so --host localhost --port 32010 --user username --password password123 pyodbc test-fetch-all
```

### test_case=test-sql-type-*<type_name>*

```
./main.py --driver /home/user/odbc_driver/_build/release/libarrow-odbc.so --host localhost --port 32010 --user username --password password123 pyodbc test-sql-type-all
```

#### Available data types: boolean, float, double, decimal, int, bigint, date, time, timestamp, intervalday (day to seconds), intervalyear (year to month), varchar, struct, list, all

---

### Script usage help:

```
usage: main.py [-h] [--user_connection_string USER_CONNECTION_STRING] [--driver DRIVER] [--dsn DSN] [--host HOST] [--port PORT] [--user USER] 
               [--password PASSWORD] [--token TOKEN] [--use_encryption USE_ENCRYPTION] [--trusted_certs TRUSTED_CERTS] 
               [--use_system_trust_store USE_SYSTEM_TRUST_STORE] [--disable_certificate_verification DISABLE_CERTIFICATE_VERIFICATION] 
               [--sql_query SQL_QUERY] [--library_options LIBRARY_OPTIONS]
               odbc_library test_case

Create test scenarios for profiling in other tools.

positional arguments:
  odbc_library                                                          Which ODBC Library to use ['pyodbc', 'turbodbc']
  test_case                                                             Which test case to run ['test-fetch-all', 'test-sql-type-{type_name}']

optional arguments:
  -h, --help                                                            show this help message and exit
  --user_connection_string USER_CONNECTION_STRING                       The ODBC Driver Path
  --driver DRIVER                                                       The ODBC Driver Path
  --dsn DSN                                                             The ODBC Driver Data Source Name
  --host HOST                                                           The host to connect
  --port PORT                                                           The port to connect
  --user USER                                                           The user to authenticate
  --password PASSWORD                                                   The password to authenticate
  --token TOKEN                                                         Defines the token for Token Authentication
  --use_encryption USE_ENCRYPTION                                       Use SSL Connections
  --trusted_certs TRUSTED_CERTS                                         Defines the certificates path
  --use_system_trust_store USE_SYSTEM_TRUST_STORE                       Tells whether the driver should use the system's Trust Store
  --disable_certificate_verification DISABLE_CERTIFICATE_VERIFICATION   Tells the driver to ignore certificate verification.
  --sql_query SQL_QUERY                                                 The SQL Query to run (only affects "test-fetch-all")
  --library_options LIBRARY_OPTIONS                                     Extra library-specific connection options in JSON format '{k: v}'

```

#### You can connect with either a connection string `--user_connection_string`, your DSN `--dsn`, or provide it via arguments.

##### Note you can't override your DSN configuration using the optional arguments.

---

#### Example for `--library_options`:

```
--library_options "{\"library_specific_option_1\": true, \"library_specific_option_2\": 100000}"
```

##### Those options will be passed as kwargs to the `connect` method of each library, make sure they're written correctly.

##### _NOTE: It might also override connection string parameters dependending on the library's implementation._

---

# ODBC Perf Testing Tool

---

## Examples:

### test_case=test-fetch-all

```
./main.py  --sql_query="SELECT * FROM table_name" "/home/user/driver_folder/_build/release/libarrow-odbc.so" "localhost" "32010" "username" "password" "pyodbc" "test-fetch-all"
```

### test_case=test-sql-type-*type_name*

```
./main.py "/home/user/driver_folder/_build/release/libarrow-odbc.so" "localhost" "32010" "username" "password" "pyodbc" "test-sql-type-boolean"
```

#### Available data types: boolean, float, double, int, bigint, date, time, timestamp, varchar

---

### Script usage help:

```
usage: main.py [-h] [--sql_query SQL_QUERY] [--dsn DSN] [--use_encryption USE_ENCRYPTION] driver host port user password odbc_library test_case

Create test scenarios for profiling in other tools.

positional arguments:
  driver                            The ODBC Driver Path
  host                              The host to connect
  port                              The port to connect
  user                              The user to authenticate
  password                          The password to authenticate
  odbc_library                      Which ODBC Library to use ["pyodbc"|"turbodbc"]
  test_case                         Which test case to run ["test-fetch-all"|"test-sql-type-*type_name*"]

optional arguments:
  -h, --help                        show this help message and exit
  --sql_query SQL_QUERY             The SQL Query to run (only affects "test-fetch-all")
  --dsn DSN                         The ODBC Driver Data Source Name
  --use_encryption USE_ENCRYPTION   Use SSL Connections
```
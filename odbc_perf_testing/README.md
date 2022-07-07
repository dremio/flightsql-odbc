### WIP

```
usage: main.py [-h] [--sql_query SQL_QUERY] [--dsn DSN] [--use_encryption USE_ENCRYPTION] driver host port user password odbc_library test_case

Create test scenarios for profiling in other tools.

positional arguments:
  driver                The ODBC Driver Path
  host                  The host to connect
  port                  The port to connect
  user                  The user to authenticate
  password              The password to authenticate
  odbc_library          Which ODBC Library to use ["pyodbc"|"turbodbc"]
  test_case             Which test case to run ["test-fetch-all"|"test-all-sql-types"]

optional arguments:
  -h, --help            show this help message and exit
  --sql_query SQL_QUERY
                        The SQL Query to run (only "test-fetch-all")
  --dsn DSN             The ODBC Driver Data Source Name
  --use_encryption USE_ENCRYPTION
                        Use SSL Connections
```
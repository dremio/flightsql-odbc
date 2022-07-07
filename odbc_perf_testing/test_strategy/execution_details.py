from dataclasses import dataclass


@dataclass
class ConnectionDetails:
    """
    Class for storing connection information like host, port, user, password.
    """
    dsn: str
    driver: str
    host: str
    port: int
    user: str
    password: str
    use_encryption: bool
    connected: bool = False


@dataclass
class TestDetails:
    """
    Class for storing test information like which ODBC library to use, and the SQL query that should run.
    """
    odbc_library: str
    sql_query: str
    test_case: str


@dataclass
class ExecutionDetails:
    """
    Class for storing the connection and test details.
    """
    connection_details: ConnectionDetails
    test_details: TestDetails

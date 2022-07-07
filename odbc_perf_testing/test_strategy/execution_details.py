#
# Copyright (C) 2020-2022 Dremio Corporation
#
# See "LICENSE" for license information.

from dataclasses import dataclass
from typing import Any, Dict


@dataclass
class ConnectionDetails:
    """
    Class for storing connection information like host, port, user, password.
    """
    dsn: str
    host: str
    port: int
    user: str
    driver: str
    password: str
    use_encryption: bool
    token: str = lambda: ''
    trusted_certs: str = lambda: ''
    connected: bool = lambda: False
    user_connection_string: str = lambda: ''
    use_system_trust_store: bool = lambda: True
    library_options: Dict[str, Any] = lambda: dict()
    disable_certificate_verification: bool = lambda: False


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

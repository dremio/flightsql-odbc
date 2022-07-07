#
# Copyright (C) 2020-2022 Dremio Corporation
#
# See "LICENSE" for license information.

from pyodbc import Row, Cursor, Connection, connect

from test_strategy.base_strategy import BaseStrategy
from test_strategy.execution_details import ExecutionDetails, ConnectionDetails, TestDetails


class PyOdbcStrategy(BaseStrategy):
    """
    Implements a BaseStrategy for PyODBC.
    """

    def __init__(self, execution_details: ExecutionDetails) -> None:
        super().__init__()
        self._execution_details: ExecutionDetails = execution_details
        self._connection: Connection = self._connect_with_execution_details(execution_details)
        execution_details.connection_details.connected = True

    @property
    def execution_details(self) -> ExecutionDetails:
        return self._execution_details

    @execution_details.setter
    def execution_details(self, value: ExecutionDetails) -> None:
        self._execution_details = value

    def fetch_all(self) -> list[Row]:
        cursor: Cursor = self._get_cursor()
        test_details: TestDetails = self.execution_details.test_details
        sql_query: str = test_details.sql_query

        cursor.execute(sql_query)
        return cursor.fetchall()

    def set_sql_query(self, value: str) -> None:
        test_details: TestDetails = self.execution_details.test_details
        test_details.sql_query = value

    def _get_cursor(self) -> Cursor:
        return self._connection.cursor()

    @staticmethod
    def _connect_with_execution_details(execution_details: ExecutionDetails) -> Connection:
        conn_details: ConnectionDetails = execution_details.connection_details

        if not conn_details.dsn:  # Uses custom properties if no DSN is provided for flexibility
            return connect(
                f"Driver={conn_details.driver};"
                f"HOST={conn_details.host};PORT={conn_details.port};"
                f"UID={conn_details.user};PWD={conn_details.password};"
                f"useEncryption={int(conn_details.use_encryption)};",  # bool to int
                f"token={conn_details.token};" if conn_details.token else ""
                f"trustStore={conn_details.trust_store};" if conn_details.trust_store else ""
                f"trustStorePassword={conn_details.trust_store_password};" if conn_details.trust_store_password else ""
                f"useSystemTrustStore={int(conn_details.use_encryption)};"  # bool to int
                f"disableCertificateVerification={int(conn_details.use_encryption)}",  # bool to int
                autocommit=True,  # transactions not supported in Flight SQL ODBC
                **conn_details.library_options
            )
        else:
            return connect(
                f"DSN={conn_details.dsn}",
                autocommit=True,  # transactions not supported in Flight SQL ODBC
                **conn_details.library_options
            )

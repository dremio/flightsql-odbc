#
# Copyright (C) 2020-2022 Dremio Corporation
#
# See "LICENSE" for license information.
from typing import Any

from turbodbc import Rows, connect, make_options
from turbodbc.connect import Connection
from turbodbc.cursor import Cursor
from turbodbc_intern import Options

from test_strategy.base_strategy import BaseStrategy
from test_strategy.execution_details import ExecutionDetails, ConnectionDetails, TestDetails


class TurbodbcStrategy(BaseStrategy):
    """
    Implements a BaseStrategy for Turbodbc.
    """

    def __init__(self, execution_details: ExecutionDetails) -> None:
        super().__init__()
        self._execution_details: ExecutionDetails = execution_details
        self._connection: Connection = self._connect_with_execution_details(execution_details)

    @property
    def execution_details(self) -> ExecutionDetails:
        return self._execution_details

    @execution_details.setter
    def execution_details(self, value) -> None:
        self._execution_details = value

    def fetch_all(self) -> list[Rows]:
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
        connection_details: ConnectionDetails = execution_details.connection_details
        extra_opts: dict[str, Any] = {
            'autocommit': True
        }
        options: Options = make_options(**extra_opts)

        if not connection_details.dsn:  # Uses custom properties if no DSN is provided for flexibility
            return connect(
                f"Driver={connection_details.driver};"
                f"Server={connection_details.host};Port={connection_details.port};"
                f"Uid={connection_details.user};Pwd={connection_details.password};"
                f"useEncryption={1 if connection_details.use_encryption else 0}",
                turbodbc_options=options)
        else:
            return connect(dsn=connection_details.dsn, turbodbc_options=options)

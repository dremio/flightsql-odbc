#
# Copyright (C) 2020-2022 Dremio Corporation
#
# See "LICENSE" for license information.

from abc import ABC, abstractmethod  # NOTE: ABC is Python >= 3.4
from enum import Enum
from typing import Any, Iterable, List

from test_strategy.execution_details import ExecutionDetails, ConnectionDetails


class BaseStrategy(ABC):
    """
    Interface for testing with different libraries.
    """

    @property
    @abstractmethod
    def execution_details(self) -> ExecutionDetails:
        """
        Defines an abstract property getter that can be used as `x.execution_details`.
        """
        pass

    @execution_details.setter
    @abstractmethod
    def execution_details(self, value: ExecutionDetails) -> None:
        """
        Defines an abstract property setter that can be used as `x.execution_details = val`.
        """
        pass

    @abstractmethod
    def fetch_all(self) -> Iterable[Any]:
        """
        Defines an abstract method that should interface with DBAPI's 'fetch_all' of the respective library.
        """
        pass

    @abstractmethod
    def set_sql_query(self, value: str) -> None:
        """
        Defines an abstract method that should change the TestDetail's SQL Query to a given value.
        """
        pass

    class ConnectionType(Enum):
        USER_DEFINED_STRING = 1
        DSN_DEFINED_PROPERTIES = 2
        PARAMETER_DEFINED_PROPERTIES = 3
        _REQUIRED_PARAMETERS: List[str] = ['driver', 'host', 'port', 'user', 'password']

        @staticmethod
        def get_connection_type(conn_details: ConnectionDetails):
            static_self = BaseStrategy.ConnectionType
            if conn_details.user_connection_string:
                return static_self.USER_DEFINED_STRING
            elif conn_details.dsn:
                return static_self.DSN_DEFINED_PROPERTIES
            elif all(val for key, val in conn_details.__dict__.items() if key in static_self._REQUIRED_PARAMETERS):
                # Proceed if all required parameters have truthy values
                return static_self.PARAMETER_DEFINED_PROPERTIES
            else:
                # All parameters are optional, so they'll all have '--'
                raise ValueError(
                    f'To run this tool you must specify either '
                    f'--user_connection_string, '
                    f'--dsn, '
                    f'or at least {["--" + val for val in static_self._REQUIRED_PARAMETERS]}')

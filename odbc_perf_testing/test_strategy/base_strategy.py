#
# Copyright (C) 2020-2022 Dremio Corporation
#
# See "LICENSE" for license information.

from abc import ABC, abstractmethod  # NOTE: ABC is Python >= 3.4
from typing import Any, Iterable

from test_strategy.execution_details import ExecutionDetails


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

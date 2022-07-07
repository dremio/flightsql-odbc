from abc import ABC, abstractmethod  # NOTE: ABC is Python >= 3.4
from typing import Any, Iterable

from test_strategy.execution_details import ExecutionDetails


class ExecutableStrategy(ABC):
    """
    Interface for testing with different libraries.
    """

    @property
    @abstractmethod
    def execution_details(self) -> ExecutionDetails:
        """Implementing classes should implement this property getter that returns the underlying ExecutionDetails"""
        pass

    @execution_details.setter
    @abstractmethod
    def execution_details(self, value) -> None:
        pass

    @abstractmethod
    def fetch_all(self) -> Iterable[Any]:
        pass

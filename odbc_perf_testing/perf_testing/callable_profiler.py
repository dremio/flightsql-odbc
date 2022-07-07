from cProfile import Profile
from io import StringIO
from pstats import Stats
from typing import Callable, Any, Iterable


class CallableProfiler:
    """
    Helper class for profiling methods by calling them using the Callable interface.
    """

    def __init__(self) -> None:
        super().__init__()
        self._profiler: Profile = Profile()

    def profile_single_method(self, runnable: Callable[[], Any]) -> None:
        self._profiler.enable()
        runnable()
        self._profiler.disable()

    def profile_multiple_methods(self, runnables: Iterable[Callable[[], Any]]) -> None:
        for runnable in runnables:
            self.profile_single_method(runnable=runnable)

    def save_results_to_file(self, file_path: str, sort_by: str = 'tottime'):
        string_buffer: StringIO = StringIO()
        Stats(self._profiler, stream=string_buffer).sort_stats(sort_by).print_stats()
        with open(file_path, 'w') as save_file:  # overwrite by default
            save_file.write(string_buffer.getvalue())

    def print_results(self, sort_by: str = 'tottime'):
        Stats(self._profiler).sort_stats(sort_by).print_stats()

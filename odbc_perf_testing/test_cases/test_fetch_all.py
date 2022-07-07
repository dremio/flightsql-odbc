from typing import Callable, Any

from test_strategy.executable_strategy import ExecutableStrategy


def run(process_name: str, create_executable: Callable[[], ExecutableStrategy]):
    profiling_func: Callable[[], Any] = lambda: create_executable().fetch_all()
    print(f'{process_name} starting...')
    profiling_func()
    print(f'{process_name} finished.')

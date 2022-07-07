from multiprocessing import Process
from typing import Callable, Any

from test_strategy.executable_strategy import ExecutableStrategy


def run(process_name: str, create_executable: Callable[[], ExecutableStrategy]):
    profiling_func: Callable[[], Any] = lambda: create_executable().fetch_all()
    test_process: Process = Process(target=profiling_func, name=process_name)
    test_process.start()
    print(f'Process for {process_name} started at pid {test_process.pid}')

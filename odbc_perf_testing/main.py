from typing import Callable, Any

from perf_testing.callable_profiler import CallableProfiler
from test_strategy.executable_strategy import ExecutableStrategy
from test_strategy.execution_details import ExecutionDetails, ConnectionDetails, TestDetails
from test_strategy.pyodbc_strategy import PyOdbcStrategy
from test_strategy.turbodbc_strategy import TurbodbcStrategy


def run(execution_details: ExecutionDetails):
    profiler: CallableProfiler = CallableProfiler()
    test_details: TestDetails = execution_details.test_details

    # TODO: Use argparse and add shebang py3 for scripting usage
    if test_details.odbc_library is None:
        exit(1)
    elif test_details.odbc_library.lower() == 'pyodbc':
        executable: ExecutableStrategy = PyOdbcStrategy(execution_details=execution_details)
        profiling_func: Callable[[], Any] = lambda: executable.fetch_all()
        profiler.profile_single_method(profiling_func)
        profiler.print_results()
    elif test_details.odbc_library.lower() == 'turbodbc':
        executable: ExecutableStrategy = TurbodbcStrategy(execution_details=execution_details)
        profiling_func: Callable[[], Any] = lambda: executable.fetch_all()
        profiler.profile_single_method(profiling_func)
        profiler.print_results()
    else:
        exit(1)


if __name__ == '__main__':
    connection_detail: ConnectionDetails = ConnectionDetails(
        dsn="",
        driver='/home/vfraga/warpdrive/_build/release/libarrow-odbc.so',
        host='automaster.drem.io',
        port=32010,
        user='dremio',
        password='dremio123',
        use_encryption=False
    )
    test_detail: TestDetails = TestDetails(
        odbc_library='pyodbc',
        sql_query='SELECT * FROM Samples."samples.dremio.com"."SF_incidents2016.json"'
    )
    execution_detail: ExecutionDetails = ExecutionDetails(
        connection_details=connection_detail,
        test_details=test_detail
    )
    run(execution_details=execution_detail)

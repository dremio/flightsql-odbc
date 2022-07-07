from multiprocessing import Process
from typing import Callable, Any

from test_strategy.executable_strategy import ExecutableStrategy
from test_strategy.execution_details import ExecutionDetails, ConnectionDetails, TestDetails
from test_strategy.pyodbc_strategy import PyOdbcStrategy
from test_strategy.turbodbc_strategy import TurbodbcStrategy


def start_new_test_process(test_case: str, create_executable: Callable[[], ExecutableStrategy]) -> None:
    test_case_all_lower: str = test_case.lower()
    if test_case_all_lower == 'test-fetch-all':
        profiling_func: Callable[[], Any] = lambda: create_executable().fetch_all()
        test_process: Process = Process(target=profiling_func, name=test_case)
        test_process.start()


def run(execution_details: ExecutionDetails):
    test_details: TestDetails = execution_details.test_details

    # TODO: Use argparse and add shebang py3 for scripting usage
    if test_details.odbc_library is None:
        exit(1)
    elif test_details.odbc_library.lower() == 'pyodbc':
        start_new_test_process(test_details.test_case, lambda: PyOdbcStrategy(execution_details=execution_details))
    elif test_details.odbc_library.lower() == 'turbodbc':
        start_new_test_process(test_details.test_case, lambda: TurbodbcStrategy(execution_details=execution_details))
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
        sql_query='SELECT * FROM Samples."samples.dremio.com"."SF_incidents2016.json"',
        test_case='test-fetch-all'
    )
    execution_detail: ExecutionDetails = ExecutionDetails(
        connection_details=connection_detail,
        test_details=test_detail
    )
    run(execution_details=execution_detail)

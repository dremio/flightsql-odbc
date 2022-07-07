#!/usr/bin/env python3
#
# Copyright (C) 2020-2022 Dremio Corporation
#
# See "LICENSE" for license information.

import argparse

from test_cases import test_fetch_all, test_data_types
from test_strategy.base_strategy import BaseStrategy
from test_strategy.execution_details import ExecutionDetails, ConnectionDetails, TestDetails
from test_strategy.pyodbc_strategy import PyOdbcStrategy
from test_strategy.turbodbc_strategy import TurbodbcStrategy


def start_new_test_process(
        test_case: str,
        strategy: BaseStrategy) -> None:
    test_case_all_lower: str = test_case.lower()
    if test_case_all_lower == 'test-fetch-all':
        test_fetch_all.run(
            underlying_test_name=test_case,
            strategy=strategy
        )
    elif 'test-sql-type-' in test_case_all_lower:
        type_name: str = test_case_all_lower.split('-')[-1]  # Get type name in the end of the test case name
        test_data_types.run(
            type_name=type_name,
            underlying_test_name=test_case,
            strategy=strategy)
    else:
        raise_for_invalid_value('test_case', "'test-fetch-all', 'test-sql-type-*type_name*")


def raise_for_invalid_value(key_for_wrong_value: str, valid_options: str) -> ValueError:
    raise ValueError(f'Received an invalid value for "{key_for_wrong_value}". Valid options are: "{valid_options}"')


def parse_bool(input_string: str) -> bool:
    return input_string and input_string.lower().strip() == 'true'


def run(execution_details: ExecutionDetails) -> None:
    test_details: TestDetails = execution_details.test_details

    if test_details.odbc_library.lower() == 'pyodbc':
        start_new_test_process(
            test_details.test_case,
            PyOdbcStrategy(execution_details=execution_details)
        )
    elif test_details.odbc_library.lower() == 'turbodbc':
        start_new_test_process(
            test_details.test_case,
            TurbodbcStrategy(execution_details=execution_details)
        )
    else:
        raise_for_invalid_value('odbc-library', "'pyodbc', 'turbodbc'")


if __name__ == '__main__':
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description='Create test scenarios for profiling in other tools.'
    )

    # required
    parser.add_argument('driver', help='The ODBC Driver Path')
    parser.add_argument('host', help='The host to connect')
    parser.add_argument('port', help='The port to connect')
    parser.add_argument('user', help='The user to authenticate')
    parser.add_argument('password', help='The password to authenticate')
    parser.add_argument('odbc_library', help='Which ODBC Library to use ["pyodbc"|"turbodbc"]')
    parser.add_argument('test_case', help='Which test case to run ["test-fetch-all"|"test-sql-type-*type_name*"]')

    # optional
    parser.add_argument('--sql_query', default="", help='The SQL Query to run (only affects "test-fetch-all")')
    parser.add_argument('--dsn', default="", help="The ODBC Driver Data Source Name")
    parser.add_argument('--use_encryption', default="false", help="Use SSL Connections")

    # TODO: Add the options below to ConnectionDetails and allow custom server-specific flags like StringColumnLength
    # parser.add_argument("--disableCertificateVerification", default="false")
    # parser.add_argument("--trustStore", default=None)
    # parser.add_argument("--trustStorePassword", default=None)
    # parser.add_argument("--useSystemTrustStore", default="true")
    # parser.add_argument("--token", default=None)

    args: dict[str, str] = vars(parser.parse_args())

    connection_detail: ConnectionDetails = ConnectionDetails(
        dsn=args['dsn'],
        driver=args['driver'],
        host=args['host'],
        port=int(args['port']),
        user=args['user'],
        password=args['password'],
        use_encryption=parse_bool(args['use_encryption'])
    )
    test_detail: TestDetails = TestDetails(
        odbc_library=args['odbc_library'],
        sql_query=args['sql_query'],
        test_case=args['test_case']
    )
    execution_detail: ExecutionDetails = ExecutionDetails(
        connection_details=connection_detail,
        test_details=test_detail
    )

    run(execution_details=execution_detail)

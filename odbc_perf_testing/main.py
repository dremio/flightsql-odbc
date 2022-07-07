#!/usr/bin/env python3
#
# Copyright (C) 2020-2022 Dremio Corporation
#
# See "LICENSE" for license information.

import argparse
import json

from test_cases import test_fetch_all, test_data_types
from test_strategy.base_strategy import BaseStrategy
from test_strategy.execution_details import ExecutionDetails, ConnectionDetails, TestDetails
from test_strategy.pyodbc_strategy import PyOdbcStrategy
from test_strategy.turbodbc_strategy import TurbodbcStrategy


def start_new_test_process(
        test_case_name: str,
        strategy: BaseStrategy) -> None:
    test_case_all_lower: str = test_case_name.lower()
    if test_case_all_lower == 'test-fetch-all':
        test_fetch_all.run(
            underlying_test_name=test_case_all_lower,
            strategy=strategy
        )
    elif 'test-sql-type-' in test_case_all_lower:
        type_name: str = test_case_all_lower.split('-')[-1]  # Get type name in the end of the test case name
        test_data_types.run(
            type_name=type_name,
            underlying_test_name=test_case_all_lower,
            strategy=strategy)
    else:
        raise_for_invalid_value('test_case', "'test-fetch-all', 'test-sql-type-*type_name*")


def raise_for_invalid_value(key_for_wrong_value: str, valid_options: str) -> ValueError:
    raise ValueError(f'Received an invalid value for "{key_for_wrong_value}". Valid options are: "{valid_options}"')


def parse_bool(input_string: str) -> bool:
    return input_string and input_string.lower().strip() in ('true', '1')


def run(execution_details: ExecutionDetails) -> None:
    test_details: TestDetails = execution_details.test_details

    odbc_library_lower = test_details.odbc_library.strip().lower()
    if odbc_library_lower == 'pyodbc':
        start_new_test_process(
            test_case_name=f'{odbc_library_lower}-{test_details.test_case}',
            strategy=PyOdbcStrategy(execution_details=execution_details)
        )
    elif odbc_library_lower == 'turbodbc':
        start_new_test_process(
            test_case_name=f'{odbc_library_lower}-{test_details.test_case}',
            strategy=TurbodbcStrategy(execution_details=execution_details)
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
    parser.add_argument(
        '--library_options', default="{}", help="Extra library-specific connection options in JSON format '{K: v}'")
    parser.add_argument(
        "--disable_certificate_verification", default="false",
        help="Tells the driver to ignore certificate verification.")
    parser.add_argument("--trusted_certs", default="", help="Defines the certificates path")
    parser.add_argument("--trust_store", default="", help="Defines the Trust Store path")
    parser.add_argument("--trust_store_password", default="", help="Defines the Trust Store password")
    parser.add_argument(
        "--use_system_trust_store", default="true", help="Tells whether the driver should use the system's Trust Store")
    parser.add_argument("--token", default="", help="Defines the token for Token Authentication")

    args: dict[str, str] = vars(parser.parse_args())

    connection_detail: ConnectionDetails = ConnectionDetails(
        dsn=args['dsn'],
        driver=args['driver'],
        host=args['host'],
        port=int(args['port']),
        user=args['user'],
        password=args['password'],
        use_encryption=parse_bool(args['use_encryption']),
        library_options=json.loads(args['library_options']),
        disable_certificate_verification=parse_bool(args['disable_certificate_verification']),
        trust_store=args['trust_store'],
        trusted_certs=args['trusted_certs'],
        trust_store_password=args['trust_store_password'],
        use_system_trust_store=parse_bool(args['use_system_trust_store']),
        token=args['token'],
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

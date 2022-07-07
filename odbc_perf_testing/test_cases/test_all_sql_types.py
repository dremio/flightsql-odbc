from typing import Callable

from test_cases import test_fetch_all
from test_strategy.executable_strategy import ExecutableStrategy
from test_strategy.execution_details import ExecutionDetails

# TODO: Change to proper CTAS when GandivaOnly issue is fixed
CTAS_DATA_TYPES_QUERIES = {
    'BOOLEAN': 'SELECT * FROM "postgres"."arp_pushdown"."dremio_boolean"',
    'FLOAT': 'SELECT * FROM "postgres"."arp_pushdown"."dremio_float"',
    'DOUBLE': 'SELECT * FROM "postgres"."arp_pushdown"."dremio_double"',
    'INT': 'SELECT * FROM "postgres"."arp_pushdown"."dremio_integer"',
    'BIGINT': 'SELECT * FROM "postgres"."arp_pushdown"."dremio_bigint"',
    'DATE': 'SELECT * FROM "postgres"."arp_pushdown"."dremio_date"',
    'TIME': 'SELECT * FROM "postgres"."arp_pushdown"."dremio_time"',
    'TIMESTAMP': 'SELECT * FROM "postgres"."arp_pushdown"."dremio_timestamp"',
    'VARCHAR': 'SELECT * FROM "postgres"."arp_pushdown"."dremio_varchar"',
}


def run_all(
        process_name: str, create_executable: Callable[[], ExecutableStrategy], execution_details: ExecutionDetails):
    for key, val in CTAS_DATA_TYPES_QUERIES.items():
        execution_details.test_details.sql_query = val
        test_fetch_all.run(
            process_name=f'{process_name}-{key.lower()}',
            create_executable=create_executable)


def run_for_type(
        type_name: str, process_name: str,
        create_executable: Callable[[], ExecutableStrategy], execution_details: ExecutionDetails):
    type_name_upper: str = type_name.strip().upper()
    if type_name_upper in CTAS_DATA_TYPES_QUERIES:
        execution_details.test_details.sql_query = CTAS_DATA_TYPES_QUERIES.get(type_name_upper)
        test_fetch_all.run(process_name=process_name, create_executable=create_executable)
    else:
        raise AttributeError(f'Please select a valid data type for this test. '
                             f'Expected one of: {CTAS_DATA_TYPES_QUERIES.keys()} '
                             f'But got: {type_name}')
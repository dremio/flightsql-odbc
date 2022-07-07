from typing import Callable

from test_cases import test_fetch_all
from test_strategy.executable_strategy import ExecutableStrategy
from test_strategy.execution_details import ExecutionDetails

CTAS_DATA_TYPES_QUERIES = {
    'BOOLEAN': 'SELECT * FROM "postgres"."arp_pushdown"."dremio_boolean"',
    'DATE': 'SELECT * FROM "postgres"."arp_pushdown"."dremio_date"',
    'FLOAT': 'SELECT * FROM "postgres"."arp_pushdown"."dremio_float"',
    'DOUBLE': 'SELECT * FROM "postgres"."arp_pushdown"."dremio_double"',
    'INT': 'SELECT * FROM "postgres"."arp_pushdown"."dremio_integer"',
    'BIGINT': 'SELECT * FROM "postgres"."arp_pushdown"."dremio_bigint"',
    'TIME': 'SELECT * FROM "postgres"."arp_pushdown"."dremio_time"',
    'TIMESTAMP': 'SELECT * FROM "postgres"."arp_pushdown"."dremio_timestamp"',
    'VARCHAR': 'SELECT * FROM "postgres"."arp_pushdown"."dremio_varchar"',
}


def run(process_name: str, create_executable: Callable[[], ExecutableStrategy], execution_details: ExecutionDetails):
    for key, val in CTAS_DATA_TYPES_QUERIES.items():
        execution_details.test_details.sql_query = val
        test_fetch_all.run(
            process_name=f'{process_name}-{key.lower()}',
            create_executable=create_executable)

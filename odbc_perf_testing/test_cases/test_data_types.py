#
# Copyright (C) 2020-2022 Dremio Corporation
#
# See "LICENSE" for license information.

from test_cases import test_fetch_all
from test_strategy.base_strategy import BaseStrategy

SCHEMA: str = 'nas'
ALL_DATA_TYPES_TABLE: str = '"data_1000000_rows.parquet"'

DATA_TYPE_QUERIES: dict[str, str] = {
    'BOOLEAN': f'SELECT booleancol FROM {SCHEMA}.{ALL_DATA_TYPES_TABLE}',
    'FLOAT': f'SELECT floatcol FROM {SCHEMA}.{ALL_DATA_TYPES_TABLE}',
    'DOUBLE': f'SELECT doublecol FROM {SCHEMA}.{ALL_DATA_TYPES_TABLE}',
    'INT': f'SELECT intcol FROM {SCHEMA}.{ALL_DATA_TYPES_TABLE}',
    'BIGINT': f'SELECT bigintcol FROM {SCHEMA}.{ALL_DATA_TYPES_TABLE}',
    'DATE': f'SELECT datecol FROM {SCHEMA}.{ALL_DATA_TYPES_TABLE}',
    'TIME': f'SELECT timecol FROM {SCHEMA}.{ALL_DATA_TYPES_TABLE}',
    'TIMESTAMP': f'SELECT timestampcol FROM {SCHEMA}.{ALL_DATA_TYPES_TABLE}',
    'VARCHAR': f'SELECT varcharcol FROM {SCHEMA}.{ALL_DATA_TYPES_TABLE}',
    'ALL': f'SELECT * FROM {SCHEMA}.{ALL_DATA_TYPES_TABLE}',
}


def run(type_name: str, underlying_test_name: str, strategy: BaseStrategy) -> None:
    type_name_upper: str = type_name.strip().upper()
    if type_name_upper in DATA_TYPE_QUERIES:
        strategy.set_sql_query(DATA_TYPE_QUERIES.get(type_name_upper))
        test_fetch_all.run(underlying_test_name=underlying_test_name, strategy=strategy)
    else:
        raise AttributeError(f'Please select a valid data type for this test. '
                             f'Expected one of: {DATA_TYPE_QUERIES.keys()} '
                             f'But instead got: {type_name}')

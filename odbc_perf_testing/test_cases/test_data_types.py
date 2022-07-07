#
# Copyright (C) 2020-2022 Dremio Corporation
#
# See "LICENSE" for license information.

from typing import Dict

from test_cases import test_fetch_all
from test_strategy.base_strategy import BaseStrategy

SCHEMA: str = 'nas'
TABLE: str = '"data_1000000_rows.parquet"'

DATA_TYPE_QUERIES: Dict[str, str] = {
    'BOOLEAN': f'SELECT booleancol FROM {SCHEMA}.{TABLE}',
    'FLOAT': f'SELECT floatcol FROM {SCHEMA}.{TABLE}',
    'DOUBLE': f'SELECT doublecol FROM {SCHEMA}.{TABLE}',
    'DECIMAL': f'SELECT decimalcol FROM {SCHEMA}.{TABLE}',
    'INT': f'SELECT intcol FROM {SCHEMA}.{TABLE}',
    'BIGINT': f'SELECT bigintcol FROM {SCHEMA}.{TABLE}',
    'DATE': f'SELECT datecol FROM {SCHEMA}.{TABLE}',
    'TIME': f'SELECT timecol FROM {SCHEMA}.{TABLE}',
    'TIMESTAMP': f'SELECT timestampcol FROM {SCHEMA}.{TABLE}',
    'INTERVALDAY': f'SELECT interval_day_to_secondscol FROM {SCHEMA}.{TABLE}',
    'INTERVALYEAR': f'SELECT interval_year_to_monthscol FROM {SCHEMA}.{TABLE}',
    'VARCHAR': f'SELECT varcharcol FROM {SCHEMA}.{TABLE}',
    'STRUCT': f'SELECT structcol FROM {SCHEMA}.{TABLE}',
    'LIST': f'SELECT listcol FROM {SCHEMA}.{TABLE}',
    'ALL': f'SELECT * FROM {SCHEMA}.{TABLE}',
}


def run(type_name: str, test_name: str, strategy: BaseStrategy) -> None:
    type_name_upper: str = type_name.strip().upper()
    if type_name_upper in DATA_TYPE_QUERIES:
        strategy.set_sql_query(DATA_TYPE_QUERIES.get(type_name_upper))
        test_fetch_all.run(test_name=test_name, strategy=strategy)
    else:
        raise AttributeError(
            f'Please select a valid data type for this test. '
            f'Expected one of: {DATA_TYPE_QUERIES.keys()} '
            f'But instead got: {type_name}')

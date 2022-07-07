#
# Copyright (C) 2020-2022 Dremio Corporation
#
# See "LICENSE" for license information.

from time import perf_counter

from test_strategy.base_strategy import BaseStrategy


def run(underlying_test_name: str, strategy: BaseStrategy):
    print(f'{underlying_test_name} starting...')
    start: float = perf_counter()
    strategy.fetch_all()
    end: float = perf_counter()
    print(f'{underlying_test_name} finished in {end - start:.2f}s.')

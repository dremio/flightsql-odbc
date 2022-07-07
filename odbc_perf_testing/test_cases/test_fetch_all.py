#
# Copyright (C) 2020-2022 Dremio Corporation
#
# See "LICENSE" for license information.

from time import perf_counter

from test_strategy.base_strategy import BaseStrategy


def run(test_name: str, strategy: BaseStrategy) -> None:
    print(f'{test_name} starting...')
    start: float = perf_counter()
    strategy.fetch_all()
    end: float = perf_counter()
    print(f'{test_name} finished in {end - start:.2f}s.')

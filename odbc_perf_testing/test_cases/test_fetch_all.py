#
# Copyright (C) 2020-2022 Dremio Corporation
#
# See "LICENSE" for license information.

from test_strategy.base_strategy import BaseStrategy


def run(underlying_test_name: str, strategy: BaseStrategy):
    print(f'{underlying_test_name} starting...')
    strategy.fetch_all()
    print(f'{underlying_test_name} finished.')

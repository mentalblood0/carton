import itertools
import sqlite3

import pytest
import pytest_benchmark.plugin

from .Log import Log, Package


@pytest.fixture
def log():
    cursor = sqlite3.connect(":memory:").cursor()
    return Log(lambda s: cursor.execute(s).fetchall())


@pytest.mark.parametrize("amount", [10**n for n in range(6)])
@pytest.mark.parametrize("selected_key_values_amount", [10**n for n in range(2)])
@pytest.mark.parametrize("selected_items_amount", [10**n for n in range(2)])
def test_benchmark_simple(
    log: Log,
    benchmark: pytest_benchmark.plugin.BenchmarkFixture,
    amount: int,
    selected_key_values_amount: int,
    selected_items_amount: int,
):
    log.insert(
        itertools.chain.from_iterable(
            Package(
                i,
                {"key": f"value_{i}"}
                | {"file": f"path_{i}_{j}" for j in range(selected_key_values_amount)},
            ).entries
            for i in range(amount)
            for _ in range(selected_items_amount)
        )
    )

    benchmark.pedantic(
        lambda: log.select({"key": f"value_{amount}"}, {}, {"file"}),
        iterations=1,
    )

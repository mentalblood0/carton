import itertools
import sqlite3

import pytest
import pytest_benchmark.plugin

from .Log import Log


@pytest.fixture
def log():
    cursor = sqlite3.connect(":memory:").cursor()
    return Log(lambda s: cursor.execute(s).fetchall())


@pytest.mark.parametrize("amount", [10**n for n in range(6)])
def test_benchmark_simple(
    log: Log,
    benchmark: pytest_benchmark.plugin.BenchmarkFixture,
    amount: int,
):
    log.insert(
        itertools.chain.from_iterable(
            Log.entries(
                i,
                {"key": f"value_{i}", "a": "b", "file": f"path_{i}"},
            )
            for i in range(amount)
        )
    )

    def select():
        for p in log.select({"key": f"value_{amount-1}"}, {}, {"file", "a"}):
            assert "id" in p
            assert "file" in p
            assert "a" in p

    benchmark.pedantic(
        select,
        iterations=1,
    )

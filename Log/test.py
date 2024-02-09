import itertools
import sqlite3

import pytest_benchmark.plugin

from .Log import Log, Package


def test_benchmark_simple(benchmark: pytest_benchmark.plugin.BenchmarkFixture):
    cursor = sqlite3.connect(":memory:").cursor()
    log = Log(lambda s: cursor.execute(s).fetchall())

    amount = 10000
    log.insert(
        itertools.chain.from_iterable(
            Package(i, {"key": f"value_{i}", "file": f"path_{i}"}).entries
            for i in range(amount)
        )
    )

    benchmark.pedantic(
        lambda: log.select({"key": f"value_{amount}"}, {}, {"file"}),
        iterations=1,
    )

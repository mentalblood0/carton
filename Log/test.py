import itertools
import sqlite3

import pytest
import pytest_benchmark.plugin

from .Log import Log


@pytest.fixture
def log():
    connection = sqlite3.connect(":memory:")
    cursor = connection.cursor()
    cursor_for_enum = connection.cursor()
    return Log(lambda s: cursor.execute(s), lambda s: cursor_for_enum.execute(s))


@pytest.mark.parametrize("amount", [10**n for n in range(5)])
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
    for row in log.execute("select * from keys"):
        print(row)
    for row in log.execute("select * from log order by i desc limit 10"):
        print(row)

    def select():
        result = False
        for p in log.select({"key": f"value_{amount-1}"}, {}, {"file", "a"}):
            result = True
            assert "id" in p
            assert type(p["id"]) == int
            assert "file" in p
            assert type(p["file"]) == str
            assert "a" in p
            assert type(p["a"]) == str
        assert result

    benchmark.pedantic(
        select,
        iterations=1,
    )


def test_groupby(log: Log):
    log.insert(Log.entries(0, {"a": "b", "x": "y"}))
    log.insert(Log.entries(1, {"c": "d", "x": "y"}))
    log.insert(Log.entries(0, {"e": "f", "x": "y"}))
    log.insert(Log.entries(1, {"g": "h", "x": "y"}))
    result = list(log.select({"x": "y"}))
    assert {"id": 0, "a": "b", "e": "f", "x": "y"} in result
    assert {"id": 1, "c": "d", "g": "h", "x": "y"} in result

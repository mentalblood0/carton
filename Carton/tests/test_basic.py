import sqlite3

import pytest
import pytest_benchmark.plugin

from ..Carton import Carton


@pytest.fixture()
def log():
    connection = sqlite3.connect(":memory:")
    cursor = connection.cursor()
    cursor_for_enum = connection.cursor()
    return Carton(
        execute=cursor.execute,
        executemany=cursor.executemany,
        execute_enum=cursor_for_enum.execute,
    )


@pytest.mark.parametrize("amount", [10**n for n in range(5)])
def test_benchmark_insert(
    log: Carton,
    benchmark: pytest_benchmark.plugin.BenchmarkFixture,
    amount: int,
):
    benchmark.pedantic(
        lambda: log.insert(
            (None if i % 2 else i, {"key": f"value_{i}", "a": "b", "file": f"path_{i}"}) for i in range(amount)
        ),
        iterations=1,
    )


@pytest.mark.parametrize("amount", [10**n for n in range(5)])
def test_benchmark_select(
    log: Carton,
    benchmark: pytest_benchmark.plugin.BenchmarkFixture,
    amount: int,
):
    log.insert((None if i % 2 else i, {"key": f"value_{i}", "a": "b", "file": f"path_{i}"}) for i in range(amount))

    benchmark.pedantic(
        lambda: list(log.select({"key": f"value_{amount-1}"}, {}, {"file", "a"})),
        iterations=1,
    )

    result = False
    for p in log.select({"key": f"value_{amount-1}"}, {}, {"file", "a"}):
        result = True
        assert "package" in p
        assert isinstance(p["package"], int)
        assert "file" in p
        assert isinstance(p["file"], str)
        assert "a" in p
        assert isinstance(p["a"], str)
    assert result


def test_groupby(log: Carton):
    log.insert([(None, {"a": "b", "x": "y"})])
    log.insert([(None, {"c": "d", "x": "y"})])
    log.insert([(0, {"e": "f", "x": "y"})])
    log.insert([(1, {"g": "h", "x": "y"})])
    result = list(log.select({"x": "y"}))
    assert {"package": 0, "a": "b", "e": "f", "x": "y"} in result
    assert {"package": 1, "c": "d", "g": "h", "x": "y"} in result
    assert list(log.select({"x": "y"})) == list(log.select({"x": "y"}, get={"package", "a", "e", "c", "g", "x"}))


def test_distinct(log: Carton):
    log.insert([(None, {"a": "b", "x": "y"})])
    log.insert([(0, {"a": "c", "x": "y"})])
    assert next(log.select({"x": "y"}))["a"] == "c"

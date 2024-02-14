import sqlite3

import pytest
import pytest_benchmark.plugin

from ..Carton import Carton


@pytest.fixture()
def carton():
    connection = sqlite3.connect(":memory:")
    return Carton(
        execute=lambda: connection.cursor().execute,
        executemany=lambda: connection.cursor().executemany,
    )


@pytest.mark.parametrize("amount", [10**n for n in range(5)])
def test_benchmark_insert(
    carton: Carton,
    benchmark: pytest_benchmark.plugin.BenchmarkFixture,
    amount: int,
):
    benchmark.pedantic(
        lambda: carton.insert(
            (None if i % 2 else i, {"key": f"value_{i}", "a": "b", "file": f"path_{i}"}) for i in range(amount)
        ),
        iterations=1,
    )


@pytest.mark.parametrize("amount", [10**n for n in range(5)])
def test_benchmark_select(
    carton: Carton,
    benchmark: pytest_benchmark.plugin.BenchmarkFixture,
    amount: int,
):
    carton.insert((None if i % 2 else i, {"key": f"value_{i}", "a": "b", "file": f"path_{i}"}) for i in range(amount))

    benchmark.pedantic(
        lambda: list(carton.select({"key": f"value_{amount-1}"}, {}, {"file", "a"})),
        iterations=1,
    )

    result = False
    for p in carton.select({"key": f"value_{amount-1}"}, {}, {"file", "a"}):
        result = True
        assert "package" in p
        assert isinstance(p["package"], int)
        assert "file" in p
        assert isinstance(p["file"], str)
        assert "a" in p
        assert isinstance(p["a"], str)
    assert result


def test_groupby(carton: Carton):
    carton.insert([(None, {"a": "b", "x": "y"})])
    carton.insert([(None, {"c": "d", "x": "y"})])
    carton.insert([(0, {"e": "f", "x": "y"})])
    carton.insert([(1, {"g": "h", "x": "y"})])
    result = list(carton.select({"x": "y"}))
    assert {"package": 0, "a": "b", "e": "f", "x": "y"} in result
    assert {"package": 1, "c": "d", "g": "h", "x": "y"} in result
    assert list(carton.select({"x": "y"})) == list(carton.select({"x": "y"}, get={"package", "a", "e", "c", "g", "x"}))


def test_distinct(carton: Carton):
    carton.insert([(None, {"a": "b", "x": "y"})])
    carton.insert([(0, {"a": "c", "x": "y"})])
    assert next(carton.select({"x": "y"}))["a"] == "c"

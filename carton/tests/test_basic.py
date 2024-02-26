import random
import sqlite3

import pytest
import pytest_benchmark.plugin

from ..Carton import Carton


@pytest.fixture()
def carton():
    connection = sqlite3.connect(":memory:")
    return Carton(execute=lambda: connection.cursor().execute, executemany=lambda: connection.cursor().executemany)


@pytest.mark.parametrize("amount", [10**n for n in range(5)])
def test_benchmark_insert(carton: Carton, benchmark: pytest_benchmark.plugin.BenchmarkFixture, amount: int):
    benchmark.pedantic(
        lambda: carton.insert(
            (None if i % 2 else i, {"key": f"value_{i}", "a": "b", "file": f"path_{i}"}) for i in range(amount)
        ),
        iterations=1,
    )


@pytest.mark.parametrize("amount", [10**n for n in range(6)])
def test_benchmark_select(
    carton: Carton, benchmark: pytest_benchmark.plugin.BenchmarkFixture, amount: int, result_amount: int = 1
):
    carton.insert(
        (None, {"key": f"value_{i}", "a": "b", "file": f"path_{i}"})
        for i in range(amount)
        for _ in range(result_amount)
    )

    benchmark(lambda: list(carton.select({"key": f"value_{random.randrange(0, amount) - 1}"}, {"file", "a"})))

    result = False
    for p in carton.select({"key": f"value_{amount-1}"}, {"file", "a"}):
        result = True
        assert "package" in p
        assert isinstance(p["package"], int)
        assert "file" in p
        assert isinstance(p["file"], str)
        assert "a" in p
        assert isinstance(p["a"], str)
    assert result


def test_present(carton: Carton):
    carton.insert([(0, {"a": "b", "x": "y"})])
    carton.insert([(1, {"a": "b", "x": "z"})])
    assert list(carton.select(present={"a": "b", "x": "y"})) == [{"a": "b", "x": "y", "package": 0}]
    assert list(carton.select(present={"a": "b", "x": "z"})) == [{"a": "b", "x": "z", "package": 1}]
    assert list(carton.select(present={"x": "y"})) == [{"a": "b", "x": "y", "package": 0}]
    assert list(carton.select(present={"x": "z"})) == [{"a": "b", "x": "z", "package": 1}]


def test_groupby(carton: Carton):
    carton.insert([(None, {"a": "b", "x": "y"})])
    carton.insert([(None, {"c": "d", "x": "y"})])
    carton.insert([(0, {"e": "f", "x": "y"})])
    carton.insert([(1, {"g": "h", "x": "y"})])
    result = list(carton.select({"x": "y"}))
    assert {"package": 0, "a": "b", "e": "f", "x": "y"} in result
    assert {"package": 1, "c": "d", "g": "h", "x": "y"} in result
    assert list(carton.select({"x": True})) == list(carton.select({"x": "y"}, get={"package", "a", "e", "c", "g", "x"}))


def test_distinct(carton: Carton):
    carton.insert([(None, {"a": "b", "x": "y"})])
    carton.insert([(0, {"a": "c", "x": "y"})])
    assert list(carton.select({"x": "y"})) == [{"a": "c", "x": "y", "package": 0}]


def test_exclude(carton: Carton):
    carton.insert([(None, {"a": "b"})])
    assert not [*carton.select({"a": "b"}, {"a"}, {0})]


def test_insert_null(carton: Carton):
    carton.insert([(0, {"a": None})])
    assert list(carton.select({"a": None})) == [{"package": 0, "a": None}]


def test_new(carton: Carton):
    carton.insert([(0, {"a": "b"})])
    carton.insert([(0, {"a": "c"})])
    assert not list(carton.select({"a": "b"}))


@pytest.mark.parametrize("amount", [10**n for n in range(6)])
def test_benchmark_select_new(carton: Carton, benchmark: pytest_benchmark.plugin.BenchmarkFixture, amount: int):
    carton.insert([(0, {"a": f"a_{i}"}) for i in range(amount)])
    benchmark(lambda: list(carton.select({"a": f"a_{amount-1}"})))

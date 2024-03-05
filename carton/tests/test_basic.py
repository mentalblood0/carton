import random

import pytest
import pytest_benchmark.plugin

from ..Carton import Carton
from .common import *


@pytest.mark.parametrize("amount", [10**n for n in range(5)])
def test_benchmark_insert(carton: Carton, benchmark: pytest_benchmark.plugin.BenchmarkFixture, amount: int):
    benchmark.pedantic(
        lambda: carton.insert((None, {"key": f"value_{i}", "a": "b", "file": f"path_{i}"}) for i in range(amount)),
        iterations=1,
    )


def test_present(carton: Carton):
    carton.insert([(0, {"a": "b", "x": "y"})])
    carton.insert([(1, {"a": "b", "x": "z"})])
    assert list(carton.select("x", "y")) == [{"a": "b", "x": "y", "package": 0}]
    assert list(carton.select("x", "z")) == [{"a": "b", "x": "z", "package": 1}]


def test_groupby(carton: Carton):
    carton.insert([(None, {"a": "b", "x": "y"})])
    carton.insert([(None, {"c": "d", "x": "y"})])
    carton.insert([(0, {"e": "f", "x": "y"})])
    carton.insert([(1, {"g": "h", "x": "y"})])
    result = list(carton.select("x", "y"))
    assert {"package": 0, "a": "b", "e": "f", "x": "y"} in result
    assert {"package": 1, "c": "d", "g": "h", "x": "y"} in result
    assert list(carton.select("x", True)) == list(carton.select("x", "y"))


def test_distinct(carton: Carton):
    carton.insert([(None, {"a": "b", "x": "y"})])
    carton.insert([(0, {"a": "c", "x": "y"})])
    assert list(carton.select("x", "y")) == [{"a": "c", "x": "y", "package": 0}]


def test_insert_null(carton: Carton):
    carton.insert([(0, {"a": None})])
    assert list(carton.select("a", None)) == [{"package": 0, "a": None}]


def test_new(carton: Carton):
    carton.insert([(0, {"a": "b"})])
    carton.insert([(0, {"a": "c"})])
    assert not list(carton.select("a", "b"))


@pytest.mark.parametrize("amount", [10**n for n in range(6)])
def test_benchmark_select_from_many_old(
    carton: Carton, benchmark: pytest_benchmark.plugin.BenchmarkFixture, amount: int
):
    old = "1"
    new = "2"
    carton.insert([(i, {"a": old}) for i in range(amount)])
    carton.insert([(i, {"a": new}) for i in range(amount)])
    carton.insert([(amount, {"a": old})])
    benchmark(lambda: list(carton.select("a", old)))
    assert list(carton.select("a", old)) == [{"package": amount, "a": old}]


@pytest.mark.parametrize("amount", [10**n for n in range(6)])
def test_benchmark_select_from_many_same_key(
    carton: Carton, benchmark: pytest_benchmark.plugin.BenchmarkFixture, amount: int
):
    carton.insert((i, {"a": str(i)}) for i in range(amount))
    benchmark(lambda: list(carton.select("a", str(amount - 1))))
    assert list(carton.select("a", str(amount - 1))) == [{"package": amount - 1, "a": str(amount - 1)}]


@pytest.mark.parametrize("amount", [10**n for n in range(6)])
def test_benchmark_select_from_many_same_value(
    carton: Carton, benchmark: pytest_benchmark.plugin.BenchmarkFixture, amount: int
):
    carton.insert((i, {str(i): "a"}) for i in range(amount))
    benchmark(lambda: list(carton.select(str(amount - 1), "a")))
    assert list(carton.select(str(amount - 1), "a")) == [{"package": amount - 1, str(amount - 1): "a"}]

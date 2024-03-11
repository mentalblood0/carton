import pytest
import pytest_benchmark.plugin

from ..Carton import Carton
from ..Subject import Subject
from .common import *


@pytest.mark.parametrize("amount", [10**n for n in range(6)])
def test_benchmark_insert(carton: Carton, benchmark: pytest_benchmark.plugin.BenchmarkFixture, amount: int):
    carton.insert(Subject(i, {"key": f"value_{i}", "a": "b", "file": f"path_{i}"}) for i in range(amount))
    benchmark.pedantic(
        lambda: carton.insert([Subject(None, {"key": f"value_{amount}", "a": "b", "file": f"path_{amount}"})]),
        iterations=1,
    )


def test_insert_simultaneously(carton: Carton):
    carton.insert(Subject(create={"key": "value"}) for _ in range(2))
    assert len(list(carton.select("key", "value"))) == 2


def test_present(carton: Carton):
    carton.insert([Subject(0, {"a": "b", "x": "y"})])
    carton.insert([Subject(1, {"a": "b", "x": "z"})])
    l = list(carton.select("x", "y"))
    assert len(l) == 1
    assert l[0]["a"] == "b"
    assert l[0]["x"] == "y"
    l = list(carton.select("x", "z"))
    assert len(l) == 1
    assert l[0]["a"] == "b"
    assert l[0]["x"] == "z"


def test_distinct(carton: Carton):
    carton.insert([Subject(0, {"a": "b", "x": "y"})])
    s = list(carton.select("a", "b"))[0]
    s["a"] = "c"
    s["x"] = "y"
    carton.insert([s])
    l = list(carton.select("x", "y"))
    assert len(l) == 1
    assert l[0].id == 0
    assert l[0]["a"] == "c"
    assert l[0]["x"] == "y"


def test_insert_null(carton: Carton):
    carton.insert([Subject(0, {"a": None})])
    l = list(carton.select("a", None))
    assert len(l) == 1
    assert l[0].id == 0
    assert l[0]["a"] is None


def test_new(carton: Carton):
    carton.insert([Subject(0, {"a": "b"})])
    s = list(carton.select("a", "b"))[0]
    s["a"] = "c"
    carton.insert([s])
    assert not list(carton.select("a", "b"))


@pytest.mark.parametrize("amount", [10**n for n in range(5)])
def test_benchmark_select_from_many_same_key(
    carton: Carton, benchmark: pytest_benchmark.plugin.BenchmarkFixture, amount: int
):
    carton.insert(Subject(i, {"a": str(i)}) for i in range(amount))
    benchmark.pedantic(lambda: list(carton.select("a", str(amount - 1))), iterations=1)
    l = list(carton.select("a", str(amount - 1)))
    assert len(l) == 1
    assert l[0].id == amount - 1
    assert l[0]["a"] == str(amount - 1)


@pytest.mark.parametrize("amount", [10**n for n in range(5)])
def test_benchmark_select_from_many_same_value(
    carton: Carton, benchmark: pytest_benchmark.plugin.BenchmarkFixture, amount: int
):
    carton.insert(Subject(i, {str(i): "a"}) for i in range(amount))
    benchmark.pedantic(lambda: list(carton.select(str(amount - 1), "a")), iterations=1)
    l = list(carton.select(str(amount - 1), "a"))
    assert len(l) == 1
    assert l[0].id == amount - 1
    assert l[0][str(amount - 1)] == "a"

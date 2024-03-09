import uuid

import pytest
import pytest_benchmark.plugin

from ..Carton import Carton
from .common import *


@pytest.mark.parametrize("amount", [10**n for n in range(5)])
def test_benchmark_insert(carton: Carton, benchmark: pytest_benchmark.plugin.BenchmarkFixture, amount: int):
    carton.insert((None, {"key": f"value_{i}", "a": "b", "file": f"path_{i}"}) for i in range(amount))
    benchmark.pedantic(
        lambda: carton.insert([(None, {"key": f"value_{amount}", "a": "b", "file": f"path_{amount}"})]), iterations=1
    )


def count(carton: Carton):
    return next(carton.db.cursor().execute("select count(distinct subject) from sentences"))[0]


def insert(carton: Carton, start: int, amount: int, batch: int, *, update: bool = True, auto_id: bool = False):
    before = None
    if start + amount <= 10:
        before = count(carton)
    for i in range(amount // batch):
        carton.insert(
            [
                (
                    (start + i * batch + j) if not auto_id else None,
                    {
                        "file": uuid.uuid4().hex,
                        "digest": uuid.uuid4().hex,
                        "schema": None,
                        "sign": None,
                        "type": None,
                        "unique_message_id": None,
                        "unique_message": None,
                        "unique_lifecycle_id": None,
                        "processor_number": None,
                    },
                )
                for j in range(batch)
            ]
        )
        if update:
            carton.insert(
                [
                    (
                        start + i * batch + j,
                        {"schema": "1", "message_id": uuid.uuid4().hex, "lifecycle_id": uuid.uuid4().hex},
                    )
                    for j in range(batch)
                ]
            )
    if before is not None:
        after = count(carton)
        assert after - before == amount


@pytest.mark.parametrize("amount", [10**n for n in range(5)])
def test_benchmark_insert_complex(
    carton: Carton, benchmark: pytest_benchmark.plugin.BenchmarkFixture, amount: int, batch: int = 1
):
    insert(carton, 0, amount, batch)
    benchmark.pedantic(lambda: insert(carton, amount, 1, batch, auto_id=True), iterations=1)


@pytest.mark.parametrize("amount", [10**n for n in range(5)])
def test_benchmark_select_complex(
    carton: Carton, benchmark: pytest_benchmark.plugin.BenchmarkFixture, amount: int, batch: int = 1
):
    insert(carton, 0, amount, batch)
    insert(carton, amount, batch, batch, update=False)

    def select():
        assert len(list(carton.select("schema", None))) == batch

    benchmark.pedantic(select, iterations=1)


def test_present(carton: Carton):
    carton.insert([(0, {"a": "b", "x": "y"})])
    carton.insert([(1, {"a": "b", "x": "z"})])
    assert list(carton.select("x", "y")) == [{"a": "b", "x": "y", "subject": 0}]
    assert list(carton.select("x", "z")) == [{"a": "b", "x": "z", "subject": 1}]


def test_groupby(carton: Carton):
    carton.insert([(None, {"a": "b", "x": "y"})])
    carton.insert([(None, {"c": "d", "x": "y"})])
    carton.insert([(0, {"e": "f", "x": "y"})])
    carton.insert([(1, {"g": "h", "x": "y"})])
    result = list(carton.select("x", "y"))
    assert {"subject": 0, "a": "b", "e": "f", "x": "y"} in result
    assert {"subject": 1, "c": "d", "g": "h", "x": "y"} in result


def test_distinct(carton: Carton):
    carton.insert([(None, {"a": "b", "x": "y"})])
    carton.insert([(0, {"a": "c", "x": "y"})])
    assert list(carton.select("x", "y")) == [{"a": "c", "x": "y", "subject": 0}]


def test_insert_null(carton: Carton):
    carton.insert([(0, {"a": None})])
    assert list(carton.select("a", None)) == [{"subject": 0, "a": None}]


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
    benchmark.pedantic(lambda: list(carton.select("a", old)), iterations=1)
    assert list(carton.select("a", old)) == [{"subject": amount, "a": old}]


@pytest.mark.parametrize("amount", [10**n for n in range(5)])
def test_benchmark_select_from_many_same_key(
    carton: Carton, benchmark: pytest_benchmark.plugin.BenchmarkFixture, amount: int
):
    carton.insert((i, {"a": str(i)}) for i in range(amount))
    benchmark.pedantic(lambda: list(carton.select("a", str(amount - 1))), iterations=1)
    assert list(carton.select("a", str(amount - 1))) == [{"subject": amount - 1, "a": str(amount - 1)}]


@pytest.mark.parametrize("amount", [10**n for n in range(5)])
def test_benchmark_select_from_many_same_value(
    carton: Carton, benchmark: pytest_benchmark.plugin.BenchmarkFixture, amount: int
):
    carton.insert((i, {str(i): "a"}) for i in range(amount))
    benchmark.pedantic(lambda: list(carton.select(str(amount - 1), "a")), iterations=1)
    assert list(carton.select(str(amount - 1), "a")) == [{"subject": amount - 1, str(amount - 1): "a"}]

import random

import pytest
import pytest_benchmark.plugin


@pytest.mark.parametrize("size", [10**n for n in range(6)])
def test_benchmark_input_size(benchmark: pytest_benchmark.plugin.BenchmarkFixture, size: int):
    input = "".join(
        chr(
            random.randrange(ord("A"), ord("Z") + 1)
            if random.randint(0, 1)
            else random.randrange(ord("a"), ord("z") + 1)
        )
        for _ in range(size)
    )
    benchmark(lambda: encrypt(input))

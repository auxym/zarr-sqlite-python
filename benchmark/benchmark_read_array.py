"""Benchmark read performance of SQLiteStore with zarr arrays."""

import os
import tempfile

import pytest
import numpy as np
import zarr
from zarr_sqlite import SQLiteStore


@pytest.fixture
def benchmark_store_file():
    """Create a temporary file-backed store with a large array."""
    fd, path = tempfile.mkstemp(suffix=".zarrdb")
    os.close(fd)

    store = SQLiteStore(path)
    z = zarr.create_array(
        store=store, shape=(4000, 4000), chunks=(500, 500), dtype="f4"
    )
    data = np.random.default_rng().random(z.shape, dtype=np.float32)
    z[:, :] = data
    store.close()

    yield path

    os.remove(path)


def test_benchmark_read_array(benchmark, benchmark_store_file):
    """Benchmark reading a large array from a SQLiteStore.

    Measures only the array read time (``z[:]`` call), excluding store
    opening and array metadata loading.
    """
    store = SQLiteStore(benchmark_store_file, read_only=True, max_connections=10)
    z = zarr.open(store=store, mode="r")

    def read_array():
        return z[:]

    result = benchmark(read_array)
    assert result.shape == (4000, 4000)

    store.close()

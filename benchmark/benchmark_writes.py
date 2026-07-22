import asyncio

import numpy as np
import pytest

from zarr_sqlite import SQLiteStore
from zarr.core.buffer import default_buffer_prototype

BLOB_SIZES = [
    100 * 1024,
    1 * 1024 * 1024,
    10 * 1024 * 1024,
]

PAGE_SIZES = [
    4096,
    16384,
    32768,
    65536,
]

NUM_KEYS = 20
SEED = 42


def _generate_blobs(blob_size, num_keys, seed=SEED):
    rng = np.random.default_rng(seed)
    return [rng.bytes(blob_size) for _ in range(num_keys)]


async def _do_writes(store, buffers):
    for i, buf in enumerate(buffers):
        key = f"key_{i:04d}"
        await store.set(key, buf)


@pytest.mark.parametrize("page_size", PAGE_SIZES)
@pytest.mark.parametrize("blob_size", BLOB_SIZES)
def test_write_benchmark(benchmark, tmp_path, blob_size, page_size):
    db_path = tmp_path / f"bench_{blob_size}_{page_size}.db"
    store = SQLiteStore(db_path, page_size=page_size)

    blobs = _generate_blobs(blob_size, NUM_KEYS)
    prototype = default_buffer_prototype()
    buffers = [prototype.buffer.from_bytes(blob) for blob in blobs]

    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(_do_writes(store, buffers))

        def do_writes():
            loop.run_until_complete(_do_writes(store, buffers))

        benchmark(do_writes)
    finally:
        loop.close()
        store.close()

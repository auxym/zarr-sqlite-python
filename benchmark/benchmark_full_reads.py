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
NUM_READS = 50
SEED = 42


def _generate_blobs(blob_size, num_keys, seed=SEED):
    rng = np.random.default_rng(seed)
    return [rng.bytes(blob_size) for _ in range(num_keys)]


async def _populate(store, blobs):
    prototype = default_buffer_prototype()
    for i, blob in enumerate(blobs):
        key = f"key_{i:04d}"
        buf = prototype.buffer.from_bytes(blob)
        await store.set(key, buf)


async def _do_reads(store, num_reads, num_keys):
    prototype = default_buffer_prototype()
    for i in range(num_reads):
        key = f"key_{i % num_keys:04d}"
        await store.get(key, prototype)


@pytest.mark.parametrize("page_size", PAGE_SIZES)
@pytest.mark.parametrize("blob_size", BLOB_SIZES)
def test_full_read_benchmark(benchmark, tmp_path, blob_size, page_size):
    db_path = tmp_path / f"bench_{blob_size}_{page_size}.db"
    store = SQLiteStore(db_path, page_size=page_size)

    blobs = _generate_blobs(blob_size, NUM_KEYS)

    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(_populate(store, blobs))

        def do_reads():
            loop.run_until_complete(_do_reads(store, NUM_READS, NUM_KEYS))

        benchmark(do_reads)
    finally:
        loop.close()
        store.close()

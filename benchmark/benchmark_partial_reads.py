import asyncio

import numpy as np
import pytest

from zarr_sqlite import SQLiteStore
from zarr.core.buffer import default_buffer_prototype
from zarr.abc.store import RangeByteRequest

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


def _generate_reads(blob_size, num_reads, seed=SEED + 1):
    rng = np.random.default_rng(seed)
    reads = []
    for _ in range(num_reads):
        offset = int(rng.integers(0.5*blob_size, blob_size))
        remaining = blob_size - offset
        log_size = rng.uniform(0, np.log(remaining))
        read_size = max(1, int(np.exp(log_size)))
        end = offset + read_size
        reads.append(RangeByteRequest(start=offset, end=end))
    return reads


async def _populate(store, blobs):
    prototype = default_buffer_prototype()
    for i, blob in enumerate(blobs):
        key = f"key_{i:04d}"
        buf = prototype.buffer.from_bytes(blob)
        await store.set(key, buf)


async def _do_reads(store, reads, num_keys):
    prototype = default_buffer_prototype()
    for i, byte_range in enumerate(reads):
        key = f"key_{i % num_keys:04d}"
        await store.get(key, prototype, byte_range)


@pytest.mark.parametrize("page_size", PAGE_SIZES)
@pytest.mark.parametrize("blob_size", BLOB_SIZES)
def test_partial_read_benchmark(benchmark, tmp_path, blob_size, page_size):
    db_path = tmp_path / f"bench_{blob_size}_{page_size}.db"
    store = SQLiteStore(db_path, page_size=page_size)

    blobs = _generate_blobs(blob_size, NUM_KEYS)
    reads = _generate_reads(blob_size, NUM_READS)

    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(_populate(store, blobs))

        def do_reads():
            loop.run_until_complete(_do_reads(store, reads, NUM_KEYS))

        benchmark(do_reads)
    finally:
        loop.close()
        store.close()

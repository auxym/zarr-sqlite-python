"""Shared helper functions for SQLiteStore tests."""

from zarr.core.buffer import BufferPrototype, default_buffer_prototype

from zarr_sqlite import SQLiteStore


def make_buffer(data: bytes, prototype: BufferPrototype | None = None) -> object:
    """Create a buffer object from raw bytes."""
    prototype = prototype or default_buffer_prototype()
    return prototype.buffer.from_bytes(data)


async def collect(iterator) -> list:
    """Collect async iterator results into a list."""
    return [item async for item in iterator]


async def get_as_bytes(s: SQLiteStore, key: str) -> bytes | None:
    """Retrieve a key's value as raw bytes."""
    buf = await s.get(key, default_buffer_prototype())
    if buf is None:
        return None
    return buf.to_bytes()

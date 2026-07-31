"""Shared fixtures and helpers for SQLiteStore tests."""

import os
from tempfile import NamedTemporaryFile

import pytest
from zarr.core.buffer import BufferPrototype, default_buffer_prototype

from zarr_sqlite import SQLiteStore


@pytest.fixture
def make_buffer():
    """Factory fixture for creating buffer objects from raw bytes."""
    def _make(data: bytes, prototype: BufferPrototype | None = None) -> object:
        prototype = prototype or default_buffer_prototype()
        return prototype.buffer.from_bytes(data)
    return _make


@pytest.fixture
def collect():
    """Fixture for collecting async iterator results into a list."""
    async def _collect(iterator):
        return [item async for item in iterator]
    return _collect


@pytest.fixture
def get_as_bytes():
    """Fixture for retrieving a key's value as raw bytes."""
    async def _get_as_bytes(s: SQLiteStore, key: str) -> bytes | None:
        buf = await s.get(key, default_buffer_prototype())
        if buf is None:
            return None
        return buf.to_bytes()
    return _get_as_bytes


@pytest.fixture
def memstore():
    """An in-memory SQLiteStore."""
    with SQLiteStore(":memory:") as s:
        yield s


@pytest.fixture
def tempstore():
    """A file-backed SQLiteStore using a temporary file."""
    with NamedTemporaryFile(suffix=".zarrdb", delete=True, delete_on_close=False) as fp:
        fp.close()
        with SQLiteStore(fp.name) as s:
            yield s

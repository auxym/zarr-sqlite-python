"""Shared fixtures for SQLiteStore tests."""

from tempfile import NamedTemporaryFile

import pytest

from zarr_sqlite import SQLiteStore


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

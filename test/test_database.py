"""Unit tests for SQLiteStore database configuration and lifecycle."""

from pathlib import Path

import pytest
from zarr.core.buffer import default_buffer_prototype

from zarr_sqlite import SQLiteStore


# ---------------------------------------------------------------------------
# Equality tests
# ---------------------------------------------------------------------------


def test_eq_same_path(tmp_path):
    db = tmp_path / "eq.db"
    s1 = SQLiteStore(db)
    s2 = SQLiteStore(str(db))
    s3 = SQLiteStore(tmp_path / "other.db")
    try:
        assert s1 == s2
        assert s1 != s3
        assert s1 != "not a store"
    finally:
        s1.close()
        s2.close()
        s3.close()


def test_eq_in_memory():
    """In-memory databases are unique, so __eq__ always returns False."""
    s1 = SQLiteStore(":memory:")
    s2 = SQLiteStore(":memory:")
    try:
        assert s1 != s2
        assert s1 != "not a store"
    finally:
        s1.close()
        s2.close()


# ---------------------------------------------------------------------------
# Database string handling tests
# ---------------------------------------------------------------------------


def test_memory_kept_as_is():
    """:memory: is kept as-is, not converted to a URI."""
    s = SQLiteStore(":memory:")
    try:
        assert s.database == ":memory:"
    finally:
        s.close()


def test_non_read_only_keeps_string():
    """Non-read-only stores keep the user's database string unmodified."""
    s = SQLiteStore("foo.db")
    try:
        assert s.database == "foo.db"
    finally:
        s.close()


def test_non_read_only_keeps_path_string():
    """Non-read-only stores convert Path to str but keep the path unmodified."""
    s = SQLiteStore(Path("foo.db"))
    try:
        assert s.database == "foo.db"
    finally:
        s.close()


def test_read_only_converts_to_uri(tmp_path):
    """Read-only stores convert the database to a URI with mode=ro."""
    db = tmp_path / "test.db"
    s = SQLiteStore(db, read_only=True)
    try:
        assert s.database.startswith("file:")
        assert "mode=ro" in s.database
    finally:
        s.close()


def test_with_read_only_in_memory_raises():
    """with_read_only raises for in-memory databases."""
    s = SQLiteStore(":memory:")
    try:
        with pytest.raises(ValueError, match="read-only view"):
            s.with_read_only(read_only=True)
    finally:
        s.close()


# ---------------------------------------------------------------------------
# In-memory isolation tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_in_memory_isolates_data(make_buffer):
    """Two in-memory stores do not share data."""
    s1 = SQLiteStore(":memory:")
    s2 = SQLiteStore(":memory:")
    try:
        await s1.set("key", make_buffer(b"data1"))
        assert await s1.get("key", default_buffer_prototype()) is not None
        assert await s2.get("key", default_buffer_prototype()) is None
    finally:
        s1.close()
        s2.close()


# ---------------------------------------------------------------------------
# Read-only tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_with_read_only(tmpfile_store, make_buffer, get_as_bytes):
    await tmpfile_store.set("a", make_buffer(b"data"))

    ro = tmpfile_store.with_read_only(read_only=True)
    assert ro.read_only is True

    assert await get_as_bytes(ro, "a") == b"data"

    with pytest.raises(ValueError):
        await ro.set("b", make_buffer(b"data"))
    ro.close()


@pytest.mark.asyncio
async def test_read_only_raises(tmpfile_store, make_buffer):
    await tmpfile_store.set("a", make_buffer(b"data"))
    tmpfile_store.close()
    store = SQLiteStore(tmpfile_store.database, read_only=True)

    with pytest.raises(ValueError):
        await store.delete("a")
    with pytest.raises(ValueError):
        await store.set("b", make_buffer(b"data"))
    with pytest.raises(ValueError):
        await store.set_if_not_exists("b", make_buffer(b"data"))
    with pytest.raises(ValueError):
        await store.delete_dir("a/")
    with pytest.raises(ValueError):
        await store.clear()


# ---------------------------------------------------------------------------
# File reuse tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_open_existing_valid_file_writable(
    tmpfile_store, make_buffer, get_as_bytes, collect
):
    """Opening an existing valid file in writable mode should not recreate schema.

    The _db_is_empty() check should skip schema creation when the database
    already contains tables, and _validate_schema() should succeed.
    """
    await tmpfile_store.set("a", make_buffer(b"first"))
    await tmpfile_store.set("b", make_buffer(b"second"))
    tmpfile_store.close()

    # Open a *new* store object pointing to the same file in writable mode.
    store = SQLiteStore(tmpfile_store.database, read_only=False)
    assert await get_as_bytes(store, "a") == b"first"
    assert await get_as_bytes(store, "b") == b"second"

    # New writes must work after opening an existing file.
    await store.set("c", make_buffer(b"third"))
    assert await get_as_bytes(store, "c") == b"third"

    keys = set(await collect(store.list()))
    assert keys == {"a", "b", "c"}

    store.close()


@pytest.mark.asyncio
async def test_reuse_after_close(tmpfile_store, make_buffer, get_as_bytes, collect):
    """A file-based SQLiteStore can be re-used after close().

    The data written before close() must persist, and new operations must
    work after the store is implicitly re-opened.
    """
    # Write data before closing
    await tmpfile_store.set("a", make_buffer(b"first"))
    await tmpfile_store.set("b", make_buffer(b"second"))

    # Close the store
    tmpfile_store.close()

    # Re-use the store: an operation triggers _ensure_open -> _open,
    # which creates a new connection pool and re-creates/validates the schema.
    assert await get_as_bytes(tmpfile_store, "a") == b"first"
    assert await get_as_bytes(tmpfile_store, "b") == b"second"

    # New writes must also work after re-open
    await tmpfile_store.set("c", make_buffer(b"third"))
    assert await get_as_bytes(tmpfile_store, "c") == b"third"

    # Listing must reflect all keys
    keys = set(await collect(tmpfile_store.list()))
    assert keys == {"a", "b", "c"}

    tmpfile_store.close()

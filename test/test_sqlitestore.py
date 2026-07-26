"""Unit tests for the public API of SQLiteStore."""

import datetime
import sqlite3

import pytest
from zarr.core.buffer import BufferPrototype, default_buffer_prototype
from zarr.abc.store import OffsetByteRequest, RangeByteRequest, SuffixByteRequest

from zarr_sqlite import SQLiteStore
from zarr_sqlite.zarr_sqlite import (
    _validate_key,
    _normalize_prefix,
    _SQLITESTORE_SPEC_VERSION,
    _SQLITESTORE_APPLICATION_ID,
)

from tempfile import NamedTemporaryFile


def make_buffer(data: bytes, prototype: BufferPrototype | None = None) -> object:
    prototype = prototype or default_buffer_prototype()
    return prototype.buffer.from_bytes(data)


async def collect(iterator):
    return [item async for item in iterator]


async def get_as_bytes(s: SQLiteStore, key: str) -> bytes | None:
    buf = await s.get(key, default_buffer_prototype())
    if buf is None:
        return None
    return buf.to_bytes()


@pytest.fixture
def store():
    s = SQLiteStore(":memory:")
    yield s
    s.close()


@pytest.fixture
def tmpfile_store():
    fp = NamedTemporaryFile(suffix=".db")
    fp.close
    s = SQLiteStore(fp.name)
    yield s
    s.close()


@pytest.mark.asyncio
async def test_set_and_get(store):
    data = b"hello world"
    await store.set("foo", make_buffer(data))
    buf = await store.get("foo", default_buffer_prototype())
    assert buf.to_bytes() == data


@pytest.mark.asyncio
async def test_get_nonexistent(store):
    buf = await store.get("missing", default_buffer_prototype())
    assert buf is None


@pytest.mark.asyncio
async def test_get_offset_byte_request(store):
    data = b"abcdefghij"
    await store.set("k", make_buffer(data))
    buf = await store.get(
        "k", default_buffer_prototype(), byte_range=OffsetByteRequest(3)
    )
    assert buf.to_bytes() == b"defghij"


@pytest.mark.asyncio
async def test_get_offset_beyond_length(store):
    data = b"abc"
    await store.set("k", make_buffer(data))
    buf = await store.get(
        "k", default_buffer_prototype(), byte_range=OffsetByteRequest(100)
    )
    assert buf.to_bytes() == b""


@pytest.mark.asyncio
async def test_get_range_byte_request(store):
    data = b"abcdefghij"
    await store.set("k", make_buffer(data))
    buf = await store.get(
        "k",
        default_buffer_prototype(),
        byte_range=RangeByteRequest(start=2, end=5),
    )
    assert buf.to_bytes() == b"cde"


@pytest.mark.asyncio
async def test_get_range_clamped(store):
    data = b"abcdefghijkl"
    await store.set("k", make_buffer(data))
    buf = await store.get(
        "k",
        default_buffer_prototype(),
        byte_range=RangeByteRequest(start=-5, end=100),
    )
    assert buf.to_bytes() == data


@pytest.mark.asyncio
async def test_get_suffix_byte_request(store):
    data = b"abcdefghij"
    await store.set("k", make_buffer(data))
    buf = await store.get(
        "k", default_buffer_prototype(), byte_range=SuffixByteRequest(3)
    )
    assert len(buf) == 3
    assert buf.to_bytes() == b"hij"


@pytest.mark.asyncio
async def test_get_suffix_larger_than_length(store):
    data = b"abc"
    await store.set("k", make_buffer(data))
    buf = await store.get(
        "k", default_buffer_prototype(), byte_range=SuffixByteRequest(100)
    )
    assert buf.to_bytes() == data


@pytest.mark.asyncio
async def test_get_non_bytes(store):
    await store._execute_write("INSERT INTO zarr (k, v) VALUES (?, ?)", ("k", 5))
    await store.get("k", default_buffer_prototype()) is None


@pytest.mark.asyncio
async def test_get_unsupported_byte_range(store):
    data = b"abc"
    await store.set("k", make_buffer(data))
    bad = object()
    with pytest.raises(ValueError):
        await store.get("k", default_buffer_prototype(), byte_range=bad)


@pytest.mark.asyncio
async def test_get_partial_values(store):
    await store.set("a", make_buffer(b"0123456789"))
    await store.set("b", make_buffer(b"ABCDEFGHIJ"))
    results = await store.get_partial_values(
        default_buffer_prototype(),
        [
            ("a", None),
            ("b", RangeByteRequest(start=0, end=3)),
            ("missing", None),
        ],
    )
    assert results[0].to_bytes() == b"0123456789"
    assert results[1].to_bytes() == b"ABC"
    assert results[2] is None


@pytest.mark.asyncio
async def test_set_overwrites(store):
    await store.set("k", make_buffer(b"first"))
    buf = await store.get("k", default_buffer_prototype())
    assert buf.to_bytes() == b"first"

    await store.set("k", make_buffer(b"second"))
    buf = await store.get("k", default_buffer_prototype())
    assert buf.to_bytes() == b"second"


@pytest.mark.asyncio
async def test_delete_erases_key(store):
    await store.set("k", make_buffer(b"data"))
    assert await store.exists("k")
    await store.delete("k")
    assert not await store.exists("k")
    assert await store.get("k", default_buffer_prototype()) is None


@pytest.mark.asyncio
async def test_delete_missing_key_is_noop(store):
    await store.delete("never_existed")


@pytest.mark.asyncio
async def test_delete_dir_erases_prefix(store):
    await store.set("a/x", make_buffer(b"1"))
    await store.set("a/y", make_buffer(b"2"))
    await store.set("b/z", make_buffer(b"3"))
    await store.delete_dir("a/")
    assert not await store.exists("a/x")
    assert not await store.exists("a/y")
    assert await store.exists("b/z")


@pytest.mark.asyncio
async def test_delete_dir_raises_when_key_is_leaf(store):
    await store.set("a", make_buffer(b"leaf"))
    with pytest.raises(ValueError):
        await store.delete_dir("a")


@pytest.mark.asyncio
async def test_delete_dir(store):
    await store.set("a/b", make_buffer(b"1"))
    await store.delete_dir("a/")
    assert not await store.exists("a/b")


@pytest.mark.asyncio
async def test_list_returns_all_keys(store):
    await store.set("a", make_buffer(b"1"))
    await store.set("b/c", make_buffer(b"2"))
    await store.set("b/d", make_buffer(b"3"))
    keys = set(await collect(store.list()))
    assert keys == {"a", "b/c", "b/d"}


@pytest.mark.asyncio
async def test_list_empty(store):
    assert await collect(store.list()) == []


@pytest.mark.asyncio
async def test_list_prefix(store):
    await store.set("a/1", make_buffer(b"1"))
    await store.set("a/2", make_buffer(b"2"))
    await store.set("ab/3", make_buffer(b"3"))
    await store.set("b/4", make_buffer(b"4"))
    keys = set(await collect(store.list_prefix("a/")))
    assert keys == {"a/1", "a/2"}


@pytest.mark.asyncio
async def test_list_prefix_root(store):
    await store.set("a/1", make_buffer(b"1"))
    await store.set("b/2", make_buffer(b"2"))
    keys = set(await collect(store.list_prefix("")))
    assert keys == {"a/1", "b/2"}


@pytest.mark.asyncio
async def test_list_prefix_no_match(store):
    await store.set("a/1", make_buffer(b"1"))
    assert await collect(store.list_prefix("zzz/")) == []


@pytest.mark.asyncio
async def test_list_dir_root(store):
    await store.set("a/1", make_buffer(b"1"))
    await store.set("b/2", make_buffer(b"2"))
    await store.set("c/d/3", make_buffer(b"3"))
    await store.set("leaf", make_buffer(b"3"))

    entries = set(await collect(store.list_dir("")))
    assert entries == {"a/", "b/", "c/", "leaf"}


@pytest.mark.asyncio
async def test_list_dir_nested(store):
    await store.set("a/x", make_buffer(b"1"))
    await store.set("a/y", make_buffer(b"2"))
    await store.set("a/sub/z", make_buffer(b"3"))
    entries = set(await collect(store.list_dir("a/")))
    assert entries == {"x", "y", "sub/"}


@pytest.mark.asyncio
async def test_list_dir_no_match(store):
    await store.set("a/1", make_buffer(b"1"))
    assert await collect(store.list_dir("zzz/")) == []


@pytest.mark.asyncio
async def test_list_dir_empty_prefix_yields_nothing(store):
    assert await collect(store.list_dir("nonexistent/")) == []


@pytest.mark.asyncio
async def test_set_if_not_exists(store):
    await store.set_if_not_exists("k", make_buffer(b"first"))
    await store.set_if_not_exists("k", make_buffer(b"second"))
    buf = await store.get("k", default_buffer_prototype())
    assert buf.to_bytes() == b"first"


@pytest.mark.asyncio
async def test_exists(store):
    assert not await store.exists("k")
    await store.set("k", make_buffer(b"data"))
    assert await store.exists("k")


@pytest.mark.asyncio
async def test_is_empty(store):
    assert await store.is_empty("")
    assert await store.is_empty("a/")

    await store.set("a/b", make_buffer(b"1"))
    assert not await store.is_empty("")
    assert not await store.is_empty("a/")
    assert await store.is_empty("c/")


@pytest.mark.asyncio
async def test_clear(store):
    await store.set("a", make_buffer(b"1"))
    await store.set("b/c", make_buffer(b"2"))
    assert len(await collect(store.list())) == 2
    await store.clear()
    assert await collect(store.list()) == []


@pytest.mark.asyncio
async def test_getsize(store):
    await store.set("k", make_buffer(b"12345"))
    assert await store.getsize("k") == 5

    await store.set("empty", make_buffer(b""))
    assert await store.getsize("empty") == 0

    with pytest.raises(FileNotFoundError):
        await store.getsize("missing")


@pytest.mark.asyncio
async def test_getsize_prefix(store):
    await store.set("a/1", make_buffer(b"abc"))
    await store.set("a/2", make_buffer(b"de"))
    await store.set("a/3", make_buffer(b""))
    assert await store.getsize_prefix("a/") == 5

    # non-existent prefix, getsize_prefix should return 0
    assert await store.getsize_prefix("missing/") == 0


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


@pytest.mark.asyncio
async def test_with_read_only(tmpfile_store):
    await tmpfile_store.set("a", make_buffer(b"data"))

    ro = tmpfile_store.with_read_only(read_only=True)
    assert ro.read_only is True

    assert await get_as_bytes(ro, "a") == b"data"

    with pytest.raises(ValueError):
        await ro.set("b", make_buffer(b"data"))


@pytest.mark.asyncio
async def test_read_only_raises(tmpfile_store):
    await tmpfile_store.set("a", make_buffer(b"data"))
    tmpfile_store.close()
    store = SQLiteStore(tmpfile_store.database_uri, read_only=True)

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


def test_validate_key_valid():
    for key in ["", "a", "a/b", "a/b/c.json", "0", "with-dash_and.dot"]:
        _validate_key(key)


def test_validate_key_rejects_leading_slash():
    invalid_keys = ["/a", "a/", "a//b", "a/b/"]
    valid_keys = ["", "a", "a/b", "a/b/c", "foo/bar/baz/qux"]

    for k in invalid_keys:
        with pytest.raises(ValueError):
            _validate_key(k)

    for k in valid_keys:
        # Should not raise
        _validate_key(k)


def test_normalize_prefix_valid_unchanged():
    assert _normalize_prefix("a/") == "a/"
    assert _normalize_prefix("a/b/") == "a/b/"

    assert _normalize_prefix("a") == "a/"
    assert _normalize_prefix("a/b") == "a/b/"

    assert _normalize_prefix("") == ""

    with pytest.raises(ValueError):
        _normalize_prefix("/a")

    with pytest.raises(ValueError):
        _normalize_prefix("a//b")

    with pytest.raises(ValueError):
        _normalize_prefix("/")


# ---------------------------------------------------------------------------
# Schema creation tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_schema_tables_exist(store):
    """Both required tables are created on first use."""
    await store.set("key", make_buffer(b"data"))
    con = store._con
    cur = con.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = {row[0] for row in cur.fetchall()}
    assert "zarr" in tables
    assert "sqlitestore_metadata" in tables


@pytest.mark.asyncio
async def test_schema_not_null_constraints(store):
    """Both k and v columns in both tables have NOT NULL."""
    await store.set("key", make_buffer(b"data"))
    con = store._con
    for table in ("zarr", "sqlitestore_metadata"):
        cur = con.execute(f"PRAGMA table_info({table})")
        for row in cur.fetchall():
            assert row[3] == 1, (
                f"Column '{row[1]}' in table '{table}' must have NOT NULL"
            )


@pytest.mark.asyncio
async def test_schema_application_id(store):
    """application_id is set to the spec value."""
    await store.set("key", make_buffer(b"data"))
    con = store._con
    cur = con.execute("PRAGMA application_id")
    assert cur.fetchone()[0] == _SQLITESTORE_APPLICATION_ID


@pytest.mark.asyncio
async def test_metadata_required_records(store):
    """All required metadata records exist with correct values."""
    await store.set("key", make_buffer(b"data"))
    con = store._con
    cur = con.execute("SELECT k, v FROM sqlitestore_metadata")
    metadata = dict(cur.fetchall())
    assert metadata["sqlitestore_version"] == _SQLITESTORE_SPEC_VERSION
    assert metadata["compatible_flags"] == ""
    assert metadata["incompatible_flags"] == ""
    assert "created_by" in metadata
    assert "created_time" in metadata


@pytest.mark.asyncio
async def test_metadata_created_by(store):
    """created_by contains the package name."""
    await store.set("key", make_buffer(b"data"))
    con = store._con
    cur = con.execute("SELECT v FROM sqlitestore_metadata WHERE k = 'created_by'")
    created_by = cur.fetchone()[0]
    assert created_by.startswith("zarr-sqlite-python")


@pytest.mark.asyncio
async def test_metadata_created_time(store):
    """created_time is a valid ISO 8601 timestamp."""
    await store.set("key", make_buffer(b"data"))
    con = store._con
    cur = con.execute("SELECT v FROM sqlitestore_metadata WHERE k = 'created_time'")
    created_time = cur.fetchone()[0]
    datetime.datetime.fromisoformat(created_time)


# ---------------------------------------------------------------------------
# Schema validation tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_validate_missing_zarr_table(tmpfile_store):
    """Opening a file without the zarr table should fail."""
    await tmpfile_store.set("key", make_buffer(b"data"))
    tmpfile_store.close()

    con = sqlite3.connect(tmpfile_store.database_uri, uri=True)
    con.execute("DROP TABLE zarr")
    con.commit()
    con.close()

    store = SQLiteStore(tmpfile_store.database_uri, read_only=True)
    with pytest.raises(ValueError, match="missing required table 'zarr'"):
        await store.exists("key")


@pytest.mark.asyncio
async def test_validate_missing_metadata_table(tmpfile_store):
    """Opening a file without sqlitestore_metadata should fail."""
    await tmpfile_store.set("key", make_buffer(b"data"))
    tmpfile_store.close()

    con = sqlite3.connect(tmpfile_store.database_uri, uri=True)
    con.execute("DROP TABLE sqlitestore_metadata")
    con.commit()
    con.close()

    store = SQLiteStore(tmpfile_store.database_uri, read_only=True)
    with pytest.raises(
        ValueError, match="missing required table 'sqlitestore_metadata'"
    ):
        await store.exists("key")


@pytest.mark.asyncio
async def test_validate_missing_metadata_record(tmpfile_store):
    """Opening a file with a missing required metadata record should fail."""
    await tmpfile_store.set("key", make_buffer(b"data"))
    tmpfile_store.close()

    con = sqlite3.connect(tmpfile_store.database_uri, uri=True)
    con.execute("DELETE FROM sqlitestore_metadata WHERE k = 'sqlitestore_version'")
    con.commit()
    con.close()

    store = SQLiteStore(tmpfile_store.database_uri, read_only=True)
    with pytest.raises(
        ValueError, match="missing required metadata entry 'sqlitestore_version'"
    ):
        await store.exists("key")


@pytest.mark.asyncio
async def test_validate_invalid_version(tmpfile_store):
    """Opening a file with an invalid version string should fail."""
    await tmpfile_store.set("key", make_buffer(b"data"))
    tmpfile_store.close()

    con = sqlite3.connect(tmpfile_store.database_uri, uri=True)
    con.execute(
        "UPDATE sqlitestore_metadata SET v = 'invalid' WHERE k = 'sqlitestore_version'"
    )
    con.commit()
    con.close()

    store = SQLiteStore(tmpfile_store.database_uri, read_only=True)
    with pytest.raises(ValueError, match="Invalid sqlitestore_version"):
        await store.exists("key")


@pytest.mark.asyncio
async def test_validate_unsupported_major_version(tmpfile_store):
    """Opening a file with an unsupported major version should fail."""
    await tmpfile_store.set("key", make_buffer(b"data"))
    tmpfile_store.close()

    con = sqlite3.connect(tmpfile_store.database_uri, uri=True)
    con.execute(
        "UPDATE sqlitestore_metadata SET v = '2.0' WHERE k = 'sqlitestore_version'"
    )
    con.commit()
    con.close()

    store = SQLiteStore(tmpfile_store.database_uri, read_only=True)
    with pytest.raises(ValueError, match="Unsupported sqlitestore_version"):
        await store.exists("key")


@pytest.mark.asyncio
async def test_validate_unknown_incompatible_flag(tmpfile_store):
    """Opening a file with an unknown incompatible flag should fail."""
    await tmpfile_store.set("key", make_buffer(b"data"))
    tmpfile_store.close()

    con = sqlite3.connect(tmpfile_store.database_uri, uri=True)
    con.execute(
        "UPDATE sqlitestore_metadata SET v = 'unknown_flag' "
        "WHERE k = 'incompatible_flags'"
    )
    con.commit()
    con.close()

    store = SQLiteStore(tmpfile_store.database_uri, read_only=True)
    with pytest.raises(
        ValueError, match="SQLiteStore flag 'unknown_flag' is not supported"
    ):
        await store.exists("key")


@pytest.mark.asyncio
async def test_read_only_valid_file(tmpfile_store):
    """A valid file can be opened in read-only mode."""
    await tmpfile_store.set("key", make_buffer(b"data"))
    tmpfile_store.close()

    store = SQLiteStore(tmpfile_store.database_uri, read_only=True)
    assert await get_as_bytes(store, "key") == b"data"
    store.close()

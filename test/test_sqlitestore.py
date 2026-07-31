"""Unit tests for the fundamental store interface of SQLiteStore."""

import sqlite3

import pytest
from zarr.core.buffer import default_buffer_prototype
from zarr.abc.store import OffsetByteRequest, RangeByteRequest, SuffixByteRequest


# ---------------------------------------------------------------------------
# Core get/set tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_set_and_get(store, make_buffer):
    data = b"hello world"
    await store.set("foo", make_buffer(data))
    buf = await store.get("foo", default_buffer_prototype())
    assert buf.to_bytes() == data


@pytest.mark.asyncio
async def test_get_nonexistent(store):
    buf = await store.get("missing", default_buffer_prototype())
    assert buf is None


@pytest.mark.asyncio
async def test_get_offset_byte_request(store, make_buffer):
    data = b"abcdefghij"
    await store.set("k", make_buffer(data))
    buf = await store.get(
        "k", default_buffer_prototype(), byte_range=OffsetByteRequest(3)
    )
    assert buf.to_bytes() == b"defghij"


@pytest.mark.asyncio
async def test_get_offset_beyond_length(store, make_buffer):
    data = b"abc"
    await store.set("k", make_buffer(data))
    buf = await store.get(
        "k", default_buffer_prototype(), byte_range=OffsetByteRequest(100)
    )
    assert buf.to_bytes() == b""


@pytest.mark.asyncio
async def test_get_range_byte_request(store, make_buffer):
    data = b"abcdefghij"
    await store.set("k", make_buffer(data))
    buf = await store.get(
        "k",
        default_buffer_prototype(),
        byte_range=RangeByteRequest(start=2, end=5),
    )
    assert buf.to_bytes() == b"cde"


@pytest.mark.asyncio
async def test_get_range_clamped(store, make_buffer):
    data = b"abcdefghijkl"
    await store.set("k", make_buffer(data))
    buf = await store.get(
        "k",
        default_buffer_prototype(),
        byte_range=RangeByteRequest(start=-5, end=100),
    )
    assert buf.to_bytes() == data


@pytest.mark.asyncio
async def test_get_suffix_byte_request(store, make_buffer):
    data = b"abcdefghij"
    await store.set("k", make_buffer(data))
    buf = await store.get(
        "k", default_buffer_prototype(), byte_range=SuffixByteRequest(3)
    )
    assert len(buf) == 3
    assert buf.to_bytes() == b"hij"


@pytest.mark.asyncio
async def test_get_suffix_larger_than_length(store, make_buffer):
    data = b"abc"
    await store.set("k", make_buffer(data))
    buf = await store.get(
        "k", default_buffer_prototype(), byte_range=SuffixByteRequest(100)
    )
    assert buf.to_bytes() == data


@pytest.mark.asyncio
async def test_get_non_bytes(tmpfile_store, make_buffer):
    await tmpfile_store.set("dummy", make_buffer(b"data"))
    con = sqlite3.connect(tmpfile_store.database, uri=True)
    con.execute("INSERT INTO zarr (k, v) VALUES (?, ?)", ("k", 5))
    con.commit()
    con.close()
    assert await tmpfile_store.get("k", default_buffer_prototype()) is None


@pytest.mark.asyncio
async def test_get_unsupported_byte_range(store, make_buffer):
    data = b"abc"
    await store.set("k", make_buffer(data))
    bad = object()
    with pytest.raises(ValueError):
        await store.get("k", default_buffer_prototype(), byte_range=bad)


@pytest.mark.asyncio
@pytest.mark.parametrize("store_fixture", ["store", "tmpfile_store"])
async def test_get_partial_values(request, store_fixture, make_buffer):
    """We run this both in-memory and with a file because the blob
    handle used for the partial request can cause concurrency issues
    (sqlite3 OperationalError: table locked) with in-memory databases.
    For this reason, the connection pool is limited to 1 connection
    for in-memory databases.
    """
    store = request.getfixturevalue(store_fixture)
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


# ---------------------------------------------------------------------------
# Set / delete tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_set_overwrites(store, make_buffer):
    await store.set("k", make_buffer(b"first"))
    buf = await store.get("k", default_buffer_prototype())
    assert buf.to_bytes() == b"first"

    await store.set("k", make_buffer(b"second"))
    buf = await store.get("k", default_buffer_prototype())
    assert buf.to_bytes() == b"second"


@pytest.mark.asyncio
async def test_delete_erases_key(store, make_buffer):
    await store.set("k", make_buffer(b"data"))
    assert await store.exists("k")
    await store.delete("k")
    assert not await store.exists("k")
    assert await store.get("k", default_buffer_prototype()) is None


@pytest.mark.asyncio
async def test_delete_missing_key_is_noop(store):
    await store.delete("never_existed")


@pytest.mark.asyncio
async def test_delete_dir_erases_prefix(store, make_buffer):
    await store.set("a/x", make_buffer(b"1"))
    await store.set("a/y", make_buffer(b"2"))
    await store.set("b/z", make_buffer(b"3"))
    await store.delete_dir("a/")
    assert not await store.exists("a/x")
    assert not await store.exists("a/y")
    assert await store.exists("b/z")


@pytest.mark.asyncio
async def test_delete_dir_raises_when_key_is_leaf(store, make_buffer):
    await store.set("a", make_buffer(b"leaf"))
    with pytest.raises(ValueError):
        await store.delete_dir("a")


@pytest.mark.asyncio
async def test_delete_dir(store, make_buffer):
    await store.set("a/b", make_buffer(b"1"))
    await store.delete_dir("a/")
    assert not await store.exists("a/b")


# ---------------------------------------------------------------------------
# Listing tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_returns_all_keys(store, make_buffer, collect):
    await store.set("a", make_buffer(b"1"))
    await store.set("b/c", make_buffer(b"2"))
    await store.set("b/d", make_buffer(b"3"))
    keys = set(await collect(store.list()))
    assert keys == {"a", "b/c", "b/d"}


@pytest.mark.asyncio
async def test_list_empty(store, collect):
    assert await collect(store.list()) == []


@pytest.mark.asyncio
async def test_list_prefix(store, make_buffer, collect):
    await store.set("a/1", make_buffer(b"1"))
    await store.set("a/2", make_buffer(b"2"))
    await store.set("ab/3", make_buffer(b"3"))
    await store.set("b/4", make_buffer(b"4"))
    keys = set(await collect(store.list_prefix("a/")))
    assert keys == {"a/1", "a/2"}


@pytest.mark.asyncio
async def test_list_prefix_root(store, make_buffer, collect):
    await store.set("a/1", make_buffer(b"1"))
    await store.set("b/2", make_buffer(b"2"))
    keys = set(await collect(store.list_prefix("")))
    assert keys == {"a/1", "b/2"}


@pytest.mark.asyncio
async def test_list_prefix_no_match(store, make_buffer, collect):
    await store.set("a/1", make_buffer(b"1"))
    assert await collect(store.list_prefix("zzz/")) == []


@pytest.mark.asyncio
async def test_list_dir_root(store, make_buffer, collect):
    await store.set("a/1", make_buffer(b"1"))
    await store.set("b/2", make_buffer(b"2"))
    await store.set("c/d/3", make_buffer(b"3"))
    await store.set("leaf", make_buffer(b"3"))

    entries = set(await collect(store.list_dir("")))
    assert entries == {"a/", "b/", "c/", "leaf"}


@pytest.mark.asyncio
async def test_list_dir_nested(store, make_buffer, collect):
    await store.set("a/x", make_buffer(b"1"))
    await store.set("a/y", make_buffer(b"2"))
    await store.set("a/sub/z", make_buffer(b"3"))
    entries = set(await collect(store.list_dir("a/")))
    assert entries == {"x", "y", "sub/"}


@pytest.mark.asyncio
async def test_list_dir_no_match(store, make_buffer, collect):
    await store.set("a/1", make_buffer(b"1"))
    assert await collect(store.list_dir("zzz/")) == []


@pytest.mark.asyncio
async def test_list_dir_empty_prefix_yields_nothing(store, collect):
    assert await collect(store.list_dir("nonexistent/")) == []


# ---------------------------------------------------------------------------
# set_if_not_exists
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_set_if_not_exists(store, make_buffer):
    await store.set_if_not_exists("k", make_buffer(b"first"))
    await store.set_if_not_exists("k", make_buffer(b"second"))
    buf = await store.get("k", default_buffer_prototype())
    assert buf.to_bytes() == b"first"


# ---------------------------------------------------------------------------
# exists / is_empty / clear
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_exists(store, make_buffer):
    assert not await store.exists("k")
    await store.set("k", make_buffer(b"data"))
    assert await store.exists("k")


@pytest.mark.asyncio
async def test_is_empty(store, make_buffer):
    assert await store.is_empty("")
    assert await store.is_empty("a/")

    await store.set("a/b", make_buffer(b"1"))
    assert not await store.is_empty("")
    assert not await store.is_empty("a/")
    assert await store.is_empty("c/")


@pytest.mark.asyncio
async def test_clear(store, make_buffer, collect):
    await store.set("a", make_buffer(b"1"))
    await store.set("b/c", make_buffer(b"2"))
    assert len(await collect(store.list())) == 2
    await store.clear()
    assert await collect(store.list()) == []


# ---------------------------------------------------------------------------
# getsize
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_getsize(store, make_buffer):
    await store.set("k", make_buffer(b"12345"))
    assert await store.getsize("k") == 5

    await store.set("empty", make_buffer(b""))
    assert await store.getsize("empty") == 0

    with pytest.raises(FileNotFoundError):
        await store.getsize("missing")


@pytest.mark.asyncio
async def test_getsize_prefix(store, make_buffer):
    await store.set("a/1", make_buffer(b"abc"))
    await store.set("a/2", make_buffer(b"de"))
    await store.set("a/3", make_buffer(b""))
    assert await store.getsize_prefix("a/") == 5

    # non-existent prefix, getsize_prefix should return 0
    assert await store.getsize_prefix("missing/") == 0


@pytest.mark.asyncio
async def test_getsize_prefix_empty(store, make_buffer, collect):
    """getsize_prefix with empty prefix returns total size of all values."""
    await store.set("a", make_buffer(b"abc"))
    await store.set("b/c", make_buffer(b"de"))
    await store.set("d/e/f", make_buffer(b""))
    assert await store.getsize_prefix("") == 5

    # empty store, getsize_prefix("") should return 0
    await store.clear()
    assert await store.getsize_prefix("") == 0


# ---------------------------------------------------------------------------
# delete_dir with empty prefix
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_delete_dir_empty_prefix(store, make_buffer, collect):
    """delete_dir with empty prefix deletes all keys."""
    await store.set("a/b", make_buffer(b"1"))
    await store.set("c/d", make_buffer(b"2"))
    await store.set("leaf", make_buffer(b"3"))
    await store.delete_dir("")
    assert await collect(store.list()) == []


@pytest.mark.asyncio
async def test_delete_dir_empty_prefix_raises_when_root_key(store, make_buffer):
    """delete_dir with empty prefix raises when the root key '' exists."""
    await store.set("", make_buffer(b"root"))
    with pytest.raises(ValueError, match="Cannot delete directory"):
        await store.delete_dir("")

"""Unit tests for the fundamental store interface of SQLiteStore."""

import sqlite3

import pytest
from zarr.core.buffer import default_buffer_prototype
from zarr.abc.store import OffsetByteRequest, RangeByteRequest, SuffixByteRequest


# ---------------------------------------------------------------------------
# Core get/set tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_set_and_get(memstore, make_buffer):
    data = b"hello world"
    await memstore.set("foo", make_buffer(data))
    buf = await memstore.get("foo", default_buffer_prototype())
    assert buf.to_bytes() == data


@pytest.mark.asyncio
async def test_get_nonexistent(memstore):
    buf = await memstore.get("missing", default_buffer_prototype())
    assert buf is None


@pytest.mark.asyncio
async def test_get_offset_byte_request(memstore, make_buffer):
    data = b"abcdefghij"
    await memstore.set("k", make_buffer(data))
    buf = await memstore.get(
        "k", default_buffer_prototype(), byte_range=OffsetByteRequest(3)
    )
    assert buf.to_bytes() == b"defghij"


@pytest.mark.asyncio
async def test_get_offset_beyond_length(memstore, make_buffer):
    data = b"abc"
    await memstore.set("k", make_buffer(data))
    buf = await memstore.get(
        "k", default_buffer_prototype(), byte_range=OffsetByteRequest(100)
    )
    assert buf.to_bytes() == b""


@pytest.mark.asyncio
async def test_get_range_byte_request(memstore, make_buffer):
    data = b"abcdefghij"
    await memstore.set("k", make_buffer(data))
    buf = await memstore.get(
        "k",
        default_buffer_prototype(),
        byte_range=RangeByteRequest(start=2, end=5),
    )
    assert buf.to_bytes() == b"cde"


@pytest.mark.asyncio
async def test_get_range_clamped(memstore, make_buffer):
    data = b"abcdefghijkl"
    await memstore.set("k", make_buffer(data))
    buf = await memstore.get(
        "k",
        default_buffer_prototype(),
        byte_range=RangeByteRequest(start=-5, end=100),
    )
    assert buf.to_bytes() == data


@pytest.mark.asyncio
async def test_get_suffix_byte_request(memstore, make_buffer):
    data = b"abcdefghij"
    await memstore.set("k", make_buffer(data))
    buf = await memstore.get(
        "k", default_buffer_prototype(), byte_range=SuffixByteRequest(3)
    )
    assert len(buf) == 3
    assert buf.to_bytes() == b"hij"


@pytest.mark.asyncio
async def test_get_suffix_larger_than_length(memstore, make_buffer):
    data = b"abc"
    await memstore.set("k", make_buffer(data))
    buf = await memstore.get(
        "k", default_buffer_prototype(), byte_range=SuffixByteRequest(100)
    )
    assert buf.to_bytes() == data


@pytest.mark.asyncio
async def test_get_non_bytes(tempstore, make_buffer):
    await tempstore.set("dummy", make_buffer(b"data"))
    con = sqlite3.connect(tempstore.database, uri=True)
    con.execute("INSERT INTO zarr (k, v) VALUES (?, ?)", ("k", 5))
    con.commit()
    con.close()
    assert await tempstore.get("k", default_buffer_prototype()) is None


@pytest.mark.asyncio
async def test_get_unsupported_byte_range(memstore, make_buffer):
    data = b"abc"
    await memstore.set("k", make_buffer(data))
    bad = object()
    with pytest.raises(ValueError):
        await memstore.get("k", default_buffer_prototype(), byte_range=bad)


@pytest.mark.asyncio
@pytest.mark.parametrize("store_fixture", ["memstore", "tempstore"])
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
async def test_set_overwrites(memstore, make_buffer):
    await memstore.set("k", make_buffer(b"first"))
    buf = await memstore.get("k", default_buffer_prototype())
    assert buf.to_bytes() == b"first"

    await memstore.set("k", make_buffer(b"second"))
    buf = await memstore.get("k", default_buffer_prototype())
    assert buf.to_bytes() == b"second"


@pytest.mark.asyncio
async def test_delete_erases_key(memstore, make_buffer):
    await memstore.set("k", make_buffer(b"data"))
    assert await memstore.exists("k")
    await memstore.delete("k")
    assert not await memstore.exists("k")
    assert await memstore.get("k", default_buffer_prototype()) is None


@pytest.mark.asyncio
async def test_delete_missing_key_is_noop(memstore):
    await memstore.delete("never_existed")


@pytest.mark.asyncio
async def test_delete_dir_erases_prefix(memstore, make_buffer):
    await memstore.set("a/x", make_buffer(b"1"))
    await memstore.set("a/y", make_buffer(b"2"))
    await memstore.set("b/z", make_buffer(b"3"))
    await memstore.delete_dir("a/")
    assert not await memstore.exists("a/x")
    assert not await memstore.exists("a/y")
    assert await memstore.exists("b/z")


@pytest.mark.asyncio
async def test_delete_dir_raises_when_key_is_leaf(memstore, make_buffer):
    await memstore.set("a", make_buffer(b"leaf"))
    with pytest.raises(ValueError):
        await memstore.delete_dir("a")


@pytest.mark.asyncio
async def test_delete_dir(memstore, make_buffer):
    await memstore.set("a/b", make_buffer(b"1"))
    await memstore.delete_dir("a/")
    assert not await memstore.exists("a/b")


# ---------------------------------------------------------------------------
# Listing tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_returns_all_keys(memstore, make_buffer, collect):
    await memstore.set("a", make_buffer(b"1"))
    await memstore.set("b/c", make_buffer(b"2"))
    await memstore.set("b/d", make_buffer(b"3"))
    keys = set(await collect(memstore.list()))
    assert keys == {"a", "b/c", "b/d"}


@pytest.mark.asyncio
async def test_list_empty(memstore, collect):
    assert await collect(memstore.list()) == []


@pytest.mark.asyncio
async def test_list_prefix(memstore, make_buffer, collect):
    await memstore.set("a/1", make_buffer(b"1"))
    await memstore.set("a/2", make_buffer(b"2"))
    await memstore.set("ab/3", make_buffer(b"3"))
    await memstore.set("b/4", make_buffer(b"4"))
    keys = set(await collect(memstore.list_prefix("a/")))
    assert keys == {"a/1", "a/2"}


@pytest.mark.asyncio
async def test_list_prefix_root(memstore, make_buffer, collect):
    await memstore.set("a/1", make_buffer(b"1"))
    await memstore.set("b/2", make_buffer(b"2"))
    keys = set(await collect(memstore.list_prefix("")))
    assert keys == {"a/1", "b/2"}


@pytest.mark.asyncio
async def test_list_prefix_no_match(memstore, make_buffer, collect):
    await memstore.set("a/1", make_buffer(b"1"))
    assert await collect(memstore.list_prefix("zzz/")) == []


@pytest.mark.asyncio
async def test_list_dir_root(memstore, make_buffer, collect):
    await memstore.set("a/1", make_buffer(b"1"))
    await memstore.set("b/2", make_buffer(b"2"))
    await memstore.set("c/d/3", make_buffer(b"3"))
    await memstore.set("leaf", make_buffer(b"3"))

    entries = set(await collect(memstore.list_dir("")))
    assert entries == {"a/", "b/", "c/", "leaf"}


@pytest.mark.asyncio
async def test_list_dir_nested(memstore, make_buffer, collect):
    await memstore.set("a/x", make_buffer(b"1"))
    await memstore.set("a/y", make_buffer(b"2"))
    await memstore.set("a/sub/z", make_buffer(b"3"))
    entries = set(await collect(memstore.list_dir("a/")))
    assert entries == {"x", "y", "sub/"}


@pytest.mark.asyncio
async def test_list_dir_no_match(memstore, make_buffer, collect):
    await memstore.set("a/1", make_buffer(b"1"))
    assert await collect(memstore.list_dir("zzz/")) == []


@pytest.mark.asyncio
async def test_list_dir_empty_prefix_yields_nothing(memstore, collect):
    assert await collect(memstore.list_dir("nonexistent/")) == []


# ---------------------------------------------------------------------------
# set_if_not_exists
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_set_if_not_exists(memstore, make_buffer):
    await memstore.set_if_not_exists("k", make_buffer(b"first"))
    await memstore.set_if_not_exists("k", make_buffer(b"second"))
    buf = await memstore.get("k", default_buffer_prototype())
    assert buf.to_bytes() == b"first"


# ---------------------------------------------------------------------------
# exists / is_empty / clear
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_exists(memstore, make_buffer):
    assert not await memstore.exists("k")
    await memstore.set("k", make_buffer(b"data"))
    assert await memstore.exists("k")


@pytest.mark.asyncio
async def test_is_empty(memstore, make_buffer):
    assert await memstore.is_empty("")
    assert await memstore.is_empty("a/")

    await memstore.set("a/b", make_buffer(b"1"))
    assert not await memstore.is_empty("")
    assert not await memstore.is_empty("a/")
    assert await memstore.is_empty("c/")


@pytest.mark.asyncio
async def test_clear(memstore, make_buffer, collect):
    await memstore.set("a", make_buffer(b"1"))
    await memstore.set("b/c", make_buffer(b"2"))
    assert len(await collect(memstore.list())) == 2
    await memstore.clear()
    assert await collect(memstore.list()) == []


# ---------------------------------------------------------------------------
# getsize
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_getsize(memstore, make_buffer):
    await memstore.set("k", make_buffer(b"12345"))
    assert await memstore.getsize("k") == 5

    await memstore.set("empty", make_buffer(b""))
    assert await memstore.getsize("empty") == 0

    with pytest.raises(FileNotFoundError):
        await memstore.getsize("missing")


@pytest.mark.asyncio
async def test_getsize_prefix(memstore, make_buffer):
    await memstore.set("a/1", make_buffer(b"abc"))
    await memstore.set("a/2", make_buffer(b"de"))
    await memstore.set("a/3", make_buffer(b""))
    assert await memstore.getsize_prefix("a/") == 5

    # non-existent prefix, getsize_prefix should return 0
    assert await memstore.getsize_prefix("missing/") == 0


@pytest.mark.asyncio
async def test_getsize_prefix_empty(memstore, make_buffer, collect):
    """getsize_prefix with empty prefix returns total size of all values."""
    await memstore.set("a", make_buffer(b"abc"))
    await memstore.set("b/c", make_buffer(b"de"))
    await memstore.set("d/e/f", make_buffer(b""))
    assert await memstore.getsize_prefix("") == 5

    # empty store, getsize_prefix("") should return 0
    await memstore.clear()
    assert await memstore.getsize_prefix("") == 0


# ---------------------------------------------------------------------------
# delete_dir with empty prefix
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_delete_dir_empty_prefix(memstore, make_buffer, collect):
    """delete_dir with empty prefix deletes all keys."""
    await memstore.set("a/b", make_buffer(b"1"))
    await memstore.set("c/d", make_buffer(b"2"))
    await memstore.set("leaf", make_buffer(b"3"))
    await memstore.delete_dir("")
    assert await collect(memstore.list()) == []


@pytest.mark.asyncio
async def test_delete_dir_empty_prefix_raises_when_root_key(memstore, make_buffer):
    """delete_dir with empty prefix raises when the root key '' exists."""
    await memstore.set("", make_buffer(b"root"))
    with pytest.raises(ValueError, match="Cannot delete directory"):
        await memstore.delete_dir("")

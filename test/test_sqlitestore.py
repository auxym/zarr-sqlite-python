"""Unit tests for the public API of SQLiteStore."""

import pytest
from zarr.core.buffer import BufferPrototype, default_buffer_prototype
from zarr.abc.store import OffsetByteRequest, RangeByteRequest, SuffixByteRequest

from zarr_sqlite import SQLiteStore


def make_buffer(data: bytes, prototype: BufferPrototype | None = None) -> object:
    prototype = prototype or default_buffer_prototype()
    return prototype.buffer.from_bytes(data)


async def collect(iterator):
    return [item async for item in iterator]


@pytest.fixture
def store():
    s = SQLiteStore(":memory:")
    yield s
    s.close()


@pytest.mark.asyncio
async def test_set_and_get(store):
    data = b"hello world"
    await store.set("foo", make_buffer(data))
    buf = await store.get("foo", default_buffer_prototype())
    assert buf.to_bytes() == data


@pytest.mark.asyncio
async def test_get_nonexistent_returns_none(store):
    buf = await store.get("missing", default_buffer_prototype())
    assert buf is None


@pytest.mark.asyncio
async def test_get_byte_range_none(store):
    data = b"abcdefghij"
    await store.set("k", make_buffer(data))
    buf = await store.get("k", default_buffer_prototype(), byte_range=None)
    assert buf.to_bytes() == data


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
    data = b"abc"
    await store.set("k", make_buffer(data))
    buf = await store.get(
        "k",
        default_buffer_prototype(),
        byte_range=RangeByteRequest(start=-5, end=100),
    )
    assert buf.to_bytes() == b"abc"


@pytest.mark.asyncio
async def test_get_suffix_byte_request(store):
    data = b"abcdefghij"
    await store.set("k", make_buffer(data))
    buf = await store.get(
        "k", default_buffer_prototype(), byte_range=SuffixByteRequest(3)
    )
    assert buf.to_bytes() == b"hij"


@pytest.mark.asyncio
async def test_get_suffix_larger_than_length(store):
    data = b"abc"
    await store.set("k", make_buffer(data))
    buf = await store.get(
        "k", default_buffer_prototype(), byte_range=SuffixByteRequest(100)
    )
    assert buf.to_bytes() == b"abc"


@pytest.mark.asyncio
async def test_get_non_bytes_raises(store):
    await store._execute_write("INSERT INTO zarr (k, v) VALUES (?, ?)", ("k", 5))
    with pytest.raises(TypeError):
        await store.get("k", default_buffer_prototype())


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

    entries = set(await collect(store.list_dir("")))
    assert entries == {"a/", "b/", "c/"}


@pytest.mark.asyncio
async def test_list_dir_nested(store):
    await store.set("a/x", make_buffer(b"1"))
    await store.set("a/y", make_buffer(b"2"))
    await store.set("a/sub/z", make_buffer(b"3"))
    entries = set(await collect(store.list_dir("a/")))
    assert entries == {"x", "y", "sub/"}


@pytest.mark.asyncio
async def test_list_dir_returns_leaf_and_prefix(store):
    await store.set("a/leaf", make_buffer(b"1"))
    await store.set("a/group/child", make_buffer(b"2"))
    entries = set(await collect(store.list_dir("a/")))
    assert entries == {"leaf", "group/"}


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
    assert await store.is_empty("a/")
    await store.set("a/b", make_buffer(b"1"))
    assert not await store.is_empty("a/")
    assert await store.is_empty("c/")

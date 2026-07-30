"""Unit tests for key validation and key handling in SQLiteStore."""

import pytest

from zarr_sqlite.zarr_sqlite import _validate_key


# ---------------------------------------------------------------------------
# Key validation tests
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Case sensitivity tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_case_sensitive_keys_get_set(store, make_buffer, get_as_bytes):
    """Keys are case-sensitive: 'Key' and 'key' are different keys."""
    await store.set("Key", make_buffer(b"uppercase"))
    await store.set("key", make_buffer(b"lowercase"))
    assert await get_as_bytes(store, "Key") == b"uppercase"
    assert await get_as_bytes(store, "key") == b"lowercase"


@pytest.mark.asyncio
async def test_case_sensitive_keys_list(store, make_buffer, collect):
    """list returns keys with different cases as distinct entries."""
    await store.set("Apple", make_buffer(b"1"))
    await store.set("apple", make_buffer(b"2"))
    await store.set("APPLE", make_buffer(b"3"))
    keys = set(await collect(store.list()))
    assert keys == {"Apple", "apple", "APPLE"}


@pytest.mark.asyncio
async def test_case_sensitive_list_prefix(store, make_buffer, collect):
    """list_prefix is case-sensitive."""
    await store.set("Data/a", make_buffer(b"1"))
    await store.set("data/b", make_buffer(b"2"))
    await store.set("DATA/c", make_buffer(b"3"))
    keys = set(await collect(store.list_prefix("Data/")))
    assert keys == {"Data/a"}


# ---------------------------------------------------------------------------
# Non-alphanumeric characters in keys (non-glob-special chars)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_non_alphanumeric_keys_get_set(store, make_buffer, get_as_bytes):
    """Keys with non-alphanumeric characters can be stored and retrieved."""
    special_keys = ["a%b", "a^b", "a¸b", "a_b", "a-b.c", "a+b", "a=b"]
    for key in special_keys:
        await store.set(key, make_buffer(b"data"))
        assert await get_as_bytes(store, key) == b"data"


@pytest.mark.asyncio
async def test_non_alphanumeric_keys_list(store, make_buffer, collect):
    """list returns keys with non-alphanumeric characters."""
    keys_to_set = ["a%b", "a^b", "a¸b", "a_b", "a-b.c"]
    for key in keys_to_set:
        await store.set(key, make_buffer(b"data"))
    result = set(await collect(store.list()))
    assert result == set(keys_to_set)


@pytest.mark.asyncio
async def test_list_prefix_with_special_chars(store, make_buffer, collect):
    """list_prefix works with prefixes containing non-glob special characters."""
    await store.set("a%b/x", make_buffer(b"1"))
    await store.set("a%b/y", make_buffer(b"2"))
    await store.set("a^b/z", make_buffer(b"3"))
    keys = set(await collect(store.list_prefix("a%b/")))
    assert keys == {"a%b/x", "a%b/y"}


@pytest.mark.asyncio
async def test_list_dir_with_special_chars(store, make_buffer, collect):
    """list_dir works with prefixes containing non-glob special characters."""
    await store.set("a%b/x", make_buffer(b"1"))
    await store.set("a%b/y", make_buffer(b"2"))
    await store.set("a%b/sub/z", make_buffer(b"3"))
    entries = set(await collect(store.list_dir("a%b/")))
    assert entries == {"x", "y", "sub/"}


@pytest.mark.asyncio
async def test_list_prefix_glob_star_in_prefix(store, make_buffer, collect):
    """list_prefix with '*' in prefix should not treat '*' as a glob wildcard."""
    await store.set("a*x/y", make_buffer(b"1"))
    await store.set("abx/y", make_buffer(b"2"))
    keys = set(await collect(store.list_prefix("a*x")))
    assert keys == {"a*x/y"}


@pytest.mark.asyncio
async def test_list_prefix_glob_question_in_prefix(store, make_buffer, collect):
    """list_prefix with '?' in prefix should not treat '?' as a glob wildcard."""
    await store.set("a?b/y", make_buffer(b"1"))
    await store.set("axb/y", make_buffer(b"2"))
    keys = set(await collect(store.list_prefix("a?b")))
    assert keys == {"a?b/y"}


# ---------------------------------------------------------------------------
# Unicode and emoji tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_unicode_keys_get_set(store, make_buffer, get_as_bytes):
    """Keys with unicode characters can be stored and retrieved."""
    unicode_keys = [
        "café/data",
        "日本語/key",
        "español/ñ",
        "русский/язык",
        "中文/测试",
        "العربية/لغة",
    ]
    for key in unicode_keys:
        await store.set(key, make_buffer(b"unicode_data"))
        assert await get_as_bytes(store, key) == b"unicode_data"


@pytest.mark.asyncio
async def test_unicode_keys_list(store, make_buffer, collect):
    """list returns keys with unicode characters."""
    unicode_keys = [
        "café/data",
        "日本語/key",
        "español/ñ",
        "русский/язык",
        "中文/测试",
        "العربية/لغة",
    ]
    for key in unicode_keys:
        await store.set(key, make_buffer(b"unicode_data"))
    result = set(await collect(store.list()))
    assert result == set(unicode_keys)


@pytest.mark.asyncio
async def test_unicode_list_prefix(store, make_buffer, collect):
    """list_prefix works with unicode prefixes."""
    await store.set("café/data", make_buffer(b"1"))
    await store.set("café/other", make_buffer(b"2"))
    await store.set("日本語/key", make_buffer(b"3"))
    keys = set(await collect(store.list_prefix("café/")))
    assert keys == {"café/data", "café/other"}


@pytest.mark.asyncio
async def test_emoji_keys_get_set(store, make_buffer, get_as_bytes):
    """Keys with emoji characters can be stored and retrieved."""
    emoji_keys = [
        "🎉/party",
        "🎊/confetti",
        "🚀/launch",
        "🌟/star",
        "🎉/data",
    ]
    for key in emoji_keys:
        await store.set(key, make_buffer(b"emoji_data"))
        assert await get_as_bytes(store, key) == b"emoji_data"


@pytest.mark.asyncio
async def test_emoji_keys_list(store, make_buffer, collect):
    """list returns keys with emoji characters."""
    emoji_keys = [
        "🎉/party",
        "🎊/confetti",
        "🚀/launch",
        "🌟/star",
        "🎉/data",
    ]
    for key in emoji_keys:
        await store.set(key, make_buffer(b"emoji_data"))
    result = set(await collect(store.list()))
    assert result == set(emoji_keys)


@pytest.mark.asyncio
async def test_emoji_list_prefix(store, make_buffer, collect):
    """list_prefix works with emoji prefixes."""
    await store.set("🎉/party", make_buffer(b"1"))
    await store.set("🎉/data", make_buffer(b"2"))
    await store.set("🎊/confetti", make_buffer(b"3"))
    keys = set(await collect(store.list_prefix("🎉/")))
    assert keys == {"🎉/party", "🎉/data"}


@pytest.mark.asyncio
async def test_mixed_unicode_emoji_keys(store, make_buffer, get_as_bytes, collect):
    """Keys with mixed unicode and emoji characters can be stored and listed."""
    mixed_keys = [
        "café/🎉",
        "日本語/🚀",
        "🎊/español",
        "русский/🌟",
    ]
    for key in mixed_keys:
        await store.set(key, make_buffer(b"mixed_data"))
        assert await get_as_bytes(store, key) == b"mixed_data"
    result = set(await collect(store.list()))
    assert result == set(mixed_keys)

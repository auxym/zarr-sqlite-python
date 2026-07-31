"""Unit tests for SQLiteStore schema creation and validation."""

import datetime
import re
import sqlite3

import pytest

from zarr_sqlite import SQLiteStore
from zarr_sqlite.zarr_sqlite import (
    _SQLITESTORE_SPEC_VERSION,
    _SQLITESTORE_APPLICATION_ID,
)


# ---------------------------------------------------------------------------
# Schema creation tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_schema_tables_exist(tempstore, make_buffer):
    """Both required tables are created on first use."""
    await tempstore.set("key", make_buffer(b"data"))
    con = sqlite3.connect(tempstore.database)
    cur = con.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = {row[0] for row in cur.fetchall()}
    con.close()
    assert "zarr" in tables
    assert "sqlitestore_metadata" in tables


@pytest.mark.asyncio
async def test_schema_not_null_constraints(tempstore, make_buffer):
    """Both k and v columns in both tables have NOT NULL."""
    await tempstore.set("key", make_buffer(b"data"))
    con = sqlite3.connect(tempstore.database)
    for table in ("zarr", "sqlitestore_metadata"):
        cur = con.execute(f"PRAGMA table_info({table})")
        for row in cur.fetchall():
            assert row[3] == 1, (
                f"Column '{row[1]}' in table '{table}' must have NOT NULL"
            )
    con.close()


@pytest.mark.asyncio
async def test_schema_application_id(tempstore, make_buffer):
    """application_id is set to the spec value."""
    await tempstore.set("key", make_buffer(b"data"))
    con = sqlite3.connect(tempstore.database)
    cur = con.execute("PRAGMA application_id")
    assert cur.fetchone()[0] == _SQLITESTORE_APPLICATION_ID
    con.close()


@pytest.mark.asyncio
async def test_metadata_required_records(tempstore, make_buffer):
    """All required metadata records exist with correct values."""
    await tempstore.set("key", make_buffer(b"data"))
    con = sqlite3.connect(tempstore.database)
    cur = con.execute("SELECT k, v FROM sqlitestore_metadata")
    metadata = dict(cur.fetchall())
    con.close()
    assert metadata["sqlitestore_version"] == _SQLITESTORE_SPEC_VERSION
    assert metadata["compatible_flags"] == ""
    assert metadata["incompatible_flags"] == ""
    assert "created_by" in metadata
    assert "created_time" in metadata


@pytest.mark.asyncio
async def test_metadata_created_by(tempstore, make_buffer):
    """created_by contains the package name."""
    await tempstore.set("key", make_buffer(b"data"))
    con = sqlite3.connect(tempstore.database)
    cur = con.execute("SELECT v FROM sqlitestore_metadata WHERE k = 'created_by'")
    created_by = cur.fetchone()[0]
    con.close()
    assert created_by.startswith("zarr-sqlite-python")


@pytest.mark.asyncio
async def test_metadata_created_time(tempstore, make_buffer):
    """created_time is a valid ISO 8601 timestamp within the last 5 minutes."""
    TIMESTAMP_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$")
    await tempstore.set("key", make_buffer(b"data"))
    with sqlite3.connect(tempstore.database, uri=True) as con:
        cur = con.execute("SELECT v FROM sqlitestore_metadata WHERE k = 'created_time'")
        created_time = cur.fetchone()[0]

    assert TIMESTAMP_PATTERN.match(created_time) is not None

    created_time_parsed = datetime.datetime.fromisoformat(created_time)
    assert created_time_parsed.tzinfo == datetime.timezone.utc

    now = datetime.datetime.now(datetime.timezone.utc)
    assert now - datetime.timedelta(minutes=5) <= created_time_parsed <= now

# ---------------------------------------------------------------------------
# Schema validation tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_validate_missing_zarr_table(tempstore, make_buffer):
    """Opening a file without the zarr table should fail."""
    await tempstore.set("key", make_buffer(b"data"))
    tempstore.close()

    con = sqlite3.connect(tempstore.database, uri=True)
    con.execute("DROP TABLE zarr")
    con.commit()
    con.close()

    store = SQLiteStore(tempstore.database, read_only=True)
    with pytest.raises(ValueError, match="missing required table 'zarr'"):
        await store.exists("key")


@pytest.mark.asyncio
async def test_validate_missing_metadata_table(tempstore, make_buffer):
    """Opening a file without sqlitestore_metadata should fail."""
    await tempstore.set("key", make_buffer(b"data"))
    tempstore.close()

    con = sqlite3.connect(tempstore.database, uri=True)
    con.execute("DROP TABLE sqlitestore_metadata")
    con.commit()
    con.close()

    store = SQLiteStore(tempstore.database, read_only=True)
    with pytest.raises(
        ValueError, match="missing required table 'sqlitestore_metadata'"
    ):
        await store.exists("key")


@pytest.mark.asyncio
async def test_validate_missing_metadata_record(tempstore, make_buffer):
    """Opening a file with a missing required metadata record should fail."""
    await tempstore.set("key", make_buffer(b"data"))
    tempstore.close()

    con = sqlite3.connect(tempstore.database, uri=True)
    con.execute("DELETE FROM sqlitestore_metadata WHERE k = 'sqlitestore_version'")
    con.commit()
    con.close()

    store = SQLiteStore(tempstore.database, read_only=True)
    with pytest.raises(
        ValueError, match="missing required metadata entry 'sqlitestore_version'"
    ):
        await store.exists("key")


@pytest.mark.asyncio
async def test_validate_invalid_version(tempstore, make_buffer):
    """Opening a file with an invalid version string should fail."""
    await tempstore.set("key", make_buffer(b"data"))
    tempstore.close()

    con = sqlite3.connect(tempstore.database, uri=True)
    con.execute(
        "UPDATE sqlitestore_metadata SET v = 'invalid' WHERE k = 'sqlitestore_version'"
    )
    con.commit()
    con.close()

    store = SQLiteStore(tempstore.database, read_only=True)
    with pytest.raises(ValueError, match="Invalid sqlitestore_version"):
        await store.exists("key")


@pytest.mark.asyncio
async def test_validate_unsupported_major_version(tempstore, make_buffer):
    """Opening a file with an unsupported major version should fail."""
    await tempstore.set("key", make_buffer(b"data"))
    tempstore.close()

    con = sqlite3.connect(tempstore.database, uri=True)
    con.execute(
        "UPDATE sqlitestore_metadata SET v = '2.0' WHERE k = 'sqlitestore_version'"
    )
    con.commit()
    con.close()

    store = SQLiteStore(tempstore.database, read_only=True)
    with pytest.raises(ValueError, match="Unsupported sqlitestore_version"):
        await store.exists("key")


@pytest.mark.asyncio
async def test_validate_unknown_incompatible_flag(tempstore, make_buffer):
    """Opening a file with an unknown incompatible flag should fail."""
    await tempstore.set("key", make_buffer(b"data"))
    tempstore.close()

    con = sqlite3.connect(tempstore.database, uri=True)
    con.execute(
        "UPDATE sqlitestore_metadata SET v = 'unknown_flag' "
        "WHERE k = 'incompatible_flags'"
    )
    con.commit()
    con.close()

    store = SQLiteStore(tempstore.database, read_only=True)
    with pytest.raises(
        ValueError, match="SQLiteStore flag 'unknown_flag' is not supported"
    ):
        await store.exists("key")


@pytest.mark.asyncio
async def test_read_only_valid_file(tempstore, make_buffer, get_as_bytes):
    """A valid file can be opened in read-only mode."""
    await tempstore.set("key", make_buffer(b"data"))
    tempstore.close()

    store = SQLiteStore(tempstore.database, read_only=True)
    assert await get_as_bytes(store, "key") == b"data"
    store.close()


@pytest.mark.asyncio
async def test_validate_wrong_application_id_writable(tempstore, make_buffer):
    """Opening a writable store with a wrong application_id should raise."""
    await tempstore.set("key", make_buffer(b"data"))
    tempstore.close()

    con = sqlite3.connect(tempstore.database, uri=True)
    con.execute("PRAGMA application_id = 0")
    con.commit()
    con.close()

    store = SQLiteStore(tempstore.database, read_only=False)
    with pytest.raises(ValueError, match="Unexpected application_id"):
        await store.exists("key")
    store.close()


@pytest.mark.asyncio
async def test_validate_wrong_application_id_read_only(
    tempstore, make_buffer, get_as_bytes
):
    """Opening a read-only store with a wrong application_id should warn."""
    await tempstore.set("key", make_buffer(b"data"))
    tempstore.close()

    con = sqlite3.connect(tempstore.database, uri=True)
    con.execute("PRAGMA application_id = 0")
    con.commit()
    con.close()

    store = SQLiteStore(tempstore.database, read_only=True)
    with pytest.warns(UserWarning, match="Unexpected application_id"):
        assert await get_as_bytes(store, "key") == b"data"
    store.close()

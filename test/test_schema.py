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

from test.helpers import make_buffer, get_as_bytes


# ---------------------------------------------------------------------------
# Schema creation tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_schema_tables_exist(tempstore):
    """Both required tables are created on first use."""
    await tempstore.set("key", make_buffer(b"data"))
    with sqlite3.connect(tempstore.database) as con:
        cur = con.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {row[0] for row in cur.fetchall()}
    assert "zarr" in tables
    assert "zarr_sqlitestore_metadata" in tables


@pytest.mark.asyncio
async def test_schema_not_null_constraints(tempstore):
    """Both k and v columns in both tables have NOT NULL."""
    await tempstore.set("key", make_buffer(b"data"))
    with sqlite3.connect(tempstore.database) as con:
        for table in ("zarr", "zarr_sqlitestore_metadata"):
            cur = con.execute(f"PRAGMA table_info({table})")
            for row in cur.fetchall():
                assert row[3] == 1, (
                    f"Column '{row[1]}' in table '{table}' must have NOT NULL"
                )


@pytest.mark.asyncio
async def test_schema_application_id(tempstore):
    """application_id is set to the spec value."""
    await tempstore.set("key", make_buffer(b"data"))
    with sqlite3.connect(tempstore.database) as con:
        cur = con.execute("PRAGMA application_id")
        assert cur.fetchone()[0] == _SQLITESTORE_APPLICATION_ID


@pytest.mark.asyncio
async def test_metadata_required_records(tempstore):
    """All required metadata records exist with correct values."""
    await tempstore.set("key", make_buffer(b"data"))
    with sqlite3.connect(tempstore.database) as con:
        cur = con.execute("SELECT k, v FROM zarr_sqlitestore_metadata")
        metadata = dict(cur.fetchall())
    assert metadata["sqlitestore_version"] == _SQLITESTORE_SPEC_VERSION
    assert metadata["compatible_flags"] == ""
    assert metadata["incompatible_flags"] == ""
    assert "created_by" in metadata
    assert "modified_at" in metadata


@pytest.mark.asyncio
async def test_metadata_created_by(tempstore):
    """created_by contains the package name."""
    await tempstore.set("key", make_buffer(b"data"))
    with sqlite3.connect(tempstore.database) as con:
        cur = con.execute("SELECT v FROM zarr_sqlitestore_metadata WHERE k = 'created_by'")
        created_by = cur.fetchone()[0]
    assert created_by.startswith("zarr-sqlite-python")


@pytest.mark.asyncio
async def test_metadata_modified_at(tempstore):
    """modified_at is a spec-conforming timestamp within the last 5 minutes."""

    await tempstore.set("key", make_buffer(b"data"))
    with sqlite3.connect(tempstore.database) as con:
        cur = con.execute("SELECT v FROM zarr_sqlitestore_metadata WHERE k = 'modified_at'")
        modified_at = cur.fetchone()[0]

    # Timestamp must be a valid RFC-3339 date-time timestamp, must be in UTC time zone,
    # must end in upper case "Z" and the date-time separator must be uppercase "T".
    TIMESTAMP_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$")
    assert TIMESTAMP_PATTERN.match(modified_at) is not None

    modified_at_parsed = datetime.datetime.fromisoformat(modified_at)
    assert modified_at_parsed.tzinfo == datetime.timezone.utc

    now = datetime.datetime.now(datetime.timezone.utc)
    assert now - datetime.timedelta(minutes=5) <= modified_at_parsed <= now

# ---------------------------------------------------------------------------
# Schema validation tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_validate_missing_zarr_table(tempstore):
    """Opening a file without the zarr table should fail."""
    await tempstore.set("key", make_buffer(b"data"))
    tempstore.close()

    with sqlite3.connect(tempstore.database, autocommit=True) as con:
        con.execute("DROP TABLE zarr")

    store = SQLiteStore(tempstore.database, read_only=True)
    with pytest.raises(ValueError, match="missing required table 'zarr'"):
        await store.exists("key")


@pytest.mark.asyncio
async def test_validate_missing_metadata_table(tempstore):
    """Opening a file without zarr_sqlitestore_metadata should fail."""
    await tempstore.set("key", make_buffer(b"data"))
    tempstore.close()

    with sqlite3.connect(tempstore.database, autocommit=True) as con:
        con.execute("DROP TABLE zarr_sqlitestore_metadata")

    store = SQLiteStore(tempstore.database, read_only=True)
    with pytest.raises(
        ValueError, match="missing required table 'zarr_sqlitestore_metadata'"
    ):
        await store.exists("key")


@pytest.mark.asyncio
async def test_validate_missing_metadata_record(tempstore):
    """Opening a file with a missing required metadata record should fail."""
    await tempstore.set("key", make_buffer(b"data"))
    tempstore.close()

    with sqlite3.connect(tempstore.database, autocommit=True) as con:
        con.execute("DELETE FROM zarr_sqlitestore_metadata WHERE k = 'sqlitestore_version'")

    store = SQLiteStore(tempstore.database, read_only=True)
    with pytest.raises(
        ValueError, match="missing required metadata entry 'sqlitestore_version'"
    ):
        await store.exists("key")


@pytest.mark.asyncio
async def test_validate_invalid_version(tempstore):
    """Opening a file with an invalid version string should fail."""
    await tempstore.set("key", make_buffer(b"data"))
    tempstore.close()

    with sqlite3.connect(tempstore.database, autocommit=True) as con:
        con.execute(
            "UPDATE zarr_sqlitestore_metadata SET v = 'invalid' WHERE k = 'sqlitestore_version'"
        )

    store = SQLiteStore(tempstore.database, read_only=True)
    with pytest.raises(ValueError, match="Invalid sqlitestore_version"):
        await store.exists("key")


@pytest.mark.asyncio
async def test_validate_unsupported_major_version(tempstore):
    """Opening a file with an unsupported major version should fail."""
    await tempstore.set("key", make_buffer(b"data"))
    tempstore.close()

    with sqlite3.connect(tempstore.database, autocommit=True) as con:
        con.execute(
            "UPDATE zarr_sqlitestore_metadata SET v = '2.0' WHERE k = 'sqlitestore_version'"
        )

    store = SQLiteStore(tempstore.database, read_only=True)
    with pytest.raises(ValueError, match="Unsupported sqlitestore_version"):
        await store.exists("key")


@pytest.mark.asyncio
async def test_validate_unknown_incompatible_flag(tempstore):
    """Opening a file with an unknown incompatible flag should fail."""
    await tempstore.set("key", make_buffer(b"data"))
    tempstore.close()

    with sqlite3.connect(tempstore.database, autocommit=True) as con:
        con.execute(
            "UPDATE zarr_sqlitestore_metadata SET v = 'unknown_flag' "
            "WHERE k = 'incompatible_flags'"
        )

    store = SQLiteStore(tempstore.database, read_only=True)
    with pytest.raises(
        ValueError, match="SQLiteStore flag 'unknown_flag' is not supported"
    ):
        await store.exists("key")


@pytest.mark.asyncio
async def test_read_only_valid_file(tempstore):
    """A valid file can be opened in read-only mode."""
    await tempstore.set("key", make_buffer(b"data"))
    tempstore.close()

    store = SQLiteStore(tempstore.database, read_only=True)
    assert await get_as_bytes(store, "key") == b"data"
    store.close()


@pytest.mark.asyncio
async def test_validate_wrong_application_id_writable(tempstore):
    """Opening a writable store with a wrong application_id should raise."""
    await tempstore.set("key", make_buffer(b"data"))
    tempstore.close()

    with sqlite3.connect(tempstore.database, autocommit=True) as con:
        con.execute("PRAGMA application_id = 0")

    store = SQLiteStore(tempstore.database, read_only=False)
    with pytest.raises(ValueError, match="Unexpected application_id"):
        await store.exists("key")
    store.close()


@pytest.mark.asyncio
async def test_validate_wrong_application_id_read_only(
    tempstore
):
    """Opening a read-only store with a wrong application_id should warn."""
    await tempstore.set("key", make_buffer(b"data"))
    tempstore.close()

    with sqlite3.connect(tempstore.database, autocommit=True) as con:
        con.execute("PRAGMA application_id = 0")

    store = SQLiteStore(tempstore.database, read_only=True)
    with pytest.warns(UserWarning, match="Unexpected application_id"):
        assert await get_as_bytes(store, "key") == b"data"
    store.close()

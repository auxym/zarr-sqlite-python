"""Tests for SQL queries documented in the SPEC.md appendix.

Each test exercises the SQL statements from the non-normative appendix
of SPEC.md, verifying that they produce the expected results against a
real SQLite database.
"""

from datetime import datetime, timezone, timedelta
import re
import sqlite3
from tempfile import NamedTemporaryFile

import pytest

_APP_ID = 0x10B50760


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _prefix_bounds(prefix: str) -> tuple[str, str] | None:
    """Compute (prefix, upper) for prefix searches per SPEC appendix.

    :prefix must end with '/'. :upper is obtained by replacing the
    trailing '/' with '0' (the next character after '/' in ASCII).
    For the empty prefix, returns None (no WHERE clause needed).
    """
    assert prefix == "" or prefix.endswith("/")
    if not prefix:
        return None
    upper = prefix[:-1] + "0"
    return prefix, upper


def _list_dir_query(prefix: str) -> tuple[str, dict]:
    """Build the list_dir CTE query (SPEC appendix) and bound parameters.

    For the empty prefix, the WHERE clause is omitted entirely.
    """
    if prefix == "":
        sql = """
            WITH matches AS (
                SELECT k, substr(k, 1) AS rest
                FROM zarr
            )
            SELECT 'key' AS type, k AS path
            FROM matches
            WHERE instr(rest, '/') = 0
            UNION
            SELECT DISTINCT 'prefix' AS type,
                substr(rest, 1, instr(rest, '/')) AS path
            FROM matches
            WHERE instr(rest, '/') > 0
            ORDER BY path
        """
        return sql, {}

    upper = prefix[:-1] + "0"
    sql = """
    WITH matches AS (
        SELECT
            k,
            substr(k, length(:prefix) + 1) AS rest
        FROM zarr
        WHERE k > :prefix
        AND k < :upper
    )
    SELECT
        'key' AS type,
        k AS path
    FROM matches
    WHERE instr(rest, '/') = 0

    UNION

    SELECT DISTINCT
        'prefix' AS type,
        :prefix || substr(rest, 1, instr(rest, '/')) AS path
    FROM matches
    WHERE instr(rest, '/') > 0

    ORDER BY path;
    """
    return sql, {"prefix": prefix, "upper": upper}


def _set(con: sqlite3.Connection, key: str, value: bytes) -> None:
    """Insert or replace using the exact SQL from the SPEC appendix 'set' section."""
    con.execute(
        "INSERT INTO zarr(k, v) VALUES (:key, :value) "
        "ON CONFLICT(k) DO UPDATE SET v = excluded.v",
        {"key": key, "value": value},
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def fresh_db():
    """Create a fresh, empty SQLite database file for raw SQL testing."""
    with NamedTemporaryFile(suffix=".db", delete=True, delete_on_close=False) as fp:
        fp.close()
        with sqlite3.connect(fp.name, autocommit=True) as con:
            yield con


@pytest.fixture
def store_db():
    """Create a fresh SQLite database with the full store schema.

    Creates both required tables, sets the application_id pragma, and
    inserts the required metadata records, exactly as documented in the
    SPEC appendix.
    """
    with NamedTemporaryFile(suffix=".zarrdb", delete=True, delete_on_close=False) as fp:
        fp.close()
        with sqlite3.connect(fp.name, autocommit=True) as con:
            con.execute(f"PRAGMA application_id = {hex(_APP_ID)}")
            con.execute(
                "CREATE TABLE IF NOT EXISTS zarr_sqlitestore_metadata("
                "k TEXT PRIMARY KEY NOT NULL, v TEXT NOT NULL)"
            )
            con.execute(
                "CREATE TABLE IF NOT EXISTS zarr(k TEXT PRIMARY KEY NOT NULL, v BLOB NOT NULL)"
            )
            con.execute(
                "INSERT OR IGNORE INTO zarr_sqlitestore_metadata(k, v) VALUES "
                "('sqlitestore_version', '1.0'),"
                "('compatible_flags', ''),"
                "('incompatible_flags', '')"
            )
            yield con


# ---------------------------------------------------------------------------
# Schema creation
# ---------------------------------------------------------------------------


def test_spec_create_schema_tables(fresh_db):
    """CREATE TABLE IF NOT EXISTS creates both required tables."""
    con = fresh_db
    con.execute(
        "CREATE TABLE IF NOT EXISTS zarr_sqlitestore_metadata("
        "k TEXT PRIMARY KEY NOT NULL, v TEXT NOT NULL)"
    )
    con.execute(
        "CREATE TABLE IF NOT EXISTS zarr(k TEXT PRIMARY KEY NOT NULL, v BLOB NOT NULL)"
    )

    tables = {
        row[0]
        for row in con.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    assert {"zarr", "zarr_sqlitestore_metadata"} <= tables


def test_spec_table_schemas(fresh_db):
    """Both tables have the column types and NOT NULL constraints from the SPEC."""
    con = fresh_db
    con.execute(
        "CREATE TABLE IF NOT EXISTS zarr_sqlitestore_metadata("
        "k TEXT PRIMARY KEY NOT NULL, v TEXT NOT NULL)"
    )
    con.execute(
        "CREATE TABLE IF NOT EXISTS zarr(k TEXT PRIMARY KEY NOT NULL, v BLOB NOT NULL)"
    )

    zarr_cols = con.execute("PRAGMA table_info(zarr)").fetchall()
    assert zarr_cols[0][1] == "k"
    assert zarr_cols[0][2].upper() == "TEXT"
    assert zarr_cols[0][3] == 1  # NOT NULL
    assert zarr_cols[1][1] == "v"
    assert zarr_cols[1][2].upper() == "BLOB"
    assert zarr_cols[1][3] == 1  # NOT NULL

    meta_cols = con.execute("PRAGMA table_info(zarr_sqlitestore_metadata)").fetchall()
    assert meta_cols[0][1] == "k"
    assert meta_cols[0][2].upper() == "TEXT"
    assert meta_cols[1][1] == "v"
    assert meta_cols[1][2].upper() == "TEXT"


def test_spec_application_id(fresh_db):
    """PRAGMA application_id sets the expected identifier."""
    con = fresh_db
    con.execute(f"PRAGMA application_id = {hex(_APP_ID)}")
    assert con.execute("PRAGMA application_id").fetchone()[0] == _APP_ID


# ---------------------------------------------------------------------------
# Metadata
# ---------------------------------------------------------------------------


def test_spec_metadata_required_records(fresh_db):
    """The required metadata records can be inserted via the SPEC SQL."""
    con = fresh_db
    con.execute(
        "CREATE TABLE IF NOT EXISTS zarr_sqlitestore_metadata("
        "k TEXT PRIMARY KEY NOT NULL, v TEXT NOT NULL)"
    )
    con.execute(
        """
        INSERT OR IGNORE INTO zarr_sqlitestore_metadata(k, v) VALUES
        ('sqlitestore_version', '1.0'),
        ('compatible_flags', ''),
        ('incompatible_flags', ''),
        ('created_by', 'Some Writer v2.3')
        """
    )
    rows = dict(con.execute("SELECT k, v FROM zarr_sqlitestore_metadata").fetchall())
    assert rows["sqlitestore_version"] == "1.0"
    assert rows["compatible_flags"] == ""
    assert rows["incompatible_flags"] == ""


def test_spec_modified_at_update(fresh_db):
    """modified_at can be updated via ON CONFLICT DO UPDATE."""
    con = fresh_db
    con.execute(
        "CREATE TABLE IF NOT EXISTS zarr_sqlitestore_metadata("
        "k TEXT PRIMARY KEY NOT NULL, v TEXT NOT NULL)"
    )
    con.execute(
        "INSERT INTO zarr_sqlitestore_metadata(k, v) "
        "VALUES ('modified_at', strftime('%Y-%m-%dT%H:%M:%fZ', '1999-12-31 01:00:00')) "
        "ON CONFLICT(k) DO UPDATE SET v = excluded.v"
    )
    t1 = con.execute(
        "SELECT v FROM zarr_sqlitestore_metadata WHERE k='modified_at'"
    ).fetchone()[0]
    con.execute(
        "INSERT INTO zarr_sqlitestore_metadata(k, v) "
        "VALUES ('modified_at', strftime('%Y-%m-%dT%H:%M:%fZ', 'now', 'utc')) "
        "ON CONFLICT(k) DO UPDATE SET v = excluded.v"
    )
    t2 = con.execute(
        "SELECT v FROM zarr_sqlitestore_metadata WHERE k='modified_at'"
    ).fetchone()[0]
    count = con.execute(
        "SELECT COUNT(*) FROM zarr_sqlitestore_metadata WHERE k = 'modified_at'"
    ).fetchone()[0]
    assert count == 1

    TIMESTAMP_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$")
    assert TIMESTAMP_PATTERN.match(t2) is not None

    # Make sure we did overwrite the timestamp and that its parseable
    assert datetime.fromisoformat(t2) > datetime.fromisoformat(t1)
    assert datetime.fromisoformat(t2).tzinfo == timezone.utc

    assert (
        timedelta(seconds=0)
        < datetime.now(tz=timezone.utc) - datetime.fromisoformat(t2)
        < timedelta(seconds=1)
    )


# ---------------------------------------------------------------------------
# get
# ---------------------------------------------------------------------------


def test_spec_get_existing_key(store_db):
    """The get query returns the value for an existing key."""
    con = store_db
    _set(con, "foo", b"hello")
    row = con.execute("SELECT v FROM zarr WHERE k = :key", {"key": "foo"}).fetchone()
    assert row == (b"hello",)


def test_spec_get_nonexistent_key(store_db):
    """The get query returns no row when the key doesn't exist."""
    con = store_db
    _set(con, "foo", b"hello")
    row = con.execute(
        "SELECT v FROM zarr WHERE k = :key", {"key": "missing"}
    ).fetchone()
    assert row is None


# ---------------------------------------------------------------------------
# get_partial_values
# ---------------------------------------------------------------------------


def test_spec_get_partial_values_offset(store_db):
    """The substr-based partial read matches OffsetByteRequest semantics."""
    con = store_db
    _set(con, "k", b"abcdefghij")
    # OffsetByteRequest(3) -> offset=3, length=7 -> "defghij"
    row = con.execute(
        "SELECT substr(v, :offset + 1, :length) FROM zarr WHERE k = :key",
        {"offset": 3, "length": 7, "key": "k"},
    ).fetchone()
    assert row[0] == b"defghij"


# ---------------------------------------------------------------------------
# set
# ---------------------------------------------------------------------------


def test_spec_set_insert(store_db):
    """The set query inserts a new key/value pair."""
    con = store_db
    _set(con, "key1", b"value1")
    row = con.execute("SELECT v FROM zarr WHERE k = :key", {"key": "key1"}).fetchone()
    assert row == (b"value1",)

    _set(con, "key1", b"second")
    row = con.execute("SELECT v FROM zarr WHERE k = :key", {"key": "key1"}).fetchone()
    assert row == (b"second",)


# ---------------------------------------------------------------------------
# erase
# ---------------------------------------------------------------------------


def test_spec_erase_existing_key(store_db):
    """The erase query deletes a single existing key."""
    con = store_db
    _set(con, "a", b"1")
    _set(con, "b", b"2")
    con.execute("DELETE FROM zarr WHERE k = :key", {"key": "a"})
    rows = sorted(r[0] for r in con.execute("SELECT k FROM zarr").fetchall())
    assert rows == ["b"]


def test_spec_erase_missing_key(store_db):
    """The erase query is a no-op when the key doesn't exist."""
    con = store_db
    _set(con, "a", b"1")
    con.execute("DELETE FROM zarr WHERE k = :key", {"key": "missing"})
    rows = con.execute("SELECT k FROM zarr").fetchall()
    assert len(rows) == 1


# ---------------------------------------------------------------------------
# erase_values
# ---------------------------------------------------------------------------


def test_spec_erase_values_multiple(store_db):
    """The erase_values query deletes multiple keys via IN clause."""
    con = store_db
    for k, v in [("a", b"1"), ("b", b"2"), ("c", b"3"), ("d", b"4")]:
        _set(con, k, v)
    con.execute("DELETE FROM zarr WHERE k IN (:k1, :k2)", {"k1": "a", "k2": "c"})
    rows = sorted(r[0] for r in con.execute("SELECT k FROM zarr").fetchall())
    assert rows == ["b", "d"]

# ---------------------------------------------------------------------------
# erase_prefix
# ---------------------------------------------------------------------------


def test_spec_erase_prefix(store_db):
    """The erase_prefix query deletes all keys with a given prefix."""
    con = store_db
    for k in ["a/1", "a/2", "b/1", "leaf"]:
        _set(con, k, b"x")

    prefix, upper = _prefix_bounds("a/")
    con.execute(
        "DELETE FROM zarr WHERE k > :prefix AND k < :upper",
        {"prefix": prefix, "upper": upper},
    )
    rows = {r[0] for r in con.execute("SELECT k FROM zarr").fetchall()}
    assert rows == {"b/1", "leaf"}


def test_spec_erase_prefix_deep(store_db):
    """erase_prefix deletes keys at any depth under the prefix."""
    con = store_db
    for k in ["a/1", "a/sub/deep/x", "a/2", "b/1"]:
        _set(con, k, b"x")

    prefix, upper = _prefix_bounds("a/")
    con.execute(
        "DELETE FROM zarr WHERE k > :prefix AND k < :upper",
        {"prefix": prefix, "upper": upper},
    )
    rows = {r[0] for r in con.execute("SELECT k FROM zarr").fetchall()}
    assert rows == {"b/1"}


def test_spec_erase_prefix_empty(store_db):
    """erase_prefix with empty prefix deletes all keys (DELETE FROM zarr)."""
    con = store_db
    for k in ["a/1", "b/2", "leaf"]:
        _set(con, k, b"x")
    con.execute("DELETE FROM zarr")
    rows = con.execute("SELECT k FROM zarr").fetchall()
    assert rows == []


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------


def test_spec_list(store_db):
    """The list query returns all keys in the store."""
    con = store_db
    for k, v in [("a", b"1"), ("b/c", b"2"), ("b/d", b"3")]:
        _set(con, k, v)
    rows = sorted(r[0] for r in con.execute("SELECT k FROM zarr").fetchall())
    assert rows == ["a", "b/c", "b/d"]


def test_spec_list_empty(store_db):
    """The list query returns no rows from an empty store."""
    con = store_db
    rows = con.execute("SELECT k FROM zarr").fetchall()
    assert rows == []


# ---------------------------------------------------------------------------
# list_prefix
# ---------------------------------------------------------------------------


def test_spec_list_prefix(store_db):
    """The list_prefix query returns keys with a matching prefix."""
    con = store_db
    for k in ["a/1", "a/2", "ab/3", "b/4"]:
        _set(con, k, b"x")

    prefix, upper = _prefix_bounds("a/")
    rows = sorted(
        r[0]
        for r in con.execute(
            "SELECT k FROM zarr WHERE k > :prefix AND k < :upper",
            {"prefix": prefix, "upper": upper},
        ).fetchall()
    )
    assert rows == ["a/1", "a/2"]


def test_spec_list_prefix_no_match(store_db):
    """list_prefix returns no rows when the prefix doesn't match."""
    con = store_db
    _set(con, "a/1", b"x")

    prefix, upper = _prefix_bounds("zzz/")
    rows = con.execute(
        "SELECT k FROM zarr WHERE k > :prefix AND k < :upper",
        {"prefix": prefix, "upper": upper},
    ).fetchall()
    assert rows == []


def test_spec_list_prefix_deep(store_db):
    """list_prefix returns keys at any depth under the prefix."""
    con = store_db
    for k in ["a/1", "a/sub/x", "a/sub/y", "b/1"]:
        _set(con, k, b"x")

    prefix, upper = _prefix_bounds("a/")
    rows = sorted(
        r[0]
        for r in con.execute(
            "SELECT k FROM zarr WHERE k > :prefix AND k < :upper",
            {"prefix": prefix, "upper": upper},
        ).fetchall()
    )
    assert rows == ["a/1", "a/sub/x", "a/sub/y"]


# ---------------------------------------------------------------------------
# list_dir
# ---------------------------------------------------------------------------


def test_spec_list_dir_root(store_db):
    """The list_dir CTE query returns keys and child prefixes at root."""
    con = store_db
    for k in ["a/1", "b/2", "c/d/3", "leaf"]:
        _set(con, k, b"x")

    sql, params = _list_dir_query("")
    rows = con.execute(sql, params).fetchall()
    assert rows == [
        ("prefix", "a/"),
        ("prefix", "b/"),
        ("prefix", "c/"),
        ("key", "leaf"),
    ]


def test_spec_list_dir_nested(store_db):
    """The list_dir CTE query returns keys and child prefixes at a nested level."""
    con = store_db
    for k in ["a/x", "a/y", "a/sub/z", "b/1"]:
        _set(con, k, b"x")

    sql, params = _list_dir_query("a/")
    rows = con.execute(sql, params).fetchall()
    assert rows == [
        ("prefix", "a/sub/"),
        ("key", "a/x"),
        ("key", "a/y"),
    ]


def test_spec_list_dir_no_match(store_db):
    """The list_dir query returns no rows for a non-matching prefix."""
    con = store_db
    _set(con, "a/1", b"x")

    sql, params = _list_dir_query("zzz/")
    rows = con.execute(sql, params).fetchall()
    assert rows == []


def test_spec_list_dir_only_key(store_db):
    """list_dir at root with only a leaf key returns just that key."""
    con = store_db
    for k in ["leaf", "a/1"]:
        _set(con, k, b"x")

    sql, params = _list_dir_query("")
    rows = con.execute(sql, params).fetchall()
    assert rows == [
        ("prefix", "a/"),
        ("key", "leaf"),
    ]


def test_spec_list_dir_deep_nested(store_db):
    """list_dir correctly identifies child prefixes at multiple levels."""
    con = store_db
    for k in ["a/x", "a/sub/w", "a/sub/y", "a/sub/deep/z"]:
        _set(con, k, b"x")

    sql, params = _list_dir_query("a/")
    rows = con.execute(sql, params).fetchall()
    assert rows == [
        ("prefix", "a/sub/"),
        ("key", "a/x"),
    ]


# ---------------------------------------------------------------------------
# Timestamp operations (SPEC appendix: Other timestamp operations)
# ---------------------------------------------------------------------------


def test_spec_timestamp_custom_format(store_db):
    """strftime returns the timestamp formatted as MM/DD/YYYY HH:MM."""
    con = store_db
    con.execute(
        "INSERT INTO zarr_sqlitestore_metadata(k, v) "
        "VALUES ('modified_at', '1999-12-31T23:59:59.999Z')"
    )
    row = con.execute(
        "SELECT strftime('%m/%d/%Y %H:%M', v) "
        "FROM zarr_sqlitestore_metadata "
        "WHERE k = 'modified_at'"
    ).fetchone()
    assert row[0] == "12/31/1999 23:59"


def test_spec_timestamp_elapsed_seconds(store_db):
    """unixepoch difference returns elapsed seconds since modified_at."""
    con = store_db
    con.execute(
        "INSERT INTO zarr_sqlitestore_metadata(k, v) "
        "VALUES ('modified_at', '1999-12-31T23:59:59.999Z')"
    )
    row = con.execute(
        "SELECT unixepoch('now') - unixepoch(v) "
        "FROM zarr_sqlitestore_metadata "
        "WHERE k = 'modified_at'"
    ).fetchone()
    assert row[0] is not None
    assert row[0] > 0
    expected = (
        datetime.now(tz=timezone.utc)
        - datetime(1999, 12, 31, 23, 59, 59, 999000, tzinfo=timezone.utc)
    ).total_seconds()
    assert abs(row[0] - expected) < 5


def test_spec_timestamp_elapsed_days(store_db):
    """julianday difference returns elapsed days as a fractional number."""
    con = store_db
    con.execute(
        "INSERT INTO zarr_sqlitestore_metadata(k, v) "
        "VALUES ('modified_at', '1999-12-31T23:59:59.999Z')"
    )
    row = con.execute(
        "SELECT julianday('now') - julianday(v) "
        "FROM zarr_sqlitestore_metadata "
        "WHERE k = 'modified_at'"
    ).fetchone()
    assert row[0] is not None
    assert row[0] > 0
    expected = (
        datetime.now(tz=timezone.utc)
        - datetime(1999, 12, 31, 23, 59, 59, 999000, tzinfo=timezone.utc)
    ).total_seconds() / 86400
    assert abs(row[0] - expected) < 0.01

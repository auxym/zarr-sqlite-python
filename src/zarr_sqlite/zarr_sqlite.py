from typing import override, Self, Sequence
from collections.abc import Iterable, AsyncIterator, AsyncGenerator
from contextlib import asynccontextmanager
import datetime
import sqlite3
import warnings
from pathlib import Path
import urllib.parse
import asyncio

from .async_sqlite import Connection, connect

from zarr.core.buffer import BufferPrototype, Buffer

from zarr.abc.store import (
    ByteRequest,
    OffsetByteRequest,
    RangeByteRequest,
    Store,
    SuffixByteRequest,
)

from ._version import __version__
from ._db_utils import is_database_uri, is_in_memory_database

if sqlite3.sqlite_version_info < (3, 17, 7):
    raise ValueError(
        f"Unsupported SQLite version {sqlite3.sqlite_version}.The minimum supported version of sqlite is 3.17.7."
    )

_SQLITESTORE_SPEC_VERSION = "1.0"  # spec version we adhere to
_SQLITESTORE_APPLICATION_ID = 0x10B50760
_CREATED_BY = f"zarr-sqlite-python v{__version__}"


def _validate_key(key: str):
    """Validates a key according to SQLiteStore specification

    From the Zarr core spec:
    - a key is a Unicode string, where the final character is not a `/` character.

    Additional checks (not in the core spec):
    - a key which starts with '/' is invalid, and
    - a key that contains '//' is invalid.

    The empty string is a valid key: it addresses a store's root resource as a single blob.
    """
    if key.startswith("/") or key.endswith("/") or "//" in key:
        raise ValueError(f"Invalid key '{key}'")


class SQLiteStore(Store):
    """
    Store for the local file system.

    Parameters
    ----------
    database : str or Path
        Directory to use as root of store.
    read_only : bool
        Whether the store is read-only

    Attributes
    ----------
    supports_writes
    supports_deletes
    supports_partial_writes
    supports_listing
    root
    """

    _database: str
    _is_open: bool
    _open_lock: asyncio.Lock
    _conn: Connection | None
    _transaction_lock: asyncio.Lock
    _journal_mode: str | None
    _page_size: int

    @property
    @override
    def supports_writes(self) -> bool:
        return True

    @property
    @override
    def supports_deletes(self) -> bool:
        return True

    @property
    @override
    def supports_listing(self) -> bool:
        return True

    def __init__(
        self,
        database: str | Path,
        *,
        read_only: bool = False,
        journal_mode: str | None = "WAL",
        page_size: int = 4096,
    ) -> None:
        super().__init__(read_only=read_only)
        self._database = str(database)
        self._journal_mode = journal_mode
        self._page_size = page_size
        self._conn = None

        self._open_lock = asyncio.Lock()
        self._transaction_lock = asyncio.Lock()

        if self.is_in_memory() and read_only:
            raise ValueError("In-memory databases cannot be opened read-only")

        # To open a database in read-only mode in python, we need to use
        # a SQLite URI.
        if read_only:
            self._database = self._database_as_uri(self.database, read_only)

    @property
    def database(self):
        return self._database

    def is_in_memory(self) -> bool:
        return is_in_memory_database(self._database)

    @staticmethod
    def _database_as_uri(database: str, read_only: bool) -> str:
        """Create a sqlite-compatible database URI.

        Parameters
        ----------
        database : str
            File path or  sqlite-compatible URI (must have `file:` scheme). If
            `database` is a URI, the parameters will be kept unmodified, except
            "mode", which will always be overwritten or added base on the value
            of `read_only`.
        read_only : bool
            Whether the store is read-only
        """

        if not is_database_uri(database):
            database = Path(database).absolute().as_uri()
        uri_parts = urllib.parse.urlsplit(database)

        query = urllib.parse.parse_qs(uri_parts.query)
        mode = "ro" if read_only else "rwc"
        query["mode"] = [mode]

        return urllib.parse.urlunsplit(
            urllib.parse.SplitResult(
                scheme=uri_parts.scheme,
                netloc="",
                path=uri_parts.path,
                query=urllib.parse.urlencode(query, doseq=True),
                fragment="",
            )
        )

    @override
    async def _ensure_open(self):
        if self._is_open:
            return

        async with self._open_lock:
            if self._is_open:
                return
            await self._open()

    @asynccontextmanager
    async def _transaction(self) -> AsyncGenerator[None, None]:
        """Context manager for write transactions.

        Acquires the transaction lock to serialize write operations on the
        single SQLite connection. Commits on success, rolls back on error.
        """
        assert self._conn is not None
        async with self._transaction_lock:
            try:
                yield
                await self._conn.commit()
            except BaseException:
                await self._conn.rollback()
                raise

    @override
    async def _open(self) -> None:
        if self._is_open:
            raise ValueError("store is already open")

        if not self.read_only and self._journal_mode is not None:
            # PRAGMA journal_mode cannot be changed within a transaction.  We
            # need to set autocommit=True but our connection does not support
            # changing autocommit on an open connection.  Open a temporary
            # connection with autocommit=True mode to set journal_mode.
            if self._journal_mode not in {"DELETE", "WAL"}:
                raise ValueError(f"Invalid journal_mode: {self._journal_mode}")
            tmp_conn = await connect(
                self._database, uri=is_database_uri(self._database), autocommit=True
            )
            try:
                await tmp_conn.execute("PRAGMA journal_mode=" + self._journal_mode)
            finally:
                await tmp_conn.close()

        self._conn = await connect(
            self._database,
            uri=is_database_uri(self._database),
            autocommit=False,
        )

        try:
            # Create schema only on empty file to avoid clobbering an existing file.
            if not self._read_only and await self._db_is_empty():
                async with self._transaction():
                    await self._create_schema()

            await self._validate_schema()
        except Exception:
            # Clean-up otherwise the worker thread hangs
            self._conn.stop(join=True)
            self._conn = None
            raise

        self._is_open = True

    @override
    def with_read_only(self, read_only: bool = False) -> Self:
        if self.is_in_memory():
            raise ValueError("Cannot create a read-only view of an in-memory database.")
        return type(self)(self.database, read_only=read_only)

    async def _create_schema(self) -> None:
        assert self._conn is not None
        await self._conn.execute("PRAGMA page_size=" + str(int(self._page_size)))
        await self._conn.execute(
            "CREATE TABLE IF NOT EXISTS sqlitestore_metadata("
            "k TEXT PRIMARY KEY NOT NULL, v TEXT NOT NULL)"
        )
        await self._conn.execute(
            "CREATE TABLE IF NOT EXISTS zarr("
            "k TEXT PRIMARY KEY NOT NULL, v BLOB NOT NULL)"
        )
        await self._conn.execute(
            "PRAGMA application_id = " + str(int(_SQLITESTORE_APPLICATION_ID))
        )
        await self._conn.executemany(
            "INSERT OR IGNORE INTO sqlitestore_metadata(k, v) VALUES (?, ?)",
            [
                ("sqlitestore_version", _SQLITESTORE_SPEC_VERSION),
                ("compatible_flags", ""),
                ("incompatible_flags", ""),
                ("created_by", _CREATED_BY),
            ],
        )
        await self._update_timestamp()

    async def _update_timestamp(self) -> None:
        assert self._conn is not None
        now = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")
        await self._conn.execute(
            "INSERT INTO sqlitestore_metadata(k, v) VALUES (?, ?) "
            "ON CONFLICT(k) DO UPDATE SET v = excluded.v",
            ("created_time", now),
        )

    async def _db_is_empty(self) -> bool:
        assert self._conn is not None
        row = await self._conn.fetchone(
            """
            SELECT EXISTS (
                SELECT 1 FROM sqlite_schema
                WHERE type IN ('table', 'view', 'index', 'trigger')
                AND name NOT LIKE 'sqlite_%'
            )
            """
        )
        assert row is not None
        return row[0] == 0

    async def _validate_schema(self) -> None:
        metadata_keys = (
            "sqlitestore_version",
            "compatible_flags",
            "incompatible_flags",
        )

        assert self._conn is not None
        for required_table in ("zarr", "sqlitestore_metadata"):
            rows = await self._conn.fetchall(
                "PRAGMA table_info(" + required_table + ")"
            )
            if not rows:
                raise ValueError(
                    f"Invalid SQLiteStore file: missing required table '{required_table}'"
                )

        app_id_row = await self._conn.fetchone("PRAGMA application_id")
        assert app_id_row is not None
        app_id = app_id_row[0]

        if app_id != _SQLITESTORE_APPLICATION_ID:
            if self.read_only:
                warnings.warn(
                    f"Unexpected application_id for SQLiteStore: {app_id:#x} "
                    f"(expected {_SQLITESTORE_APPLICATION_ID:#x}). "
                    "This may not be a valid Zarr SQLiteStore file. Attempting to open it anyway.",
                    stacklevel=2,
                )
            else:
                # For writable files, we don't want to make modifications to a non-sqlitestore
                # file that may have been opened by error, so refuse to open it.
                raise ValueError(
                    f"Invalid SQLiteStore file: "
                    f"Unexpected application_id for SQLiteStore {app_id:#x} "
                    f"(expected {_SQLITESTORE_APPLICATION_ID:#x})."
                )

        metadata_rows = await self._conn.fetchall(
            "SELECT k, v FROM sqlitestore_metadata WHERE k IN (?, ?, ?)",
            metadata_keys,
        )

        metadata: dict[str, str] = (
            {k: v for k, v in metadata_rows} if metadata_rows else {}
        )
        for k in metadata_keys:
            if k not in metadata:
                raise ValueError(
                    f"Invalid SQLiteStore file: missing required metadata entry '{k}'"
                )

        version_str = metadata["sqlitestore_version"]
        try:
            version = [int(n) for n in version_str.strip().split(".")]
        except ValueError:
            raise ValueError(f"Invalid sqlitestore_version string: '{version_str}'")
        if len(version) != 2:
            raise ValueError(f"Invalid sqlitestore_version string: '{version_str}'")
        if version[0] > 1:
            raise ValueError(f"Unsupported sqlitestore_version: {version_str}")

        incompat_flags = [
            f.strip()
            for f in metadata["incompatible_flags"].split(",")
            if len(f.strip()) > 0
        ]
        if len(incompat_flags) > 0:
            raise ValueError(f"SQLiteStore flag '{incompat_flags[0]}' is not supported")

    @override
    def close(self) -> None:
        """Immediately close the store and the SQLite connection."""
        if self._conn is not None:
            self._conn.stop(join=True)
            self._conn = None
        self._is_open = False

    @staticmethod
    def _get_text_boundaries(prefix: str) -> tuple[str, str]:
        """Compute a pair of (lower bound, upper bound) strings.

        These strings are used to achieve efficient prefix search with an SQL
        clause like "(...) WHERE k < :lower_bound AND k > :upper_bound".

        The upper bound string is obtained by replacing the trailing slash (/)
        character in a prefix with a zero (0) character, which is the next
        character is lexicographical order (in ASCII and Unicode).

        We do not use GLOB or LIKE in prefix searches because they have many
        issues in SQLite. For example, LIKE does not support case-sensitive
        matching (required for zarr keys), and GLOB does not support escaping
        the special glob characters (*, [, ] and ?).

        IMPORTANT: this does not work for the empty prefix "", which is considered
        valid.
        """
        assert prefix != ""

        is_valid = not (prefix.startswith("/") or "//" in prefix)
        if not is_valid:
            raise ValueError(f"Invalid prefix '{prefix}'")
        if prefix != "" and not prefix.endswith("/"):
            prefix += "/"

        upper = prefix[:-1] + "0"
        return (prefix, upper)

    @override
    async def is_empty(self, prefix: str) -> bool:
        await self._ensure_open()
        assert self._conn is not None

        params: Sequence[str] = ()
        if prefix == "":
            sql = "SELECT EXISTS(SELECT 1 FROM zarr LIMIT 1)"
        else:
            params = self._get_text_boundaries(prefix)
            sql = "SELECT EXISTS(SELECT 1 FROM zarr WHERE k > ? AND k < ? LIMIT 1)"

        row = await self._conn.fetchone(sql, params)
        return row is not None and row[0] == 0

    @override
    async def clear(self) -> None:
        """Clear the store."""
        self._check_writable()
        await self._ensure_open()
        assert self._conn is not None
        async with self._transaction():
            await self._conn.execute("DROP TABLE IF EXISTS zarr")
            await self._create_schema()

    @override
    def __str__(self) -> str:
        return repr(self)

    @override
    def __repr__(self) -> str:
        return f"SQLiteStore('{self._database}')"

    @override
    def __eq__(self, other: object) -> bool:
        """Equality comparison."""
        if not isinstance(other, type(self)):
            return False

        if self.is_in_memory() or other.is_in_memory():
            return False

        return self.database == other.database

    @override
    async def get(
        self,
        key: str,
        prototype: BufferPrototype,
        byte_range: ByteRequest | None = None,
    ) -> Buffer | None:
        _validate_key(key)
        await self._ensure_open()
        assert self._conn is not None

        row = await self._conn.fetchone("SELECT v FROM zarr WHERE k = ?", (key,))

        if row is None:
            return None
        data = row[0]
        if not isinstance(data, bytes):
            return None

        if byte_range is not None:
            # The connection does not expose the SQLite blob API, so we read the
            # full blob and apply the byte range as a Python slice.
            blob_len = len(data)
            match byte_range:
                case OffsetByteRequest(offset=o):
                    start = min(o, blob_len)
                    data = data[start:]
                case RangeByteRequest(start=s, end=e):
                    start = max(0, s)
                    end_clamped = min(blob_len, max(0, e))
                    data = data[start:end_clamped]
                case SuffixByteRequest(suffix=s):
                    start = max(0, blob_len - s)
                    data = data[start:]
                case _:
                    raise ValueError(f"Unsupported byte range type: {type(byte_range)}")

        return prototype.buffer.from_bytes(data)

    @override
    async def get_partial_values(
        self,
        prototype: BufferPrototype,
        key_ranges: Iterable[tuple[str, ByteRequest | None]],
    ) -> list[Buffer | None]:
        return await asyncio.gather(
            *[self.get(key, prototype, byte_range) for key, byte_range in key_ranges]
        )

    @override
    async def exists(self, key: str) -> bool:
        _validate_key(key)
        await self._ensure_open()
        assert self._conn is not None
        row = await self._conn.fetchone("SELECT v FROM zarr WHERE k = ?", (key,))
        return row is not None

    @override
    async def set(self, key: str, value: Buffer) -> None:
        self._check_writable()
        _validate_key(key)
        await self._ensure_open()
        assert self._conn is not None
        async with self._transaction():
            await self._conn.execute(
                "INSERT OR REPLACE INTO zarr (k, v) VALUES (?, ?)",
                (key, value.to_bytes()),
            )

    @override
    async def set_if_not_exists(self, key: str, value: Buffer) -> None:
        self._check_writable()
        _validate_key(key)
        await self._ensure_open()
        assert self._conn is not None
        async with self._transaction():
            await self._conn.execute(
                "INSERT OR IGNORE INTO zarr (k, v) VALUES (?, ?)",
                (key, value.to_bytes()),
            )

    @override
    async def delete(self, key: str) -> None:
        self._check_writable()
        await self._ensure_open()
        assert self._conn is not None
        async with self._transaction():
            await self._conn.execute("DELETE FROM zarr WHERE k = ?", (key,))

    @override
    async def list(self) -> AsyncIterator[str]:
        await self._ensure_open()
        assert self._conn is not None
        async for row in self._conn.fetch_iter("SELECT k FROM zarr"):
            yield str(row[0])

    @override
    async def list_prefix(self, prefix: str) -> AsyncIterator[str]:
        await self._ensure_open()
        assert self._conn is not None
        params: Sequence[str] = ()
        if prefix == "":
            sql = "SELECT k FROM zarr"
        else:
            params = self._get_text_boundaries(prefix)
            sql = "SELECT k FROM zarr WHERE k > ? AND k < ?"
        async for row in self._conn.fetch_iter(sql, params):
            yield str(row[0])

    @override
    async def list_dir(self, prefix: str) -> AsyncIterator[str]:
        # Even though prefix will be normalized in list_prefix(), this is
        # required for the removeprefix(prefix) call used below.
        if prefix != "" and not prefix[-1] == "/":
            prefix += "/"

        # _ensure_open is called in list_prefix()
        seen: set[str] = set()
        async for full_key in self.list_prefix(prefix):
            rel_key = full_key.removeprefix(prefix)
            parts = rel_key.split("/")
            k = parts[0]
            if len(parts) > 1:
                k = k + "/"
            if k not in seen:
                seen.add(k)
                yield k

    @override
    async def delete_dir(self, prefix: str) -> None:
        self._check_writable()
        await self._ensure_open()

        async with self._transaction():
            assert self._conn is not None
            # Check if key exists
            row = await self._conn.fetchone(
                "SELECT v FROM zarr WHERE k = ?", (prefix.rstrip("/"),)
            )
            if row is not None:
                raise ValueError(
                    f"Cannot delete directory {prefix} as it is a key in the store."
                )

            params: Sequence[str] = ()
            if prefix == "":
                sql = "DELETE FROM zarr"
            else:
                params = self._get_text_boundaries(prefix)
                sql = "DELETE FROM zarr WHERE k > ? AND k < ?"
            await self._conn.execute(sql, params)

    @override
    async def getsize(self, key: str) -> int:
        _validate_key(key)
        await self._ensure_open()
        assert self._conn is not None
        row = await self._conn.fetchone(
            "SELECT LENGTH(v) FROM zarr WHERE k = ?", (key,)
        )
        if row is None:
            raise FileNotFoundError(key)
        return int(row[0])

    @override
    async def getsize_prefix(self, prefix: str) -> int:
        await self._ensure_open()
        assert self._conn is not None
        params: Sequence[str] = ()
        if prefix == "":
            sql = "SELECT SUM(LENGTH(v)) FROM zarr"
        else:
            params = self._get_text_boundaries(prefix)
            sql = "SELECT SUM(LENGTH(v)) FROM zarr WHERE k > ? AND k < ?"
        size_row = await self._conn.fetchone(sql, params)
        if size_row is None or size_row[0] is None:
            return 0
        return int(size_row[0])

    def __del__(self):
        if self._conn is not None:
            self._conn.stop()

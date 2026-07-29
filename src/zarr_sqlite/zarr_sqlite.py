import urllib
from typing import override, Self
from collections.abc import Iterable, AsyncIterator
import datetime
import sqlite3
from pathlib import Path
import urllib.parse
import asyncio

from zarr.core.buffer import BufferPrototype, Buffer

from zarr.abc.store import (
    ByteRequest,
    OffsetByteRequest,
    RangeByteRequest,
    Store,
    SuffixByteRequest,
)

from ._version import __version__
from ._pool import AsyncConnectionPool, PooledConnection
from ._db_utils import is_database_uri, is_in_memory_database

if sqlite3.threadsafety not in {1, 3}:
    raise ImportError(
        "SQLiteStore requires sqlite3 to be compiled with multi-thread or serialized threading mode."
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
    is_valid = not (key.startswith("/") or key.endswith("/") or "//" in key)
    if not is_valid:
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
    _pool: AsyncConnectionPool
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
        self._pool = AsyncConnectionPool(self.database, read_only=read_only)
        self._open_lock = asyncio.Lock()

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
                netloc='',
                path=uri_parts.path,
                query=urllib.parse.urlencode(query, doseq=True),
                fragment=''
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

    @override
    async def _open(self) -> None:
        if self._is_open:
            raise ValueError("store is already open")

        # Connection pool cannot be reused, if it was closed, we need to create
        # a new one.
        if not self._pool.is_open:
            self._pool = AsyncConnectionPool(self.database, read_only=self._read_only)

        if not self._read_only:
            async with self._pool.acquire_write() as conn:
                await self._create_schema(conn)
        await self._validate_schema()
        self._is_open = True

    @override
    def with_read_only(self, read_only: bool = False) -> Self:
        if self.is_in_memory():
            raise ValueError("Cannot create a read-only view of an in-memory database.")
        return type(self)(self.database, read_only=read_only)

    async def _create_schema(self, conn: PooledConnection) -> None:
        if self._journal_mode is not None:
            if self._journal_mode not in {"DELETE", "WAL"}:
                raise ValueError(f"Invalid journal_mode: {self._journal_mode}")
            try:
                conn.autocommit = True
                await conn.execute("PRAGMA journal_mode=" + self._journal_mode)
            finally:
                conn.autocommit = False

        await conn.execute("PRAGMA page_size=" + str(int(self._page_size)))
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS sqlitestore_metadata("
            "k TEXT PRIMARY KEY NOT NULL, v TEXT NOT NULL)"
        )
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS zarr("
            "k TEXT PRIMARY KEY NOT NULL, v BLOB NOT NULL)"
        )
        await conn.execute(
            "PRAGMA application_id = " + str(_SQLITESTORE_APPLICATION_ID)
        )
        await conn.executemany(
            "INSERT OR IGNORE INTO sqlitestore_metadata(k, v) VALUES (?, ?)",
            [
                ("sqlitestore_version", _SQLITESTORE_SPEC_VERSION),
                ("compatible_flags", ""),
                ("incompatible_flags", ""),
                ("created_by", _CREATED_BY),
            ],
        )
        await self._update_timestamp(conn)

    async def _update_timestamp(self, conn: PooledConnection):
        now = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")
        await conn.execute(
            "INSERT INTO sqlitestore_metadata(k, v) VALUES (?, ?) "
            "ON CONFLICT(k) DO UPDATE SET v = excluded.v",
            ("created_time", now),
        )

    async def _validate_schema(self) -> None:
        metadata_keys = (
            "sqlitestore_version",
            "compatible_flags",
            "incompatible_flags",
        )

        async with self._pool.acquire() as conn:
            for required_table in ("zarr", "sqlitestore_metadata"):
                rows = await conn.fetchall("PRAGMA table_info(" + required_table + ")")
                if not rows:
                    raise ValueError(
                        f"Invalid SQLiteStore file: missing required table '{required_table}'"
                    )
            metadata_rows = await conn.fetchall(
                "SELECT k, v FROM sqlitestore_metadata WHERE k IN (?, ?, ?)",
                metadata_keys,
            )

        metadata = dict(metadata_rows) if metadata_rows else {}
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
        """Immediately close the store and all SQLite connections.

        Calling close() closes all SQLite connections immediately. SQLite will
        perform its normal connection cleanup, including rolling back
        uncommitted transactions and performing the normal WAL close-time
        checkpoint behavior when the last connection closes. Any in-flight
        operations using those connections may fail and callers must ensure that
        all operations have completed before closing the store.
        """
        if not self._is_open:
            return
        self._is_open = False
        self._pool.close()

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
        if prefix == "":
            row = await self._pool.fetchone("SELECT EXISTS(SELECT 1 FROM zarr LIMIT 1)")
        else:
            bounds = self._get_text_boundaries(prefix)
            row = await self._pool.fetchone(
                "SELECT EXISTS(SELECT 1 FROM zarr WHERE k > ? AND k < ? LIMIT 1)",
                bounds,
            )
        return row is not None and row[0] == 0

    @override
    async def clear(self) -> None:
        """Clear the store."""
        self._check_writable()
        await self._ensure_open()
        async with self._pool.acquire_write() as conn:
            await conn.execute("DROP TABLE IF EXISTS zarr")
            await self._create_schema(conn)

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

    async def _get_partial_blob(
        self, key: str, byte_range: ByteRequest
    ) -> bytes | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchone("SELECT rowid FROM zarr WHERE k = ?", (key,))
            if row is None:
                return None
            rowid = row[0]

            blob = await conn.blobopen("zarr", "v", rowid)
            blob_len = len(blob)

            match byte_range:
                case OffsetByteRequest(offset=o):
                    start, length = min(o, blob_len), blob_len - min(o, blob_len)
                case RangeByteRequest(start=s, end=e):
                    start = max(0, s)
                    end_clamped = min(blob_len, e)
                    length = max(0, end_clamped - start)
                case SuffixByteRequest(suffix=s):
                    start = max(0, blob_len - s)
                    length = blob_len - start
                case _:
                    raise ValueError(f"Unsupported byte range type: {type(byte_range)}")

            if length == 0:
                return b""

            try:
                await blob.seek(start)
                data = await blob.read(length)
            finally:
                await blob.close()

        return data

    @override
    async def get(
        self,
        key: str,
        prototype: BufferPrototype,
        byte_range: ByteRequest | None = None,
    ) -> Buffer | None:
        _validate_key(key)
        await self._ensure_open()

        data = None
        if byte_range is None:
            row = await self._pool.fetchone("SELECT v FROM zarr WHERE k = ?", (key,))
            if row is not None and isinstance(row[0], bytes):
                data = row[0]
        else:
            data = await self._get_partial_blob(key, byte_range)

        if data is None:
            return None
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
        row = await self._pool.fetchone("SELECT v FROM zarr WHERE k = ?", (key,))
        return row is not None

    @override
    async def set(self, key: str, value: Buffer) -> None:
        self._check_writable()
        _validate_key(key)
        await self._ensure_open()
        await self._pool.execute_write(
            "INSERT OR REPLACE INTO zarr (k, v) VALUES (?, ?)", (key, value.to_bytes())
        )

    @override
    async def set_if_not_exists(self, key: str, value: Buffer) -> None:
        self._check_writable()
        _validate_key(key)
        await self._ensure_open()
        await self._pool.execute_write(
            "INSERT OR IGNORE INTO zarr (k, v) VALUES (?, ?)", (key, value.to_bytes())
        )

    @override
    async def delete(self, key: str) -> None:
        self._check_writable()
        await self._ensure_open()
        await self._pool.execute_write("DELETE FROM zarr WHERE k = ?", (key,))

    @override
    async def list(self) -> AsyncIterator[str]:
        await self._ensure_open()
        async for row in self._pool.fetch_iter("SELECT k FROM zarr"):
            yield str(row[0])

    @override
    async def list_prefix(self, prefix: str) -> AsyncIterator[str]:
        await self._ensure_open()
        if prefix == "":
            async for row in self._pool.fetch_iter("SELECT k FROM zarr"):
                yield str(row[0])
        else:
            bounds = self._get_text_boundaries(prefix)
            async for row in self._pool.fetch_iter(
                "SELECT k FROM zarr WHERE k > ? AND k < ?", bounds
            ):
                yield str(row[0])

    @override
    async def list_dir(self, prefix: str) -> AsyncIterator[str]:
        if prefix != "" and not prefix[-1] == "/":
            # Even though prefix will be normalized in list_prefix(), this is
            # required for the removeprefix(prefix) call used below.
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

        async with self._pool.acquire_write() as conn:
            # Check if key exists
            row = await conn.fetchone(
                "SELECT v FROM zarr WHERE k = ?", (prefix.rstrip("/"),)
            )
            if row is not None:
                raise ValueError(
                    f"Cannot delete directory {prefix} as it is a key in the store."
                )

            if prefix == "":
                await conn.execute("DELETE FROM zarr")
            else:
                bounds = self._get_text_boundaries(prefix)
                await conn.execute("DELETE FROM zarr WHERE k > ? AND k < ?", bounds)

    @override
    async def getsize(self, key: str) -> int:
        _validate_key(key)
        await self._ensure_open()
        row = await self._pool.fetchone(
            "SELECT LENGTH(v) FROM zarr WHERE k = ?", (key,)
        )
        if row is None:
            raise FileNotFoundError(key)
        return int(row[0])

    @override
    async def getsize_prefix(self, prefix: str) -> int:
        await self._ensure_open()
        if prefix == "":
            size_row = await self._pool.fetchone("SELECT SUM(LENGTH(v)) FROM zarr")
        else:
            bounds = self._get_text_boundaries(prefix)
            size_row = await self._pool.fetchone(
                "SELECT SUM(LENGTH(v)) FROM zarr WHERE k > ? AND k < ?", bounds
            )
        if size_row is None or size_row[0] is None:
            return 0
        return int(size_row[0])

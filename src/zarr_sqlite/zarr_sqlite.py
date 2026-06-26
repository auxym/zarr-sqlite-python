from __future__ import annotations

__version__ = "0.1.0"

import asyncio
import sqlite3
from pathlib import Path
from typing import TYPE_CHECKING, override, cast
import urllib.parse
import uuid

from zarr.abc.store import (
    ByteRequest,
    OffsetByteRequest,
    RangeByteRequest,
    Store,
    SuffixByteRequest,
)
from zarr.core.buffer import Buffer
from zarr.core.common import BytesLike
from zarr import __version__ as zarr_version
from sys import version as py_version

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterable, Sequence

    from zarr.core.buffer import BufferPrototype


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

    database_uri: str
    _is_open: bool
    _con: sqlite3.Connection
    _lock: asyncio.Lock | None
    _journal_mode: str | None

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

    @property
    @override
    def supports_partial_writes(self) -> bool:
        # TODO: implement with blob API
        return False

    def __init__(
        self,
        database: str | Path,
        *,
        read_only: bool = False,
        journal_mode: str | None = 'WAL',
    ) -> None:
        super().__init__(read_only=read_only)
        self.database_uri = self._build_database_uri(database, read_only=read_only)
        self._journal_mode = journal_mode

    @staticmethod
    def _build_database_uri(database: Path | str, read_only: bool) -> str:
        """Create a sqlite-compatible URI string from a user-provided path or string.

        database may be either:
            - A Path object
            - The string ":memory:" for an in-memory database
            - A valid (sqlite compatible) filename URI

        If a URI is passed in, the `mode` query parameter will be modified, if
        present, according to `read_only`. Other query parameters will be left
        unmodified.

        Ref: https://sqlite.org/uri.html
        """

        query = {"mode": ["ro"] if read_only else ["rw"]}
        uri_path = ""

        if isinstance(database, Path):
            uri_path = database.resolve().as_uri().removeprefix("file://")
        elif database == ":memory:":
            # In-memory databases cannot be opened in read-only mode
            if read_only:
                raise ValueError("Cannot open an in-memory database in read-only mode.")
            uri_path = "mem-" + str(
                uuid.uuid4()
            )  # Generate a unique ID for the in-memory database
            query["mode"] = ["memory"]
            query["cache"] = ["shared"]
        elif not database.startswith("file:"):
            # If the database is not a URI and not in-memory, we assume it's a file path
            uri_path = Path(database).resolve().as_uri().removeprefix("file://")
        else:
            # If we get here, assume that database is a uri string.
            # Extract the path and query, keep the query as is except for mode, which is set
            # based on read_only.
            parsed = urllib.parse.urlparse(database)
            uri_path = parsed.path
            query = urllib.parse.parse_qs(parsed.query) | query

        return f"file:{uri_path}?{urllib.parse.urlencode(query, doseq=True)}"

    @override
    async def _open(self) -> None:
        if self._is_open:
            raise ValueError("store is already open")

        # Ensure sqlite3's thread safe serialized mode is enabled
        # zarr accesses the store from multiple threads
        # See: https://docs.python.org/3/library/sqlite3.html#sqlite3.threadsafety
        if sqlite3.threadsafety != 3:
            raise RuntimeError(
                "SQLiteStore requires sqlite3 to be compiled with threadsafety=3 (serialized mode)."
            )

        await super()._open()
        self._lock = asyncio.Lock()
        self._con = sqlite3.connect(
            self.database_uri, uri=True, autocommit=False, check_same_thread=False
        )
        if not self._read_only:
            if self._journal_mode is not None:
                if self._journal_mode not in ["DELETE", "TRUNCATE", "PERSIST", "WAL", "OFF"]:
                    raise ValueError(f"Invalid journal_mode: {self._journal_mode}")
                self._con.autocommit = True
                self._con.execute(f"PRAGMA journal_mode={self._journal_mode}")
                self._con.autocommit = False
            self._con.execute("PRAGMA page_size=16384")
            await self._create_schema()
        # TODO: If read-only, validate the schema?
        self._is_open = True

    @override
    def with_read_only(self, read_only: bool = False) -> SQLiteStore:
        return SQLiteStore(self.database_uri, read_only=read_only)

    async def _execute_write(self, query: str, params: Sequence[object] = ()) -> None:
        """Execute a query with our lock and commit."""
        await self._ensure_open()
        if self._lock is None:
            raise ValueError("Store is not open")
        async with self._lock:
            cursor = self._con.cursor()
            _ = cursor.execute(query, params)
            self._con.commit()

    async def _execute(
        self, query: str, params: Sequence[object] = ()
    ) -> sqlite3.Cursor:
        """Placeholder for future async implementation."""
        await self._ensure_open()
        cur = self._con.cursor()
        return cur.execute(query, params)

    async def _create_schema(self) -> None:
        await self._ensure_open()
        if self._lock is None:
            raise ValueError("Store is not open")

        schema =  ["CREATE TABLE IF NOT EXISTS zarr(k TEXT PRIMARY KEY, v BLOB)",
                   "CREATE TABLE IF NOT EXISTS zarr_versions(name TEXT PRIMARY KEY, version TEXT)"]

        set_version  = "INSERT OR REPLACE INTO zarr_versions (name, version) VALUES (?, ?)"
        versions = {'SqliteStore': __version__,
                     'Zarr_Create': zarr_version,
                     'Python_Create':  py_version}
        async with self._lock:
            cursor = self._con.cursor()
            for statement in schema:
                _ = cursor.execute(statement)
            for name, ver in versions.items():
                _ = cursor.execute(set_version, (name, ver))
            self._con.commit()

    async def get_versions(self):
        "return a dict of version information"
        cur = await self._execute("SELECT * from zarr_versions;")
        result = {}
        print(cur)
        for row in cur.fetchall():
            result[row.name] = row.version
            print("got result ", row)
        return result


    @override
    def close(self) -> None:
        if self._is_open:
            super().close()
            self._con.close()
            self._lock = None

    @override
    async def is_empty(self, prefix: str) -> bool:
        if not prefix.endswith("/"):
            prefix += "/"
        cur = await self._execute(
            "SELECT COUNT(*) FROM zarr WHERE k GLOB ?", (prefix + "/*",)
        )
        return cast(tuple[int], cur.fetchone())[0] == 0

    @override
    async def clear(self) -> None:
        """Clear the store."""
        await self._execute_write("DROP TABLE IF EXISTS zarr;")
        await self._execute_write("DROP TABLE IF exists zarr_versions;")
        await self._create_schema()

    @override
    def __str__(self) -> str:
        return f"sqlite://{self.database_uri}"

    @override
    def __repr__(self) -> str:
        return f"SQLiteStore('{self}')"

    @override
    def __eq__(self, other: object) -> bool:
        """Equality comparison."""
        if not isinstance(other, type(self)):
            return False

        parsed_uri_self = urllib.parse.urlparse(self.database_uri)
        parsed_uri_other = urllib.parse.urlparse(other.database_uri)
        return parsed_uri_other.path == parsed_uri_self.path

    @override
    async def get(
        self,
        key: str,
        prototype: BufferPrototype,
        byte_range: ByteRequest | None = None,
    ) -> Buffer | None:
        # TODO: use the blob API to select a byte range directly from SQLite if possible
        cur = await self._execute("SELECT v FROM zarr WHERE k = ?", (key,))
        row = cast(tuple[object] | None, cur.fetchone())
        if row is None:
            return None
        blob = row[0]
        if not isinstance(blob, bytes):
            raise TypeError(f"Expected bytes for key {key}, got {type(blob)}")

        if byte_range is None:
            return prototype.buffer.from_bytes(blob)
        elif isinstance(byte_range, OffsetByteRequest):
            a = min(byte_range.offset, len(blob))
            return prototype.buffer.from_bytes(blob[a:])
        elif isinstance(byte_range, RangeByteRequest):
            a, b = max(0, byte_range.start), min(len(blob), byte_range.end)
            return prototype.buffer.from_bytes(blob[a:b])
        elif isinstance(byte_range, SuffixByteRequest):
            a = min(len(blob), byte_range.suffix)
            return prototype.buffer.from_bytes(blob[-a:])

    @override
    async def get_partial_values(
        self,
        prototype: BufferPrototype,
        key_ranges: Iterable[tuple[str, ByteRequest | None]],
    ) -> list[Buffer | None]:
        result: list[Buffer | None] = []
        for key, byte_range in key_ranges:
            buffer = await self.get(key, prototype, byte_range)
            result.append(buffer)
        return result

    @override
    async def exists(self, key: str) -> bool:
        cur = await self._execute("SELECT v FROM zarr WHERE k = ?", (key,))
        return cur.fetchone() is not None

    @override
    async def set(self, key: str, value: Buffer) -> None:
        await self._execute_write(
            "INSERT OR REPLACE INTO zarr (k, v) VALUES (?, ?)", (key, value.to_bytes())
        )

    @override
    async def set_if_not_exists(self, key: str, value: Buffer) -> None:
        await self._execute_write(
            "INSERT OR IGNORE INTO zarr (k, v) VALUES (?, ?)", (key, value.to_bytes())
        )

    @override
    async def delete(self, key: str) -> None:
        await self._execute_write("DELETE FROM zarr WHERE k = ?", (key,))

    # TODO: Implement partial writes with blob API
    @override
    async def set_partial_values(
        self, key_start_values: Iterable[tuple[str, int, BytesLike]]
    ) -> None:
        raise NotImplementedError("Partial writes are not supported by SQLiteStore.")

    @override
    async def list(self) -> AsyncIterator[str]:
        cur = await self._execute("SELECT k FROM zarr")
        for row in cast(Iterable[tuple[str]], cur):
            yield row[0]

    @override
    async def list_prefix(self, prefix: str) -> AsyncIterator[str]:
        if not prefix.endswith("/"):
            prefix += "/"
        cur = await self._execute("SELECT k FROM zarr WHERE k GLOB ?", (prefix + "*",))
        for row in cast(Iterable[tuple[str]], cur):
            yield row[0]

    @override
    async def list_dir(self, prefix: str) -> AsyncIterator[str]:
        seen: set[str] = set()
        async for full_key in self.list_prefix(prefix):
            relative_parts = full_key.removeprefix(prefix).split("/")
            k = relative_parts[0]
            if len(relative_parts) > 1:
                k = k + "/"  # Is a prefix
            if k not in seen:
                seen.add(k)
                yield k

    @override
    async def delete_dir(self, prefix: str) -> None:
        prefix = prefix.rstrip("/")
        if await self.exists(prefix):
            raise ValueError(
                f"Cannot delete directory {prefix} as it is a key in the store."
            )
        else:
            await self._execute_write(
                "DELETE FROM zarr WHERE k GLOB ?", (prefix + "/*",)
            )

    @override
    async def getsize(self, key: str) -> int:
        cur = await self._execute("SELECT LENGTH(v) FROM zarr WHERE k = ?", (key,))
        row = cast(tuple[int] | None, cur.fetchone())
        if row is None:
            raise FileNotFoundError(key)
        return row[0]

    @override
    async def getsize_prefix(self, prefix: str) -> int:
        if not prefix.endswith("/"):
            prefix += "/"
        cur = await self._execute(
            "SELECT SUM(LENGTH(v)) FROM zarr WHERE k GLOB ?", (prefix + "*",)
        )
        size = cast(tuple[int | None], cur.fetchone())[0]
        if size is None:
            size = 0
        return size

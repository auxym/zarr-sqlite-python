import asyncio

import sqlite3

from contextlib import asynccontextmanager

from typing import Sequence, AsyncGenerator


class AsyncBlob:
    _blob: sqlite3.Blob

    def __init__(self, blob: sqlite3.Blob):
        self._blob = blob

    def __len__(self) -> int:
        return len(self._blob)

    async def seek(self, offset: int, whence: int = 0) -> None:
        return await asyncio.to_thread(self._blob.seek, offset, whence)

    async def read(self, length: int = -1) -> bytes:
        return await asyncio.to_thread(self._blob.read, length)

    async def close(self) -> None:
        await asyncio.to_thread(self._blob.close)


class PooledConnection:
    _conn: sqlite3.Connection

    def __init__(self, connection: sqlite3.Connection):
        self._conn = connection

    async def commit(self) -> None:
        await asyncio.to_thread(self._conn.commit)

    async def rollback(self) -> None:
        await asyncio.to_thread(self._conn.rollback)

    async def execute(self, sql: str, params: Sequence = ()) -> None:
        await asyncio.to_thread(self._conn.execute, sql, params)

    async def executemany(self, sql: str, params: Sequence[Sequence] = ()) -> None:
        await asyncio.to_thread(self._conn.executemany, sql, params)

    async def fetchone(self, sql: str, params: Sequence = ()) -> tuple:
        cur = await asyncio.to_thread(self._conn.execute, sql, params)
        return await asyncio.to_thread(cur.fetchone)

    async def fetchall(self, sql: str, params: Sequence = ()) -> Sequence[tuple]:
        cur = await asyncio.to_thread(self._conn.execute, sql, params)
        return await asyncio.to_thread(cur.fetchall)

    async def blobopen(self, table: str, column: str, rowid: int) -> AsyncBlob:
        blob = await asyncio.to_thread(self._conn.blobopen, table, column, rowid)
        return AsyncBlob(blob)

    @property
    def autocommit(self) -> None:
        return self._conn.autocommit

    @autocommit.setter
    def autocommit(self, value: bool):
        self._conn.autocommit = value

    def close(self):
        self._conn.close()


class AsyncConnectionPool:
    _available: asyncio.Queue[PooledConnection]
    _writer_connection: PooledConnection | None
    _writer_lock: asyncio.Lock
    _creation_lock: asyncio.Lock
    _uri: str
    _is_open: bool
    _raw_connections: list[sqlite3.Connection]
    _max_connections: int
    _read_only: bool
    _acquire_timeout: int

    def __init__(
        self,
        uri: str,
        n_connections: int = 10,
        read_only: bool = False,
        acquire_timeout=5.0,
    ):
        self._writer_lock = asyncio.Lock()
        self._available = asyncio.Queue()
        self._uri = uri
        self._is_open = True
        self._raw_connections = []
        self._max_connections = n_connections
        self._read_only = read_only
        self._creation_lock = asyncio.Lock()
        self._acquire_timeout = acquire_timeout

        if not self._read_only:
            self._writer_connection = PooledConnection(self._create_connection_sync())
        else:
            self._writer_connection = None

    @property
    def is_open(self) -> bool:
        return self._is_open

    @property
    def read_only(self):
        return self._read_only

    def _create_connection_sync(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self._uri,
            uri=True,
            autocommit=False,
            check_same_thread=False,
        )
        return conn

    async def _create_connection(self) -> PooledConnection:
        conn = await asyncio.to_thread(self._create_connection_sync)
        self._raw_connections.append(conn)
        return PooledConnection(conn)

    @asynccontextmanager
    async def _writer_lock_timeout(self):
        try:
            await asyncio.wait_for(
                self._writer_lock.acquire(), timeout=self._acquire_timeout
            )
        except asyncio.TimeoutError:
            raise TimeoutError(
                "Timed out waiting for writer connection to become available"
            )

        try:
            yield
        finally:
            self._writer_lock.release()

    @asynccontextmanager
    async def acquire(self) -> AsyncGenerator[PooledConnection, None]:
        """Get a connection for read operations"""
        if not self.is_open:
            raise ValueError("Connection pool is closed.")
        try:
            conn = self._available.get_nowait()
        except asyncio.QueueEmpty:
            # Lazily create a new connection if we haven't reached max number of
            # connections. Otherwise, wait for a connection to become available.
            async with self._creation_lock:
                if len(self._raw_connections) < self._max_connections:
                    conn = await self._create_connection()
                else:
                    conn = await asyncio.wait_for(
                        self._available.get(), timeout=self._acquire_timeout
                    )

        try:
            yield conn
        finally:
            try:
                await conn.rollback()
            finally:
                await self._available.put(conn)

    @asynccontextmanager
    async def acquire_write(self) -> AsyncGenerator[PooledConnection, None]:
        """Acquire single writer connection"""
        if not self.is_open:
            raise ValueError("Connection pool is closed.")
        if self.read_only:
            raise ValueError("Connection pool is read-only.")
        assert self._writer_connection is not None

        async with self._writer_lock_timeout():
            try:
                yield self._writer_connection
            except BaseException:
                await self._writer_connection.rollback()
                raise
            else:
                try:
                    await self._writer_connection.commit()
                except BaseException:
                    await self._writer_connection.rollback()
                    raise

    async def execute_write(self, sql: str, params: Sequence = ()) -> None:
        async with self.acquire_write() as conn:
            await conn.execute(sql, params)

    async def executemany_write(
        self, sql: str, params: Sequence[Sequence] = ()
    ) -> None:
        async with self.acquire_write() as conn:
            await conn.executemany(sql, params)

    async def fetchone(self, sql: str, params: Sequence = ()) -> tuple:
        async with self.acquire() as conn:
            return await conn.fetchone(sql, params)

    async def fetchall(self, sql: str, params: Sequence = ()) -> tuple:
        async with self.acquire() as conn:
            return await conn.fetchall(sql, params)

    def close(self):
        if not self.is_open:
            return
        self._is_open = False

        if self._writer_connection is not None:
            try:
                self._writer_connection.close()
            except Exception:
                pass

        for conn in self._raw_connections:
            try:
                conn.close()
            except Exception:
                pass

        self._raw_connections = []
        self._writer_connection = None

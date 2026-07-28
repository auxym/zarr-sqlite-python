import asyncio

import sqlite3

from contextlib import asynccontextmanager

from typing import Sequence, AsyncGenerator, Any


class AsyncBlob:
    """Async wrapper for SQLite blob objects.

    This class wraps a sqlite3.Blob object to provide async access methods,
    allowing blob operations to be performed without blocking the event loop.
    """

    _blob: sqlite3.Blob

    def __init__(self, blob: sqlite3.Blob) -> None:
        """Initialize the AsyncBlob wrapper.

        Args:
            blob: The underlying sqlite3.Blob object to wrap.
        """
        self._blob = blob

    def __len__(self) -> int:
        """Return the size of the blob in bytes."""
        return len(self._blob)

    async def seek(self, offset: int, whence: int = 0) -> None:
        """Seek to a position in the blob.

        Args:
            offset: The byte offset to seek to.
            whence: How to interpret offset (0=absolute, 1=relative, 2=from end).
        """
        return await asyncio.to_thread(self._blob.seek, offset, whence)

    async def read(self, length: int = -1) -> bytes:
        """Read bytes from the blob.

        Args:
            length: Number of bytes to read. -1 reads all remaining bytes.

        Returns:
            The bytes read from the blob.
        """
        return await asyncio.to_thread(self._blob.read, length)

    async def close(self) -> None:
        """Close the blob."""
        await asyncio.to_thread(self._blob.close)


class PooledConnection:
    """A wrapper for sqlite3.Connection that provides async methods.

    This class wraps a synchronous sqlite3.Connection to provide asynchronous
    versions of common database operations, allowing them to be used in
    async contexts without blocking the event loop.
    """

    _conn: sqlite3.Connection

    def __init__(self, connection: sqlite3.Connection) -> None:
        """Initialize the PooledConnection wrapper.

        Args:
            connection: The underlying sqlite3.Connection to wrap.
        """
        self._conn = connection

    async def commit(self) -> None:
        """Commit the current transaction."""
        await asyncio.to_thread(self._conn.commit)

    async def rollback(self) -> None:
        """Roll back the current transaction."""
        await asyncio.to_thread(self._conn.rollback)

    async def execute(self, sql: str, params: Sequence[Any] = ()) -> None:
        """Execute a SQL statement.

        Args:
            sql: The SQL statement to execute.
            params: Parameters for the SQL statement.
        """
        await asyncio.to_thread(self._conn.execute, sql, params)

    async def executemany(self, sql: str, params: Sequence[Sequence[Any]] = ()) -> None:
        """Execute a SQL statement multiple times with different parameters.

        Args:
            sql: The SQL statement to execute.
            params: A sequence of parameter sequences, one for each execution.
        """
        await asyncio.to_thread(self._conn.executemany, sql, params)

    async def fetchone(
        self, sql: str, params: Sequence[Any] = ()
    ) -> tuple[Any, ...] | None:
        """Execute a SQL statement and fetch one result row.

        Args:
            sql: The SQL statement to execute.
            params: Parameters for the SQL statement.

        Returns:
            A single row as a tuple, or None if no rows were returned.
        """
        cur = await asyncio.to_thread(self._conn.execute, sql, params)
        return await asyncio.to_thread(cur.fetchone)

    async def fetchall(
        self, sql: str, params: Sequence[Any] = ()
    ) -> list[tuple[Any, ...]]:
        """Execute a SQL statement and fetch all result rows.

        Args:
            sql: The SQL statement to execute.
            params: Parameters for the SQL statement.

        Returns:
            A list of rows, each row being a tuple of column values.
        """
        cur = await asyncio.to_thread(self._conn.execute, sql, params)
        return await asyncio.to_thread(cur.fetchall)

    async def blobopen(self, table: str, column: str, rowid: int) -> AsyncBlob:
        """Open a BLOB for incremental I/O.

        Args:
            table: The table containing the BLOB.
            column: The column containing the BLOB.
            rowid: The rowid of the row containing the BLOB.

        Returns:
            An AsyncBlob object for reading/writing the BLOB.
        """
        blob = await asyncio.to_thread(self._conn.blobopen, table, column, rowid)
        return AsyncBlob(blob)

    @property
    def autocommit(self) -> bool:
        """Get the autocommit state of the connection."""
        return bool(self._conn.autocommit)

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        """Set the autocommit state of the connection.

        Args:
            value: True to enable autocommit, False to disable.
        """
        self._conn.autocommit = value

    def close(self) -> None:
        """Close the connection."""
        self._conn.close()


class AsyncConnectionPool:
    """An async connection pool for SQLite databases.

    This pool manages a set of SQLite connections that can be acquired for
    read operations and a single dedicated writer connection. It uses asyncio
    to ensure non-blocking database operations.

    Attributes:
        is_open: Whether the pool is currently open and accepting connections.
        read_only: Whether the pool is in read-only mode.
    """

    _available: asyncio.Queue[PooledConnection]
    _writer_connection: PooledConnection | None
    _writer_lock: asyncio.Lock
    _creation_lock: asyncio.Lock
    _uri: str
    _is_open: bool
    _raw_connections: list[sqlite3.Connection]
    _max_connections: int
    _read_only: bool
    _acquire_timeout: float

    def __init__(
        self,
        uri: str,
        n_connections: int = 10,
        read_only: bool = False,
        acquire_timeout: float = 5.0,
    ) -> None:
        """Initialize the connection pool.

        Args:
            uri: The SQLite database URI.
            n_connections: Maximum number of read connections in the pool.
            read_only: If True, the pool will not create a writer connection.
            acquire_timeout: Timeout in seconds for acquiring a connection.
        """
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
        """Whether the pool is currently open."""
        return self._is_open

    @property
    def read_only(self) -> bool:
        """Whether the pool is in read-only mode."""
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
    async def _writer_lock_timeout(self) -> AsyncGenerator[None, None]:
        """Acquire the writer lock with a timeout."""
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
        """Acquire a connection for read operations.

        This method gets a connection from the pool. If no connections are
        available and the maximum hasn't been reached, a new connection is
        created. Otherwise, it waits for a connection to become available.

        Yields:
            A PooledConnection for read operations.

        Raises:
            ValueError: If the pool is closed.
            asyncio.TimeoutError: If no connection becomes available within the timeout.
        """
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
        """Acquire the single writer connection.

        This method provides exclusive access to the writer connection.
        Transactions are automatically committed on success or rolled back on failure.

        Yields:
            The PooledConnection for write operations.

        Raises:
            ValueError: If the pool is closed or read-only.
            TimeoutError: If the writer lock could not be acquired within the timeout.
        """
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

    async def execute_write(self, sql: str, params: Sequence[Any] = ()) -> None:
        """Execute a write SQL statement and commit afterwards.

        Args:
            sql: The SQL statement to execute.
            params: Parameters for the SQL statement.
        """
        async with self.acquire_write() as conn:
            await conn.execute(sql, params)

    async def executemany_write(
        self, sql: str, params: Sequence[Sequence[Any]] = ()
    ) -> None:
        """Execute a write SQL statement multiple times with different parameters, commit afterwards.

        Args:
            sql: The SQL statement to execute.
            params: A sequence of parameter sequences, one for each execution.
        """
        async with self.acquire_write() as conn:
            await conn.executemany(sql, params)

    async def fetchone(
        self, sql: str, params: Sequence[Any] = ()
    ) -> tuple[Any, ...] | None:
        """Execute a read SQL statement and fetch one result row.

        Args:
            sql: The SQL statement to execute.
            params: Parameters for the SQL statement.

        Returns:
            A single row as a tuple, or None if no rows were returned.
        """
        async with self.acquire() as conn:
            return await conn.fetchone(sql, params)

    async def fetchall(
        self, sql: str, params: Sequence[Any] = ()
    ) -> list[tuple[Any, ...]]:
        """Execute a read SQL statement and fetch all result rows.

        Args:
            sql: The SQL statement to execute.
            params: Parameters for the SQL statement.

        Returns:
            A list of rows, each row being a tuple of column values.
        """
        async with self.acquire() as conn:
            return await conn.fetchall(sql, params)

    def close(self) -> None:
        """Close the connection pool and all connections.

        This method closes all connections in the pool, including the writer
        connection. After calling close(), the pool cannot be used again.
        """
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

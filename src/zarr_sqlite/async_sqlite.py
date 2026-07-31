# This file contains code which is derived from aiosqlite v0.22.1
# aiosqlite is Copyright (c) 2022 Amethyst Reese
# Modifications are Copyright (c) 2026 Francis Thérien
#
# The work is re-distributed here under the terms of the MIT license, reproduced
# below.

# MIT License

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

"""
Async SQLite connection proxy.

Core implementation derived from aiosqlite, simplified for use with
zarr-sqlite.  The background-thread model is preserved: SQL is executed on a
dedicated thread and results are returned as raw ``sqlite3`` objects.
"""

import asyncio
import logging
import sqlite3
from collections.abc import AsyncIterator, Generator, Iterable
from functools import partial
from pathlib import Path
from queue import SimpleQueue
from threading import Thread
from typing import Any, Callable, Optional
from warnings import warn

__all__ = ["connect", "Connection"]

LOG = logging.getLogger("zarr_sqlite")


class _Stop:
    pass


_STOP_RUNNING_SENTINEL = _Stop()
_TxQueue = SimpleQueue[tuple[Optional[asyncio.Future], Callable[[], Any]]]


def set_result(fut: asyncio.Future, result: Any) -> None:
    """Set the result of a future if it hasn't been set already."""
    if not fut.done():
        fut.set_result(result)


def set_exception(fut: asyncio.Future, e: BaseException) -> None:
    """Set the exception of a future if it hasn't been set already."""
    if not fut.done():
        fut.set_exception(e)


def _connection_worker_thread(tx: _TxQueue):
    """
    Execute function calls on a separate thread.

    :meta private:
    """
    while True:
        # Continues running until all queue items are processed,
        # even after connection is closed (so we can finalize all
        # futures)

        future, function = tx.get()

        try:
            LOG.debug("executing %s", function)
            result = function()

            if future:
                future.get_loop().call_soon_threadsafe(set_result, future, result)
            LOG.debug("operation %s completed", function)

            if result is _STOP_RUNNING_SENTINEL:
                break

        except BaseException as e:  # noqa B036
            LOG.debug("returning exception %s", e)
            if future:
                future.get_loop().call_soon_threadsafe(set_exception, future, e)


class Connection:
    def __init__(
        self,
        connector: Callable[[], sqlite3.Connection],
    ) -> None:
        self._running = True
        self._connection: Optional[sqlite3.Connection] = None
        self._connector = connector
        self._tx: _TxQueue = SimpleQueue()
        self._thread = Thread(target=_connection_worker_thread, args=(self._tx,))

    def __del__(self):
        if self._connection is None:
            return

        warn(
            (
                f"{self!r} was deleted before being closed. "
                "Please use 'async with' or '.close()' to close the connection properly."
            ),
            ResourceWarning,
            stacklevel=1,
        )

        # Don't try to be creative here, the event loop may have already been
        # closed.  Simply stop the worker thread, and let the underlying
        # sqlite3 connection be finalized by its own __del__.
        self.stop()

    def stop(self, join=False, join_timeout: float | None = None) -> Optional[asyncio.Future]:
        """Stop the background thread. Prefer `async with` or `await close()`"""
        self._running = False

        def close_and_stop():
            if self._connection is not None:
                self._connection.close()
                self._connection = None
            return _STOP_RUNNING_SENTINEL

        try:
            future = asyncio.get_event_loop().create_future()
        except Exception:
            future = None

        self._tx.put_nowait((future, close_and_stop))
        if join:
            self._thread.join(timeout=join_timeout)
        return future

    @property
    def _conn(self) -> sqlite3.Connection:
        if self._connection is None:
            raise ValueError("no active connection")

        return self._connection

    async def _execute(self, fn, *args, **kwargs):
        """Queue a function with the given arguments for execution."""
        if not self._running or not self._connection:
            raise ValueError("Connection closed")

        function = partial(fn, *args, **kwargs)
        future = asyncio.get_event_loop().create_future()

        self._tx.put_nowait((future, function))

        return await future

    async def _connect(self) -> "Connection":
        """Connect to the actual sqlite database."""
        if self._connection is None:
            try:
                future = asyncio.get_event_loop().create_future()
                self._tx.put_nowait((future, self._connector))
                self._connection = await future
            except BaseException:
                self.stop()
                self._connection = None
                raise

        return self

    def __await__(self) -> Generator[Any, None, "Connection"]:
        self._thread.start()
        return self._connect().__await__()

    async def commit(self) -> None:
        """Commit the current transaction."""
        await self._execute(self._conn.commit)

    async def rollback(self) -> None:
        """Roll back the current transaction."""
        await self._execute(self._conn.rollback)

    async def close(self) -> None:
        """Complete queued queries/cursors and close the connection."""

        if self._connection is None:
            return

        try:
            await self._execute(self._conn.close)
        except Exception:
            LOG.info("exception occurred while closing connection")
            raise
        finally:
            self._connection = None
            future = self.stop(join=False)
            if future:
                await future

    async def execute(self, sql: str, parameters: Iterable[Any] | None = None) -> None:
        """Execute a SQL statement."""
        if parameters is None:
            parameters = []
        await self._execute(self._conn.execute, sql, parameters)

    async def executemany(self, sql: str, parameters: Iterable[Any]) -> None:
        """Execute a SQL statement many times."""
        await self._execute(self._conn.executemany, sql, parameters)

    async def fetchall(
        self, sql: str, parameters: Iterable[Any] | None = None
    ) -> list[tuple[Any, ...]]:
        """Execute a SQL query and return all matching rows.

        Both the cursor creation and ``fetchall`` happen on the
        worker thread in a single queued operation.
        """
        if parameters is None:
            parameters = []

        def _fetchall():
            cursor = self._conn.execute(sql, parameters)
            return cursor.fetchall()

        return await self._execute(_fetchall)

    async def fetchone(
        self, sql: str, parameters: Iterable[Any] | None = None
    ) -> tuple[Any, ...] | None:
        """Execute a SQL query and return the first matching row.

        Returns ``None`` if no rows match.  Both the cursor creation
        and ``fetchone`` happen on the worker thread in a single
        queued operation.
        """
        if parameters is None:
            parameters = []

        def _fetchone():
            cursor = self._conn.execute(sql, parameters)
            return cursor.fetchone()

        return await self._execute(_fetchone)

    async def fetch_iter(
        self, sql: str, parameters: Iterable[Any] | None = None
    ) -> AsyncIterator[tuple[Any, ...]]:
        """Asynchronously iterate over rows from a SQL query.

        The cursor is created and all fetches happen on the worker
        thread.  Rows are fetched in chunks of ``_fetch_chunk_size``
        for efficiency, with individual rows yielded to the caller.
        The cursor is closed when the iterator is exhausted or when
        the caller breaks out of the loop.
        """
        if parameters is None:
            parameters = []

        _fetch_chunk_size: int = 20
        cursor = await self._execute(self._conn.execute, sql, parameters)
        try:
            while True:
                chunk = await self._execute(cursor.fetchmany, _fetch_chunk_size)
                if not chunk:
                    break
                for row in chunk:
                    yield row
        finally:
            await self._execute(cursor.close)


def connect(
    database: Path | str,
    **kwargs: Any,
) -> Connection:
    """Create and return a connection proxy to the sqlite database."""

    def connector() -> sqlite3.Connection:
        if isinstance(database, str):
            loc = database
        elif isinstance(database, bytes):
            loc = database.decode("utf-8")
        else:
            loc = str(database)

        return sqlite3.connect(loc, **kwargs)

    return Connection(connector)

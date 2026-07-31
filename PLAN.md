# Implementation Plan: Replace Connection Pool with Single aiosqlite Connection

## Goal

Replace the custom connection pool (`_pool.py`) with a single `aiosqlite.Connection`.
This simplifies the architecture by removing the pool abstraction, writer lock,
and per-operation connection management.

## Key API Findings (aiosqlite 0.22.1)

- `aiosqlite.connect(database, uri=..., **kwargs)` returns a `Connection` (awaitable).
  The `**kwargs` are passed to `sqlite3.connect()`.
- `Connection.execute(sql, params)` returns a `Cursor` (awaitable). Cursors have
  `fetchone`, `fetchall`, `fetchmany`, `close` (all async). Cursors are also async
  context managers.
- `Connection.execute_fetchall(sql, params)` returns a list of rows (awaitable).
- `Connection.executemany(sql, params)` returns a `Cursor` (awaitable).
- `Connection.commit()` and `Connection.rollback()` are async.
- `Connection.close()` is async — closes the connection and stops the background thread.
- `Connection.stop()` is **synchronous** — sets `_running = False`, enqueues a
  `close_and_stop` task on the internal queue. The background thread picks it up
  and closes the underlying `sqlite3.Connection`. Returns a future (may be `None`
  if no event loop is running). This is the synchronous shutdown method.
- There is **no** `shutdown()` method on aiosqlite. `stop()` is the synchronous
  equivalent and will be used in the store's synchronous `close()`.
- `Connection` does **not** have `fetchone`/`fetchall`/`fetchmany` — these are on
  `Cursor` only.
- `Connection` does **not** have an `autocommit` property.
- **Important finding**: Passing `autocommit=False` to `aiosqlite.connect()`
  causes `sqlite3.OperationalError: cannot change into wal mode from within a
  transaction` when executing `PRAGMA journal_mode=WAL`, even on a fresh
  connection. This is because aiosqlite with `autocommit=False` keeps a
  transaction open. **Decision**: Use `autocommit=False` for transaction control.
  For `PRAGMA journal_mode`, open a temporary connection with
  `isolation_level=None` (autocommit mode), set the journal mode, close it,
  then reopen the final connection with `autocommit=False`.
- For multi-statement write operations that need atomicity (e.g. `clear`,
  `delete_dir`), use explicit `BEGIN` / `COMMIT` / `ROLLBACK` SQL statements
  since `commit()`/`rollback()` are no-ops in autocommit mode.

## Changes

### 1. `src/zarr_sqlite/_pool.py`  ✅ DONE

- Remove all code (empty file). All classes (`AsyncBlob`, `PooledConnection`,
  `AsyncConnectionPool`) are deleted.

### 2. `src/zarr_sqlite/zarr_sqlite.py`

#### 2.1 Imports  ✅ DONE
- Remove `from ._pool import AsyncConnectionPool, PooledConnection`.
- Add `import aiosqlite`.
- Remove `import urllib` (unused; `urllib.parse` is imported separately).
- Keep `import asyncio` (used for `asyncio.Lock` in `_open_lock` and
  `asyncio.gather` in `get_partial_values`).
- Keep `import sqlite3` (used for module-level version/threadsafety checks).
- Add `Any` to `typing` imports, `Sequence`/`AsyncGenerator` to `collections.abc` imports.
- Keep `from ._db_utils import is_database_uri, is_in_memory_database`.

#### 2.2 Instance attributes  ✅ DONE
- Replace `_pool: AsyncConnectionPool` with `_conn: aiosqlite.Connection | None`.
- Remove `_max_connections` attribute and constructor parameter.
- Keep `_journal_mode` and `_page_size`.

#### 2.3 `__init__`  ✅ DONE
- Remove `max_connections` parameter.
- Do **not** create a connection in `__init__` (lazy opening via `_ensure_open`).
- Set `self._conn = None`.
- Keep the `read_only` URI conversion logic.

#### 2.4 `_open()`  ✅ DONE
- Create the aiosqlite connection with `autocommit=False`:
  ```python
  self._conn = await aiosqlite.connect(
      self._database,
      uri=is_database_uri(self._database),
      autocommit=False,
  )
  ```
- **Journal mode handling** (user's modification): Before opening the final
  `autocommit=False` connection, if journal mode needs to be changed, open a
  temporary connection with `autocommit=True`, set `PRAGMA journal_mode`, close
  it, then open the final connection with `autocommit=False`. This avoids
  having to close and reopen the final connection.
- Remove the `if not self._pool.is_open` check (no pool to reuse).
- Call `_create_schema()` within `async with self._transaction():` (which
  commits on success, rolls back on error).
- Call `_validate_schema()` using `self._conn` directly.
- Added `_transaction_lock: asyncio.Lock` attribute and initialization in `__init__`.
- Added `_transaction()` async context manager: acquires `_transaction_lock`,
  yields, commits on success (in `try` block — commit can fail), rolls back on error.
- Removed helper methods (`_fetchone`, `_fetchall`, `_fetch_iter`, `_execute_write`).
  Reads use `self._conn.execute()` directly; writes use `_transaction()`.
- Updated `_create_schema()`: removed `conn` parameter, removed journal mode
  logic (moved to `_open()`), uses `self._conn` directly.
- Updated `_update_timestamp()`: removed `conn` parameter, uses `self._conn`.
- Updated `_db_is_empty()`: uses `self._conn.execute()` directly.
- Updated `_validate_schema()`: uses `self._conn.execute()` directly.
- Updated `close()`: uses `self._conn.stop()` instead of `self._pool.close()`.

#### 2.5 `_create_schema()`  ✅ DONE (done as part of 2.4)
- Remove `conn: PooledConnection` parameter; use `self._conn` directly.
- Journal mode logic moved to `_open()`.
- Uses `async with self._conn.execute(...)` for all statements.

#### 2.6 `_update_timestamp()`  ✅ DONE (done as part of 2.4)
- Remove `conn: PooledConnection` parameter; use `self._conn` directly.

#### 2.7 `_db_is_empty()`  ✅ DONE (done as part of 2.4)
- Uses `self._conn.execute()` directly (no helper).

#### 2.8 `_validate_schema()`  ✅ DONE (done as part of 2.4)
- Uses `self._conn.execute()` directly (no helpers).

#### 2.9 `close()`  ✅ DONE (done as part of 2.4)
- Uses `self._conn.stop()` instead of `self._pool.close()`.

#### 2.10 Helper methods — REMOVED
- No helper methods. Reads use `self._conn.execute()` directly.
- Writes use `async with self._transaction():` context manager.

#### 2.11 `is_empty()`  ✅ DONE
- Uses `self._conn.execute()` directly for reads (no helper).

#### 2.12 `clear()`  ✅ DONE
- Uses `async with self._transaction():` for writes.
- Uses `self._conn.execute()` directly inside the transaction.

#### 2.13 `_get_partial_blob()` — REMOVED  ✅ DONE
- No longer needed. Byte-range slicing is inlined into `get()`.

#### 2.14 `get()`  ✅ DONE
- Fetch full blob via `self._conn.execute()` with cursor context manager.
- For `byte_range is not None`, apply Python slice directly on the fetched bytes.
- For `byte_range is None`, return the full blob.

#### 2.15 `get_partial_values()`  ✅ DONE
- No change needed (uses `asyncio.gather` with `self.get`).

#### 2.16 `exists()`  ✅ DONE
- Use `self._conn.execute()` directly for reads.

#### 2.17 `set()` / `set_if_not_exists()` / `delete()`  ✅ DONE
- Use `async with self._transaction():` for writes.

#### 2.18 `list()` / `list_prefix()`  ✅ DONE
- Use `self._conn.execute()` directly with cursor iteration for reads.

#### 2.19 `delete_dir()`  ✅ DONE
- Replaced `async with self._pool.acquire_write() as conn:` with
  `async with self._transaction():` for writes.

#### 2.20 `getsize()` / `getsize_prefix()`  ✅ DONE
- Use `self._conn.execute()` directly for reads.

### 3. `src/zarr_sqlite/__init__.py`
- No changes needed (only imports `SQLiteStore` and `__version__`).

### 4. `src/zarr_sqlite/_db_utils.py`
- No changes needed (still used for URI/memory detection).

## Concurrency Model

With a single aiosqlite connection, all database operations are serialized
through the background thread. This means:
- No concurrent reads (single connection).
- No separate writer connection.
- No need for writer lock or connection pool.
- Transactions are managed manually (commit after writes, rollback on error).

## Transaction Handling

- **`autocommit=False`**: The sqlite3 module manages transactions (legacy mode).
  DML statements (INSERT, UPDATE, DELETE) automatically start a transaction.
  PRAGMA and DDL do not start a transaction. `commit()`/`rollback()` are used
  to save/undo changes.
- **Read operations**: No commit/rollback needed. Cursors are closed after use.
  Use `self._conn.execute()` directly.
- **Write operations**: Use `async with self._transaction():` which acquires
  `_transaction_lock`, yields, commits on success, rolls back on error.
  The `commit()` is inside the `try` block because it can fail.
- **Journal mode**: Set via a temporary `isolation_level=None` connection
  (autocommit mode) before opening the final `autocommit=False` connection.
- **Multi-statement writes** (`clear`, `delete_dir`): Wrap in
  `async with self._transaction():` for atomicity.

## Testing

Tests will be updated in a follow-up. Known test impacts:
- `test_concurrent_read_creates_multiple_connections` — references
  `sqlite_store._pool.num_connections`, will need rewriting.
- `test_get_partial_values` docstring mentions connection pool, will need
  updating.

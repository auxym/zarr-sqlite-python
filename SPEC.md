# Zarr SQLiteStore specification version 1.0

## Scope

This document describes **SQLiteStore**, a [Zarr (v3)](https://zarr.dev/) store
that allows the storage of a complete Zarr hierarchy inside a single file by
using the SQLite database engine.

## Notes about the design decisions

SQLiteStore is designed primarily for sharing moderately sized datasets by
storing an entire dataset in a single file.

SQLite was chosen to accomplish this goal for the following reasons:

- SQLite has been in widespread use for a long time and is considered to be
  highly robust software.
- Unlike many archive formats, SQLite natively supports Zarr store operations
  such as key deletion, overwriting values, and modifying stored values.
- SQLite libraries are available for nearly all programming languages and
  computers.


SQLite supports databases several terabytes in size and this specification
does not define any file size limit. However, the design of SQLiteStore is
intended for datasets up to approximately 10 GB.
  
SQLiteStore was not designed to compete in read or write speed with other
stores, such as *FileSystemStore*.

## Document conventions

The key words “MUST”, “MUST NOT”, “REQUIRED”, “SHALL”, “SHALL NOT”, “SHOULD”,
“SHOULD NOT”, “RECOMMENDED”, “MAY”, and “OPTIONAL” in this document are to be
interpreted as described in [IETF RFC
2119](https://datatracker.ietf.org/doc/html/rfc2119).

## Definitions

The terms *store*, *hierarchy*, *chunk* and *array* are defined by the [Zarr core specification](https://zarr-specs.readthedocs.io/en/latest/v3/core/index.html#concepts-and-terminology).

A SQLiteStore file is a SQLite database conforming to this specification.

An *implementation* refers to library or software that is designed to read and
write SQLiteStore files by implementing the operations defined by the [*Abstract store interface*](https://zarr-specs.readthedocs.io/en/latest/v3/core/index.html#abstract-store-interface).

A *reader* is an implementation that is reading a SQLiteStore
file.

A *writer* is an implementation that is creating or modifying a
SQLiteStore file.

A *client* refers to software that uses an *implementation*, for example
by calling the functions defined by the implementation. Examples of clients
may include [zarr-python](https://zarr.readthedocs.io/en/stable/) and [zarrs](
https://zarrs.dev/).

## Store capabilities

SQLiteStore implements the [*Abstract store
interface*](https://zarr-specs.readthedocs.io/en/latest/v3/core/index.html#abstract-store-interface)
as defined by the *Zarr v3 core specification*. SQLiteStore supports the
capabilities **Readable**, **Writable** and **Listable**.  However, specific
implementations of SQLiteStore may choose to support only some of these
capabilities (for example, a read-only SQLiteStore implementation may support
only Readable and Listable).

## File format

Zarr SQLiteStore files must conform to the on-disk SQLite file format used by
SQLite releases 3.0.0 and later. This file format is described by the document
[*Database File Format*](https://sqlite.org/fileformat.html). Zarr SQLiteStore
files are therefore regular SQLite files which use a specific database schema,
defined by this document.

The file extension `.zarrdb` is recommended for Zarr SQLiteStore files.

### Minimal SQLite version

Files conforming to this specification require features introduced in SQLite
3.7.17.

### Application ID

The SQLite file format defines a 4-byte integer at offset 68 which can be used
to identify the application-specific file type. SQLiteStore files must have this
value set to the hexadecimal integer `0x10b50760`. Implementations may satisfy
this requirement by executing the following SQL pragma statement:

```sql
PRAGMA application_id = 0x10b50760
```

## Database schema

SQLiteStore files must contain two exactly tables, `zarr_sqlitestore_metadata` and
`zarr`. The schema for each table are described in the
subsections below. Records must not contain NULL values and the table schemas
should therefore be specified with a `NOT NULL` constraint on each column.

Readers must ignore all other tables present within the file.

### Table `zarr_sqlitestore_metadata`

Schema:

| Column | Type | Description |
|--------|------|-------------|
| k | `TEXT` | Metadata key |
| v | `TEXT` | Metadata value string |

The `zarr_sqlitestore_metadata` table stores metadata that is useful for the
SQLiteStore implementation.

The `zarr_sqlitestore_metadata` table must include records which are marked
*required* in the table below. Writers must not insert in the
`zarr_sqlitestore_metadata` table any record with a key that is not described here.
Readers must ignore any record with a key that is not described here.

| Key | Required | Description of value |
|-----|----------|----------------------|
| `sqlitestore_version` | Yes | SQLiteStore file format version, formatted as `MAJOR.MINOR`. For SQLiteStore version 1.0, this value must be the string "1.0". |
| `compatible_flags`    | Yes | Comma-separated list of flag strings. Reserved for future extensions to the store format. |
| `incompatible_flags`  | Yes | Comma-separated list of flag strings. Reserved for future extensions to the store format. |
| `created_by`  | No | Arbitrary string describing the software that wrote the file. |
| `modified_at`  | No | Timestamp indicating when the file was most recently modified in UTC, formatted as a string that conforms to [RFC3339](https://datatracker.ietf.org/doc/html/rfc3339), using an upper-case T to separate the date and time and an upper-case Z to represent the timezone e.g. `1999-12-31T23:59:59.999Z`. Note that this format is natively understood as a "time-value" by SQLite. |

Readers must reject files whose major version is unsupported, as specified by
the `sqlitestore_version` metadata record. Readers should ignore minor versions
greater than those they implement unless incompatible_flags contains unknown
values.

Compatible flags may be used in future versions of SQLiteStore to declare that
the store uses optional features but may still be read or written to by an
implementation that does not implement these features.

Incompatible flags may be used in future versions of SQLiteStore to declare that
the store uses optional features that must be supported by the implementation in
order to read or write the store.

Readers must:

1. Ignore all compatible flags it does not recognize;

2. Reject the file if any incompatible flag is not recognized.

In SQLiteStore version 1.0, no flags are defined. Writers must therefore set the
values of "compatible_flags" and "incompatible_flags" to the empty string
(`""`).

The `created_by` record is informational. Readers must not use it to determine
how the file is to be interpreted. The file format must be fully determined by
the metadata records sqlitestore_version, compatible_flags, and
incompatible_flags.

Writers are not required to create or update the modified_at field. Consequently,
readers should not rely on its presence or accuracy.

### Table `zarr`

Schema:

| Column | Type | Description |
|--------|------|-------------|
| k | `TEXT` | Primary key. The Zarr key (full path as a Unicode string). |
| v | `BLOB` | Binary value. The chunk or metadata content. |

The `zarr` table acts as a key-value store for the Zarr data.

Keys are stored exactly as provided by the Zarr client, without modification.
Per the *Abstract store interface* specification, keys are Unicode strings.
Implementations must preserve the logical Unicode string regardless of the
underlying SQLite database encoding. No Unicode normalization shall be
performed.

Writers must insert only valid keys in the zarr table. Valid keys are
those which respect the following rules:

1. The final character of the key must not be a `/` character.
2. The first character of the key must not be a `/` character.
3. A key must not contain the substring `//`.

The empty string is a valid key.

Implementations must preserve the key comparison semantics defined by the Zarr
core specification. In SQLite, this can be achieved by using the "BINARY"
collating function for all key comparisons.

Values are stored as binary blobs (SQLite type `BLOB`) exactly as provided by
the Zarr client, without modification.

## Database journaling mode

The default journaling mode of SQLite is `DELETE`, and this mode is recommended
for SQLiteStore files.

Implementations may use journal mode `WAL` (write-ahead log) for performance or
concurrency reasons, especially in write-heavy use cases. When using `WAL`
journal mode, implementations should attempt to restore the journal mode to
`DELETE` or perform a WAL checkpoint before the database is closed.

The purpose of this best-effort requirement is to ensure all data is moved by
SQLite from the WAL files (files suffixed with `-wal` and `-shm` automatically
created by SQLite) to the main database file, so that the SQLiteStore file
consists of a single self-contained file that can be safely shared.

## Limitations

The maximum size of a value that can be stored (for example, an array chunk) is
limited to 2147483645 bytes (3 bytes less than 2 GiB) by current SQLite
implementations.

## Appendix: Example SQL queries (non-normative)

This appendix provides example SQL statements that implement the fundamental
Zarr store operations described by the Zarr storage API. These examples are
informative only and are intended to illustrate one possible implementation.

Implementations may use different SQL statements or SQLite APIs provided they
exhibit equivalent externally observable behaviour.

In all examples below:

- `:key`, `:value`, `:prefix`, and similar identifiers denote bound SQL
  parameters.
- The `zarr` table is assumed to have the schema defined in this specification.
- Implementations SHOULD use prepared statements rather than constructing SQL
  strings dynamically.

Several operations in this appendix require selecting all keys that begin with a
given prefix. Throughout this appendix, such operations assume the following
parameter definitions:

- `:prefix` is the prefix to search for. `:prefix` must respect the following rules:
    * `:prefix` must not be the empty string (`''`)
    * `:prefix` must end with the slash character (`/`)
    * `:prefix` must not start with a slash character (`/`)
    * `:prefix` must not contain the substring (`//`)
- `:upper` is derived from `:prefix` by replacing the trailing slash character
  `/` with the character zero (`0`, U+0030). If `:prefix` is the empty string,
  Then `:upper` is omitted and no upper bound is required.

Because Zarr keys use `/` exclusively as a path separator, no valid key is equal
to a non-empty `:prefix`. Furthermore, under the required `BINARY` collating
function, every key beginning with `:prefix` compares lexicographically greater
than `:prefix` and less than `:upper`.

Thus, for non-empty prefixes, a prefix search may be implemented as:

```sql
WHERE k > :prefix
  AND k < :upper;
```

For an empty prefix, every key matches and the `WHERE` clause should therefore
be omitted.

### Read operations

#### `get`

Retrieve the value associated with a key.

```sql
SELECT v
FROM zarr
WHERE k = :key;
```

If no row is returned, the key does not exist.

---

#### `get_partial_values`

Retrieve one or more byte ranges from stored values.

Implementations may use the SQLite incremental BLOB API (`sqlite3_blob_open()` and related functions) for efficient partial reads of large values. Alternatively, implementations may use SQL functions such as `substr()`.

For example, a partial read using SQL may be performed as:

```sql
SELECT substr(v, :offset + 1, :length)
FROM zarr
WHERE k = :key;
```

where `offset` is zero-based and `length` is the number of bytes to retrieve.

The Zarr API permits multiple key/range pairs to be requested simultaneously. Implementations may satisfy these requests individually.

### Write operations

#### `set`

Insert or replace a key/value pair.

```sql
INSERT INTO zarr(k, v)
VALUES (:key, :value)
ON CONFLICT(k)
DO UPDATE SET v = excluded.v;
```

---

#### `set_partial_values`

Modify one or more byte ranges within an existing value.

Implementations may use the SQLite incremental BLOB API to update only the requested byte ranges without replacing the entire value.

If incremental BLOB access is not used, an implementation may instead read the existing value, modify the requested byte ranges in memory, and write the updated value using the `set` operation.

### Delete operations

#### `erase`

Delete a single key.

```sql
DELETE FROM zarr
WHERE k = :key;
```

---

#### `erase_values`

Delete multiple keys.

```sql
DELETE FROM zarr
WHERE k IN (...);
```

Implementations should use a parameterized statement with one bound parameter for each key.

---

#### `erase_prefix`

Delete every key having the specified prefix.

```sql
DELETE FROM zarr
WHERE k > :prefix
  AND k < :upper;
```

### Listing operations

#### `list`

Return every key in the store.

```sql
SELECT k
FROM zarr;
```

---

#### `list_prefix`

Return every key having a given prefix.

```sql
SELECT k
FROM zarr
WHERE k > :prefix
  AND k < :upper;
```

---

#### `list_dir`

The `list_dir` operation returns:

- keys immediately beneath the supplied prefix; and
- immediate child prefixes.

```sql
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
```
This query returns rows containing two TEXT columns: `type` and `path`. The
`type` column indicates whether the value in `path` represents a stored key
(`'key'`) or a child prefix (`'prefix'`).  Additionally, all returned prefixes
end in a trailing slash (`/`) character.

Alternatively, implementations may choose to implement `list_dir` by first
obtaining all keys returned by `list_prefix`. The returned keys can then be
processed by the implementation to separate immediate child keys from immediate
child prefixes according to the Zarr storage API semantics.

### SQLiteStore specific operations

#### Create database schema

The required database schema may be created using the following SQL statements:

```sql
CREATE TABLE IF NOT EXISTS zarr_sqlitestore_metadata (
    k TEXT PRIMARY KEY NOT NULL,
    v TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS zarr (
    k TEXT PRIMARY KEY NOT NULL,
    v BLOB NOT NULL
);
```

---

#### Metadata

The metadata records defined by this specification may be inserted using the
following SQL statements.

```sql
INSERT INTO zarr_sqlitestore_metadata(k, v)
VALUES
    ('sqlitestore_version', '1.0'),
    ('created_by', :created_by),
    ('compatible_flags', ''),
    ('incompatible_flags', '');
```

Where `:created_by` may be an arbitrary string that identifies the writer (not
required).

The `modified_at` timestamp record may be created or updated with the current
UTC time using:

```sql
INSERT INTO zarr_sqlitestore_metadata(k, v)
VALUES ('modified_at', strftime('%Y-%m-%dT%H:%M:%fZ', 'now', 'utc'))
ON CONFLICT(k)
DO UPDATE SET v = excluded.v;
```

---

#### Other timestamp operations

Because `modified_at` is stored in a format that is natively understood by
SQLite as a "time-value", SQLite's date and time functions can be used to
convert it into other representations. For example:

```sql
SELECT strftime('%m/%d/%Y %H:%M', v)
FROM zarr_sqlitestore_metadata
WHERE k = 'modified_at';
```

This query returns the timestamp formatted as `MM/DD/YYYY HH:MM`.

SQLite's `unixepoch()` function may be used to compute the elapsed time, in
seconds, since the `modified_at` timestamp:

```sql
SELECT unixepoch('now') - unixepoch(v)
FROM zarr_sqlitestore_metadata
WHERE k = 'modified_at';
```

Or the `julianday()` function to obtain the elapsed time as fractional number of
days:

```sql
SELECT julianday('now') - julianday(v)
FROM zarr_sqlitestore_metadata
WHERE k = 'modified_at';
```

# Zarr SQLiteStore specification version 1.0

## Scope

This document describes **SQLiteStore**, a [Zarr (v3)](https://zarr.dev/) store
that allows the storage of a complete Zarr hierarchy inside a single file by
using the SQLite database engine.

## Notes about the design decisions

SQLiteStore is designed primarily for sharing moderately sized datasets by storing an entire dataset in a single file.

Although SQLite supports databases several terabytes in size, no file size limit is defined by this specification. However, SQLiteStore is intended for datasets up to approximately 10 GB.

SQLite was chosen to accomplish this goal for the following reasons:

- SQLite has been in widespread use for a long time and is considered to be
  highly robust software.
- Unlike many archive formats, SQLite natively supports Zarr store operations such as key deletion, overwriting values, and modifying stored values.
- SQLite libraries are available for nearly all programming languages and
  computers.
  
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

## Store Capabilities

SQLiteStore implements the [*Abstract store
interface*](https://zarr-specs.readthedocs.io/en/latest/v3/core/index.html#abstract-store-interface)
as defined by the *Zarr v3 core specification*. SQLiteStore supports the
capabilities **Readable**, **Writable** and **Listable**.  However, specific
implementations of SQLiteStore may choose to support only some of these
capabilities (for example, a read-only SQLiteStore implementation may support
only Readable and Listable).

## Canonical URI

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

SQLiteStore files must contain two tables, as described below. The value columns
*v* must not contain NULL values and must therefore be specified with a
`NOT NULL` constraint in the table schemas.

Implementations may satisfy this requirement by executing the following SQL
statements:

```sql
CREATE TABLE IF NOT EXISTS sqlitestore_metadata(
  k TEXT PRIMARY KEY,
  v TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS zarr(
  k TEXT PRIMARY KEY,
  v BLOB NOT NULL
);
```

Readers must ignore all other tables present within the file.

### Table `sqlitestore_metadata`

Schema:

| Column | Type | Description |
|--------|------|-------------|
| k | `TEXT` | Metadata key |
| v | `TEXT` | Metadata value string |

The `sqlitestore_metadata` table stores metadata that is useful for the
SQLiteStore implementation.

The `sqlitestore_metadata` table must include records which are marked
*required* in the table below. Writers must not insert in the
`sqlitestore_metadata` table any record with a key that is not described here.
Readers must ignore any record with a key that is not described here.

| Key | Required | Description of value |
|-----|----------|----------------------|
| `sqlitestore_version` | Yes | SQLiteStore file format version, formatted as `MAJOR.MINOR`. For SQLiteStore version 1.0, this value must be the string "1.0". |
| `compatible_flags`    | Yes | Comma-separated list of flag strings. Reserved for future extensions to the store format. |
| `incompatible_flags`  | Yes | Comma-separated list of flag strings. Reserved for future extensions to the store format. |
| `created_by`  | No | Arbitrary string describing the software that wrote the file. |
| `created_time`  | No | Timestamp indicating when the file was most recently modified, formatted as a string that conforms to [RFC3339](https://datatracker.ietf.org/doc/html/rfc3339). |

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

### Table `zarr`

Schema:

| Column | Type | Description |
|--------|------|-------------|
| k | `TEXT` | Primary key. The Zarr key (full path as a Unicode string). |
| v | `BLOB` | Binary value. The chunk or metadata content. |

The `zarr` table acts as a key-value store for the Zarr data.

Keys are stored exactly as provided by the Zarr client, without modification.
Per the *Abstract store interface* specification, keys are Unicode strings.
Implementations shall preserve the logical Unicode string regardless of the
underlying SQLite database encoding. No Unicode normalization shall be
performed. Keys must be compared using SQLite's native TEXT comparison, for
example when running `SELECT` queries for key lookup.

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

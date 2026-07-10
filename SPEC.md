# Zarr SQLiteStore Specification Version 1.0

## Context

SQLiteStore is a single-file storage backend for [Zarr (v3)](https://zarr.dev/)
that stores array metadata and chunk data in an SQLite database.

Key advantages of SQLiteStore over alternative single-file formats (e.g., Zip):

  - Full support for key deletion, overwriting, and partial value writes.
  - Full ACID guarantees provided by SQLite.
  - High availability of SQLite implementations across programming languages
    and environments.

SQLiteStore implements the [*Abstract store
interface*](https://zarr-specs.readthedocs.io/en/latest/v3/core/index.html#abstract-store-interface)
as defined by the *Zarr v3 core specification*. SQLiteStore as defined by the
current document is a store implementation which is **Readable**, **Writeable**
and **Listable**.  However, implementations of SQLiteStore may choose to support
only some of these capabilities (for example, an SQLiteStore implementation may
support only Readable and Listable).

## File Format

Zarr SQLiteStore files MUST conform to the on-disk SQLite file format used by
SQLite releases 3.0.0 and later. This file format is described by the document
[*Database File Format*](https://sqlite.org/fileformat.html). Zarr SQLiteStore
files are therefore regular SQLite files, but are structured with a specific
schema, defined by this document.

The file extension `.zarrdb` is **recommended** for Zarr SQLiteStore files.

## Database schema

The database contains two tables, as described below.

The schema may be created by the following SQL statements:

```sql
CREATE TABLE IF NOT EXISTS sqlitestore_metadata(k TEXT PRIMARY KEY, v TEXT);
CREATE TABLE IF NOT EXISTS zarr(k TEXT PRIMARY KEY, v BLOB);
```

### Table `sqlitestore_metadata`

| Column | Type | Description |
|--------|------|-------------|
| k | `TEXT` | Primary key. The Zarr key (full path as unicode string). |
| v | `TEXT` | Binary value. The chunk or metadata content. |

The `sqlitestore_metadata` table stores metadata that is useful for the SQLiteStore
implementation.

The `sqlitestore_metadata` table MUST include the following records.

| Key | Description of value |
|-----|----------------------|
| `sqlitestore_version` | Version of the SQLiteStore file format in `MAJOR.MINOR` format. As of the present document, this should be the string "1.0". |
| `compatible_flags`    | List of flag strings separated by a comma (`0x2C`) character. Reserved for future extensions to the store format. |
| `incompatible_flags`  | List of flag strings separated by a comma (`0x2C`) character. Reserved for future extensions to the store format. |

Compatible flags are used to declare that the store uses optional features but
may still be read or written to by an implementation which does not implement
these features.

Incompatible flags are used to declare that the store uses optional features
that must be supported by the implementation in order to read or write the
store.

### Table `zarr`

| Column | Type | Description |
|--------|------|-------------|
| k | `TEXT` | Primary key. The Zarr key (full path as unicode string). |
| v | `BLOB` | Binary value. The chunk or metadata content. |

The `zarr` table acts as a key-value store for the Zarr data.

Keys are stored exactly as provided by the Zarr client, without modification.
Per the *Abstract store interface* specification, keys are Unicode strings. Keys
may be stored in any of the Unicode encodings supported by SQLite (database
encoding).

Values are stored as binary blobs (SQLite type `BLOB`) exactly as provided by
the Zarr client, without modification.

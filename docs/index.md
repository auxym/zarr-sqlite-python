# zarr-sqlite

Single-file [Zarr](https://zarr.dev/) v3 dataset store using SQLite.

## Overview

`SQLiteStore` provides a single-file storage backend for Zarr. It stores a
complete Zarr hierarchy inside a single SQLite database file, providing
full ACID guarantees and support for key deletion, overwriting, and partial
value writes.

Key advantages over alternative single-file formats (e.g., `ZipStore`):

- Support for key deletion, overwriting, and partial value writes.
- Full ACID guarantees provided by SQLite.
- High availability of SQLite implementations across programming languages
  and environments.

## Specification

The store format is described in the [Specification](https://github.com/auxym/zarr-sqlite/blob/main/SPEC.md). This document should allow the implementation of SQLiteStore for other programming languages or Zarr libraries.

## Installation

Install from PyPI using `pip` or `uv`:

```bash
pip install zarr-sqlite
```

```bash
uv pip install zarr-sqlite
```

## Quick start

```python
import zarr

from zarr_sqlite import SQLiteStore

with SQLiteStore("my_zarr_file.zarrdb") as store:
    root = zarr.create_group(store=store)
    foo = root.create_group("foo")
    bar = foo.create_group("bar")
    z1 = bar.create_array(name="baz", shape=(10000, 10000), chunks=(1000, 1000), dtype="int32")
    z1[:] = 42
```

`SQLiteStore` otherwise behaves identically to other stores used with zarr. See
the [zarr user guide](https://zarr.readthedocs.io/en/stable/user-guide/storage.html)
for more information.

## Status

The version of this library was incremented to v1.0 to reflect compliance with
v1 of the SQLiteStore Specification. However, this is a relatively new library
that has not seen a lot of real-world use yet, and users should expect the
occasional bug, and possibly breaking API changes in future versions, if
absolutely necessary.

The database format, defined by the Specification, can be expected to be stable.

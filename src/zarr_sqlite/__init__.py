"""Python library which provdes a single-file store implementation for
[zarr-python](https://zarr.readthedocs.io/en/stable/), backed by SQLite.

`SQLiteStore` allows you to store Zarr datasets in a single
SQLite file. Compared to zarr-python's built-in
[ZipStore](https://zarr.readthedocs.io/en/stable/api/zarr/storage/#zarr.storage.ZipStore),
`SQLiteStore` supports key deletion and overwriting and provides ACID guarantees
(thanks to the underlying SQLite database). 

## Installation

Install from PyPI using your favorite package manager, for example:

```bash
pip install zarr-sqlite
```

```bash
uv add zarr-sqlite
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

`SQLiteStore` otherwise behaves identically to other stores used with zarr.
See the [zarr-python user guide](https://zarr.readthedocs.io/en/stable/user-guide/storage.html)
for more information.

## Specification

The file format is described by a [specification stored in this project's
repository](https://github.com/auxym/zarr-sqlite/blob/main/SPEC.md).  This
document should allow the implementation of SQLiteStore for other programming
languages or Zarr libraries.
"""

from .zarr_sqlite import SQLiteStore

from ._version import __version__

__all__ = ["__version__", "SQLiteStore"]

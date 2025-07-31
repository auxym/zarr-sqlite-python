# zarr-sqlite

Experimental implementation of a SQLite-based store for [zarr](https://zarr.dev/) v3, in python.

Example usage:

```python
import zarr

from zarr_sqlite import SQLiteStore

with SQLiteStore("my_zarr_file.sqlite") as store:
    root = zarr.create_group(store=store)
    foo = root.create_group('foo')
    bar = foo.create_group('bar')
    z1 = bar.create_array(name='baz', shape=(10000, 10000), chunks=(1000, 1000), dtype='int32')
    z1[:] = 42
```

`SQLiteStore` otherwise behaves identically to other stores used with zarr, see
the [zarr user guide](https://zarr.readthedocs.io/en/stable/user-guide/storage.html)
for more information.

# zarr-sqlite-python

**Store [Zarr](zarr.dev) datasets in a single SQLite database.**

`zarr-sqlite` is a Python library which provides `SQLiteStore`, a single-file store implementation
backed by SQLite for [zarr-python](https://zarr.readthedocs.io/en/stable/). It combines Zarr's chunked, hierarchical data model with SQLite's single-file database format, mutability and ACID guarantees.

## Why SQLiteStore?

* Single file — complete Zarr hierarchy is stored in one .zarrdb file.
* ACID transactions — writes are backed by SQLite's transactional database
  engine. A crash or power loss during a write cannot corrupt the entire file,
  at most, a single chunk may be lost. Readers can safely access the store
  concurrently with writers and are guaranteed to see a consistent view of the
  data.
* Mutable storage — arrays can be modified, overwritten, resized, appended to 
  and deleted.

## Installation

Install from PyPI using your favorite package manager, for example:

```
pip install zarr-sqlite
```

```
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

## Documentation

There isn't much more to using `SQLiteStore` as it "just works" with
zarr-python. Full API reference can be accessed by running `uv run pdoc -d
numpy` (or `just doc` if you have [just](https://just.systems/) installed).

A copy of the API documentation for the current *main* branch is also hosted here:

**[Documentation for zarr_sqlite](https://auxym.github.io/zarr-sqlite-python)**

## How it works

`SQLiteStore` implements the [abstract store
interface](https://zarr-specs.readthedocs.io/en/latest/v3/core/index.html#abstract-store-interface)
defined by the Zarr Core Specification, which essentially provides a key-value
storage interface. It maps this interface directly onto a SQLite table, using
string keys and binary BLOB values. Keys correspond to Zarr paths, while values
contain either array chunks or Zarr metadata items. SQLite then provides the
persistence and transactional semantics underneath the Zarr store.


## Specification

The file format is described by a [specification stored in this project's
repository](./SPEC.md).  This document should allow the implementation of
SQLiteStore for other programming languages or Zarr libraries.

## Status

The version of this library was incremented to v1.0 to reflect the fact that it
complies with v1 of the SQLiteStore Specification. However, this is a relatively
young library that has not seen a lot of real-world use yet, therefore users
should expect the occasional bug, and possibly breaking API changes in future
versions, if absolutely necessary.

The database format defined by the [Specification](./SPEC.md) is intended to provide
a stable basis for interoperability between implementations.

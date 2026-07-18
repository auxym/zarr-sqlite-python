"""Test integration with zarr library"""

import os
import tempfile
from pathlib import Path
import pytest
import numpy as np
import zarr
from zarr_sqlite import SQLiteStore


@pytest.fixture
def temp_db_file():
    tmp_db = tempfile.NamedTemporaryFile(
        suffix=".db", delete=False, delete_on_close=False
    )
    tmp_db.close()
    yield tmp_db.name
    os.remove(tmp_db.name)


@pytest.fixture
def sqlite_store(temp_db_file):
    store = SQLiteStore(temp_db_file)
    yield store
    store.close()


def random_array(shape, dtype=np.float64):
    return np.random.default_rng().random(shape, dtype)


def test_open_close(temp_db_file):
    store = SQLiteStore.open(temp_db_file)
    store.close()


def test_store_array(sqlite_store):
    z = zarr.create_array(
        store=sqlite_store, shape=(100, 100), chunks=(10, 10), dtype="f4"
    )
    data = random_array((100, 100), dtype=np.float32)
    z[:, :] = data
    read_back = z[:]
    assert np.array_equal(read_back, data)


def test_create_group(sqlite_store):
    root = zarr.create_group(store=sqlite_store)
    group1 = root.create_group("group1")

    assert isinstance(root, zarr.Group)
    assert isinstance(group1, zarr.Group)
    assert "group1" in root
    assert isinstance(root["group1"], zarr.Group)


def test_save_array_to_group(temp_db_file):
    with SQLiteStore(temp_db_file) as sqlite_store:
        root = zarr.create_group(store=sqlite_store)
        group1 = root.create_group("group1")
        z = group1.create_array(shape=(100, 100), chunks=(10, 10), dtype="f4", name="z")

        data = random_array((100, 100), dtype=np.float32)
        z[:, :] = data

    del root, group1, z, sqlite_store

    with SQLiteStore(temp_db_file) as sqlite_store:
        root = zarr.open_group(store=sqlite_store)
        assert np.array_equal(data, root["group1/z"][:])


@pytest.mark.skip(reason="assumed bug in zarr-python")
def test_delete_array(temp_db_file):
    with SQLiteStore(temp_db_file) as sqlite_store:
        root = zarr.create_group(store=sqlite_store)
        group1 = root.create_group("group1")
        group1.create_array(shape=(100, 100), chunks=(10, 10), dtype="f4", name="z")

    with SQLiteStore(temp_db_file) as sqlite_store:
        root = zarr.open_group(store=sqlite_store)
        assert "group1/z" in root
        assert root["group1/z"].shape == (100, 100)
        del root["group1/z"]

    with SQLiteStore(temp_db_file) as sqlite_store:
        root = zarr.open_group(store=sqlite_store)
        assert "group1" in root
        assert isinstance(root["group1"], zarr.Group)
        assert "group1/z" not in root
        with pytest.raises(KeyError):
            root["group1/z"][:]


def test_append_array(sqlite_store):
    z = zarr.create_array(
        store=sqlite_store, shape=(100, 100), chunks=(10, 10), dtype="f4"
    )
    data = random_array((100, 100), dtype=np.float32)
    z[:, :] = data

    z.append(data, axis=0)

    assert z.shape == (200, 100)
    expected = np.tile(data, (2, 1))
    assert np.array_equal(z[:], expected)


def test_group_listing_methods(sqlite_store):
    root = zarr.create_group(store=sqlite_store)

    a = root.create_group("a")
    a.create_array(shape=(10, 10), chunks=(10, 10), dtype="f4", name="array_a")
    sub = a.create_group("sub")
    sub.create_array(shape=(5, 5), chunks=(5, 5), dtype="f4", name="array_b")

    b = root.create_group("b")
    b.create_array(shape=(3, 3), chunks=(3, 3), dtype="f4", name="array_c")

    root = zarr.open_group(store=sqlite_store)

    assert set(root.keys()) == {"a", "b"}
    assert set(root.array_keys()) == set()
    assert set(root.group_keys()) == {"a", "b"}

    assert set(k for k, _ in root.groups()) == {"a", "b"}
    assert all(isinstance(g, zarr.Group) for _, g in root.groups())

    a = root["a"]
    assert set(a.keys()) == {"array_a", "sub"}
    assert set(a.array_keys()) == {"array_a"}
    assert set(a.group_keys()) == {"sub"}
    assert len(list(a.array_values())) == 1
    assert set(k for k, _ in a.arrays()) == {"array_a"}
    assert all(isinstance(arr, zarr.Array) for arr in a.array_values())

    sub = a["sub"]
    assert set(sub.array_keys()) == {"array_b"}
    assert len(list(sub.array_values())) == 1

    assert set(g.name for g in root.group_values()) == {"/a", "/b"}
    assert all(isinstance(g, zarr.Group) for g in root.group_values())

    assert set(k for k, _ in root.groups()) == {"a", "b"}
    assert isinstance(root["a"], zarr.Group)
    assert isinstance(root["b"], zarr.Group)
    assert isinstance(root["a/array_a"], zarr.Array)
    assert isinstance(root["a/sub/array_b"], zarr.Array)

    assert set(root) == {"a", "b"}


def test_store_array_creates_file_and_persists():
    """Ensure file is created automatically when it doesn't exist"""
    with tempfile.TemporaryDirectory() as tmpdir:
        fname = Path(tmpdir) / "test1.zarrdb"
        store = SQLiteStore(fname, read_only=False)
        root = zarr.open(store=store, mode="a")
        group1 = root.create_group("group1")

        i = np.arange(80000) / 60.0
        x = i + np.random.normal(size=len(i), scale=1.5)
        y = (
            np.sin(x / 13)
            + 0.7 * np.cos(x / 47)
            + np.random.normal(size=len(i), scale=0.05)
        )
        x.shape = (400, 200)
        y.shape = (200, 400)

        group1.create_array(data=x, name="xdat")
        group1.create_array(data=y, name="ydat")
        store.close()

        read_root = zarr.open(store=SQLiteStore(fname, read_only=True), mode="r")
        assert np.array_equal(read_root["group1/ydat"][:], y)
        assert np.array_equal(read_root["group1/xdat"], x)

        # Ensure store is closed so tmpdir can be safely deleted
        read_root.store.close()

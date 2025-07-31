"""Test integration with zarr library"""

import os

import numpy as np

import pytest

import zarr

from tempfile import NamedTemporaryFile

from zarr_sqlite import SQLiteStore


@pytest.fixture
def temp_db_file():
    tmp_db = NamedTemporaryFile(suffix=".db", delete=False, delete_on_close=False)
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
        z = group1.create_array(
            shape=(100, 100), chunks=(10, 10), dtype="f4", name="z"
        )

        data = random_array((100, 100), dtype=np.float32)
        z[:, :] = data

    del root, group1, z, sqlite_store

    with SQLiteStore(temp_db_file) as sqlite_store:
        root = zarr.open_group(store=sqlite_store)
        assert np.array_equal(data, root["group1/z"][:])

def test_delete_array(temp_db_file):
    with SQLiteStore(temp_db_file) as sqlite_store:
        root = zarr.create_group(store=sqlite_store)
        group1 = root.create_group("group1")
        group1.create_array(
            shape=(100, 100), chunks=(10, 10), dtype="f4", name="z"
        )

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

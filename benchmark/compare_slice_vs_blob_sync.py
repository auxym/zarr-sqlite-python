#!/usr/bin/env python3

import argparse
import os
import sqlite3
import statistics
import time

DB_NAME = "blob_benchmark.db"
TABLE_NAME = "zarr"
KEY = "blob"

BLOB_SIZE = 10 * 1024 * 1024      # 10 MiB
READ_SIZE = 1 * 1024 * 1024       # 1 MiB
READ_OFFSET = BLOB_SIZE - READ_SIZE


def setup_db():
    """Create the benchmark database if it does not already exist."""
    con = sqlite3.connect(DB_NAME)
    cur = con.cursor()

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            k TEXT PRIMARY KEY,
            v BLOB NOT NULL
        )
    """)

    cur.execute(f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE k=?", (KEY,))
    exists = cur.fetchone()[0]

    if not exists:
        print("Creating 10 MiB blob...")
        blob = os.urandom(BLOB_SIZE)
        cur.execute(
            f"INSERT INTO {TABLE_NAME}(k, v) VALUES (?, ?)",
            (KEY, blob),
        )
        con.commit()

    con.close()


def benchmark_query(con):
    cur = con.cursor()

    t0 = time.perf_counter()

    cur.execute(f"SELECT v FROM {TABLE_NAME} WHERE k=?", (KEY,))
    data = cur.fetchone()[0]
    data = data[READ_OFFSET:]

    t1 = time.perf_counter()

    assert len(data) == READ_SIZE
    return t1 - t0


def benchmark_blob(con):
    cur = con.cursor()

    cur.execute(f"SELECT rowid FROM {TABLE_NAME} WHERE k=?", (KEY,))
    rowid = cur.fetchone()[0]

    t0 = time.perf_counter()

    blob = con.blobopen(TABLE_NAME, "v", rowid, readonly=True)
    try:
        blob.seek(READ_OFFSET)
        data = blob.read(READ_SIZE)
    finally:
        blob.close()

    t1 = time.perf_counter()

    assert len(data) == READ_SIZE
    return t1 - t0


def run(name, func, con, rounds):
    times = []

    for _ in range(rounds):
        times.append(func(con))

    print(f"\n{name}")
    print("-" * len(name))
    print(f"Rounds : {rounds}")
    print(f"Mean   : {statistics.mean(times) * 1000:.3f} ms")
    print(f"Median : {statistics.median(times) * 1000:.3f} ms")
    print(f"Min    : {min(times) * 1000:.3f} ms")
    print(f"Max    : {max(times) * 1000:.3f} ms")


def main():
    parser = argparse.ArgumentParser(
        description="Benchmark SQLite BLOB reads."
    )
    parser.add_argument(
        "-n",
        "--rounds",
        type=int,
        default=10,
        help="Number of benchmark rounds (default: 10)",
    )
    parser.add_argument(
        "--blob-only",
        action="store_true",
        help="Run only the blob benchmark (useful with cProfile).",
    )

    args = parser.parse_args()

    setup_db()

    con = sqlite3.connect(DB_NAME)

    if args.blob_only:
        run("Blob API", benchmark_blob, con, args.rounds)
    else:
        run("SELECT + slice", benchmark_query, con, args.rounds)
        run("Blob API", benchmark_blob, con, args.rounds)

    con.close()


if __name__ == "__main__":
    main()
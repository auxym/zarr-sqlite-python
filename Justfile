test:
    uv run pytest

check:
    uv run ruff check
    uv run mypy src/

build-docs:
    uv run pdoc -o site -d numpy zarr_sqlite

doc:
    uv run pdoc -d numpy zarr_sqlite

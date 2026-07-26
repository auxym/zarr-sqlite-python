from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("zarr_sqlite")
except PackageNotFoundError:
    # Package is not installed (e.g. running from source)
    __version__ = "unknown"

def is_in_memory_database(database: str) -> bool:
    """Check whether a database specifier refers to an in-memory database.

    Args:
        database: The SQLite database specifier (path, URI, or ":memory:").

    Returns:
        True if the database is in-memory, False otherwise.
    """
    return database == ":memory:" or (
        database.startswith("file:/") and "mode=memory" in database
    )


def is_database_uri(database: str) -> bool:
    """Check whether a database specifier is a SQLite URI.

    SQLite URIs use the ``file:`` scheme, e.g. ``file:/path/to/db`` or
    ``file::memory:?mode=memory``.

    Args:
        database: The SQLite database specifier.

    Returns:
        True if the database is a URI, False otherwise.
    """
    return database.startswith("file:/")

"""Database connection and introspection utilities.

Supports PostgreSQL and SQLite backends. Provides a unified interface
for extracting schema metadata (tables, columns, indexes, constraints)
regardless of the underlying database engine.
"""

from __future__ import annotations

import sqlite3
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

try:
    import psycopg2
    import psycopg2.extras
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False


class UnsupportedDialectError(Exception):
    """Raised when the database URL uses an unsupported dialect."""


class ConnectionError(Exception):  # noqa: A001
    """Raised when a database connection cannot be established."""


def _detect_dialect(url: str) -> str:
    """Return the dialect string ('postgresql' or 'sqlite') from a URL.

    Args:
        url: A database connection URL, e.g. ``postgresql://user:pass@host/db``
             or ``sqlite:///path/to/db.sqlite3``.

    Returns:
        One of ``'postgresql'`` or ``'sqlite'``.

    Raises:
        UnsupportedDialectError: If the URL scheme is not recognised.
    """
    scheme = urlparse(url).scheme.lower()
    if scheme in ("postgresql", "postgres", "pg"):
        return "postgresql"
    if scheme in ("sqlite", "sqlite3", ""):
        return "sqlite"
    raise UnsupportedDialectError(
        f"Unsupported database dialect '{scheme}'. "
        "Supported dialects: postgresql, sqlite."
    )


# ---------------------------------------------------------------------------
# PostgreSQL introspection
# ---------------------------------------------------------------------------

_PG_COLUMNS_SQL = """
    SELECT
        c.table_name,
        c.column_name,
        c.data_type,
        c.character_maximum_length,
        c.is_nullable,
        c.column_default
    FROM information_schema.columns c
    WHERE c.table_schema = 'public'
    ORDER BY c.table_name, c.ordinal_position;
"""

_PG_INDEXES_SQL = """
    SELECT
        t.relname  AS table_name,
        i.relname  AS index_name,
        ix.indisunique AS is_unique,
        array_agg(a.attname ORDER BY a.attnum) AS columns
    FROM pg_class t
    JOIN pg_index ix ON t.oid = ix.indrelid
    JOIN pg_class i  ON i.oid = ix.indexrelid
    JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
    JOIN pg_namespace n ON n.oid = t.relnamespace
    WHERE n.nspname = 'public'
      AND t.relkind = 'r'
    GROUP BY t.relname, i.relname, ix.indisunique
    ORDER BY t.relname, i.relname;
"""


def _introspect_postgresql(url: str) -> Dict[str, Any]:
    """Return a schema dict for a PostgreSQL database.

    Args:
        url: A ``postgresql://`` connection URL.

    Returns:
        A dict with keys ``'tables'`` and ``'indexes'``, where ``'tables'``
        maps table names to lists of column metadata dicts and ``'indexes'``
        maps table names to lists of index metadata dicts.

    Raises:
        ImportError: If *psycopg2* is not installed.
        ConnectionError: If the database cannot be reached.
    """
    if not HAS_PSYCOPG2:
        raise ImportError(
            "psycopg2 is required for PostgreSQL support. "
            "Install it with: pip install psycopg2-binary"
        )
    try:
        conn = psycopg2.connect(url)
    except psycopg2.OperationalError as exc:
        raise ConnectionError(f"Cannot connect to PostgreSQL: {exc}") from exc

    schema: Dict[str, Any] = {"tables": {}, "indexes": {}}
    with conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(_PG_COLUMNS_SQL)
            for row in cur.fetchall():
                tbl = row["table_name"]

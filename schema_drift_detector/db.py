"""Database introspection helpers for supported dialects.

Currently supports PostgreSQL and SQLite. Extend `_DIALECT_MAP` and add
an introspection function to support additional databases.
"""

from __future__ import annotations

import sqlite3
from typing import Any, Dict, List, Optional

try:
    import psycopg2
    import psycopg2.extras
    _PSYCOPG2_AVAILABLE = True
except ImportError:  # pragma: no cover
    _PSYCOPG2_AVAILABLE = False


class UnsupportedDialectError(Exception):
    """Raised when the connection URL uses an unrecognised database dialect."""


class ConnectionError(Exception):  # noqa: A001  (shadows built-in intentionally)
    """Raised when a database connection cannot be established."""


# ---------------------------------------------------------------------------
# Dialect detection
# ---------------------------------------------------------------------------

def _detect_dialect(url: str) -> str:
    """Return a normalised dialect string from a connection *url*.

    >>> _detect_dialect("postgresql://user:pw@localhost/db")
    'postgresql'
    >>> _detect_dialect("sqlite:///local.db")
    'sqlite'
    """
    lower = url.lower()
    if lower.startswith(("postgresql://", "postgres://")):
        return "postgresql"
    if lower.startswith("sqlite://"):
        return "sqlite"
    raise UnsupportedDialectError(
        f"Cannot determine dialect from URL: {url!r}. "
        "Supported prefixes: postgresql://, postgres://, sqlite://"
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
JOIN information_schema.tables t
    ON t.table_name = c.table_name
   AND t.table_schema = c.table_schema
WHERE c.table_schema = %s
  AND t.table_type = 'BASE TABLE'
ORDER BY c.table_name, c.ordinal_position;
"""


def _introspect_postgresql(url: str, schema: str = "public") -> Dict[str, Any]:
    """Return a schema dict for a PostgreSQL database.

    Parameters
    ----------
    url:
        A libpq-compatible connection string, e.g.
        ``postgresql://user:pw@host:5432/dbname``.
    schema:
        The PostgreSQL schema (namespace) to introspect. Defaults to
        ``"public"``.

    Returns
    -------
    dict
        ``{table_name: {column_name: {type, nullable, default, ...}}}``
    """
    if not _PSYCOPG2_AVAILABLE:  # pragma: no cover
        raise ConnectionError(
            "psycopg2 is required for PostgreSQL support. "
            "Install it with: pip install psycopg2-binary"
        )
    try:
        conn = psycopg2.connect(url)
    except Exception as exc:  # psycopg2.OperationalError etc.
        raise ConnectionError(f"Could not connect to PostgreSQL: {exc}") from exc

    result: Dict[str, Any] = {}
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(_PG_COLUMNS_SQL, (schema,))
            for row in cur.fetchall():
                table = row["table_name"]
                col = row["column_name"]
                result.setdefault(table, {})[col] = {
                    "type": row["data_type"],
                    "max_length": row["character_maximum_length"],
                    "nullable": row["is_nullable"] == "YES",
                    "default": row["column_default"],
                }
    finally:
        conn.close()

    return result


# ---------------------------------------------------------------------------
# SQLite introspection
# ---------------------------------------------------------------------------

def _introspect_sqlite(url: str) -> Dict[str, Any]:
    """Return a schema dict for a SQLite database.

    Parameters
    ----------
    url:
        A SQLite connection URL of the form ``sqlite:///path/to/file.db``
        or ``sqlite:///:memory:``.
    """
    # Strip the scheme prefix to get the raw file path.
    path = url[len("sqlite:///"):] if url.startswith("sqlite:///") else ":memory:"
    try:
        conn = sqlite3.connect(path)
    except sqlite3.OperationalError as exc:
        raise ConnectionError(f"Could not open SQLite database {path!r}: {exc}") from exc

    result: Dict[str, Any] = {}
    try:
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
        tables: List[str] = [row[0] for row in cur.fetchall()]
        for table in tables:
            cur.execute(f"PRAGMA table_info({table});")
            columns = {}
            for row in cur.fetchall():
                # row: (cid, name, type, notnull, dflt_value, pk)
                columns[row[1]] = {
                    "type": row[2].upper() if row[2] else "TEXT",
                    "max_length": None,
                    "nullable": not bool(row[3]),
                    "default": row[4],
                }
            result[table] = columns
    finally:
        conn.close()

    return result


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def introspect(url: str, pg_schema: str = "public") -> Dict[str, Any]:
    """Introspect a database and return its schema as a plain dict.

    Parameters
    ----------
    url:
        Database connection URL.  Supported schemes: ``postgresql://``,
        ``postgres://``, ``sqlite://``.
    pg_schema:
        PostgreSQL schema/namespace to inspect (ignored for SQLite).

    Returns
    -------
    dict
        ``{table_name: {column_name: {type, nullable, default, max_length}}}``
    """
    dialect = _detect_dialect(url)
    if dialect == "postgresql":
        return _introspect_postgresql(url, schema=pg_schema)
    if dialect == "sqlite":
        return _introspect_sqlite(url)
    # Should be unreachable given _detect_dialect's guard, but keeps mypy happy.
    raise UnsupportedDialectError(dialect)  # pragma: no cover

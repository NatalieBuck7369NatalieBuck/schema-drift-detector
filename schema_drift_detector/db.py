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
    """Return the dialect string ('postgresql' or 'sqlite') from a URL."""
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
    """Return a schema dict for a PostgreSQL database."""
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
                schema["tables"].setdefault(tbl, {"columns": {}})
                schema["tables"][tbl]["columns"][row["column_name"]] = {
                    "type": row["data_type"],
                    "max_length": row["character_maximum_length"],
                    "nullable": row["is_nullable"] == "YES",
                    "default": row["column_default"],
                }

            cur.execute(_PG_INDEXES_SQL)
            for row in cur.fetchall():
                tbl = row["table_name"]
                schema["indexes"].setdefault(tbl, {})
                schema["indexes"][tbl][row["index_name"]] = {
                    "unique": row["is_unique"],
                    "columns": list(row["columns"]),
                }
    conn.close()
    return schema


# ---------------------------------------------------------------------------
# SQLite introspection
# ---------------------------------------------------------------------------

def _introspect_sqlite(url: str) -> Dict[str, Any]:
    """Return a schema dict for a SQLite database."""
    # Accept both 'sqlite:///path' and bare file paths.
    parsed = urlparse(url)
    db_path = parsed.path or parsed.netloc or url
    if db_path.startswith("///"):
        db_path = db_path[2:]  # keep leading /
    elif db_path.startswith("//"):
        db_path = db_path[2:]

    try:
        conn = sqlite3.connect(db_path)
    except sqlite3.OperationalError as exc:
        raise ConnectionError(f"Cannot open SQLite database '{db_path}': {exc}") from exc

    schema: Dict[str, Any] = {"tables": {}, "indexes": {}}
    cur = conn.cursor()

    cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
    tables: List[str] = [row[0] for row in cur.fetchall() if row[0] != "sqlite_sequence"]

    for tbl in tables:
        schema["tables"][tbl] = {"columns": {}}
        cur.execute(f"PRAGMA table_info('{tbl}');")
        for col in cur.fetchall():
            # cid, name, type, notnull, dflt_value, pk
            schema["tables"][tbl]["columns"][col[1]] = {
                "type": col[2],
                "max_length": None,
                "nullable": not bool(col[3]),
                "default": col[4],
            }

        schema["indexes"][tbl] = {}
        cur.execute(f"PRAGMA index_list('{tbl}');")
        for idx in cur.fetchall():
            # seq, name, unique, origin, partial
            idx_name: str = idx[1]
            cur.execute(f"PRAGMA index_info('{idx_name}');")
            idx_cols = [r[2] for r in cur.fetchall()]
            schema["indexes"][tbl][idx_name] = {
                "unique": bool(idx[2]),
                "columns": idx_cols,
            }

    conn.close()
    return schema


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def introspect(url: str) -> Dict[str, Any]:
    """Introspect a database and return its schema as a plain dictionary.

    Parameters
    ----------
    url:
        Database connection URL.  Examples::

            postgresql://user:pass@localhost:5432/mydb
            sqlite:///path/to/db.sqlite3

    Returns
    -------
    dict
        A nested dict with keys ``'tables'`` and ``'indexes'``.
    """
    dialect = _detect_dialect(url)
    if dialect == "postgresql":
        return _introspect_postgresql(url)
    return _introspect_sqlite(url)

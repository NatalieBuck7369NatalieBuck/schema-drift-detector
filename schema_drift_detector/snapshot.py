"""Snapshot module for capturing and persisting database schema state.

This module handles connecting to a database, introspecting its schema,
and serializing the result to a versioned snapshot file on disk.
"""

import json
import hashlib
import datetime
from pathlib import Path
from typing import Any

import sqlalchemy
from sqlalchemy import inspect, text


DEFAULT_SNAPSHOT_DIR = Path(".schema_snapshots")


def get_schema(engine: sqlalchemy.engine.Engine) -> dict[str, Any]:
    """Introspect a live database and return a structured schema dict.

    Args:
        engine: A SQLAlchemy engine connected to the target database.

    Returns:
        A dict keyed by table name, each value containing columns,
        primary keys, foreign keys, indexes, and unique constraints.

    Raises:
        sqlalchemy.exc.OperationalError: If the database cannot be reached
            or the connection is refused.
        sqlalchemy.exc.NoSuchTableError: If introspection encounters a table
            that disappears mid-inspection (e.g. in a concurrent environment).
    """
    inspector = inspect(engine)
    schema: dict[str, Any] = {}

    for table_name in sorted(inspector.get_table_names()):
        columns = [
            {
                "name": col["name"],
                "type": str(col["type"]),
                "nullable": col["nullable"],
                "default": str(col["default"]) if col["default"] is not None else None,
            }
            for col in inspector.get_columns(table_name)
        ]

        pk_constraint = inspector.get_pk_constraint(table_name)
        foreign_keys = [
            {
                "constrained_columns": fk["constrained_columns"],
                "referred_table": fk["referred_table"],
                "referred_columns": fk["referred_columns"],
            }
            for fk in inspector.get_foreign_keys(table_name)
        ]

        indexes = [
            {
                "name": idx["name"],
                "columns": idx["column_names"],
                "unique": idx["unique"],
            }
            for idx in inspector.get_indexes(table_name)
        ]

        unique_constraints = [
            {
                "name": uc["name"],
                "columns": uc["column_names"],
            }
            for uc in inspector.get_unique_constraints(table_name)
        ]

        schema[table_name] = {
            "columns": columns,
            "primary_key": pk_constraint.get("constrained_columns", []),
            "foreign_keys": foreign_keys,
            "indexes": indexes,
            "unique_constraints": unique_constraints,
        }

    return schema


def schema_fingerprint(schema: dict[str, Any]) -> str:
    """Return a short SHA-256 hex digest of the serialised schema."""
    blob = json.dumps(schema, sort_keys=True).encode()
    return hashlib.sha256(blob).hexdigest()[:12]


def load_snapshot(snapshot_path: Path) -> dict[str, Any]:
    """Load a previously saved snapshot from disk.

    Args:
        snapshot_path: Path to a JSON snapshot file produced by
            :func:`save_snapshot`.

    Returns:
        The snapshot dict, including the ``schema`` and any metadata
        that was stored alongside it.

    Raises:
        FileNotFoundError: If *snapshot_path* does not exist.
        ValueError: If the file exists but cannot be parsed as JSON.
    """
    if not snapshot_path.exists():
        raise FileNotFoundError(f"Snapshot file not found: {snapshot_path}")
    try:
        return json.loads(snapshot_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Could not parse snapshot file {snapshot_path}: {exc}") from exc


def save_snapshot(
    schema: dict[str, Any],
    label: str | None = None,
    snapshot_dir: Path = DEFAULT_SNAPSHOT_DIR,
) -> Path:
    """Persist a schema snapshot to disk as a JSON file.

    The filename encodes the UTC timestamp and an optional human-readable
    label so snapshots sort chronologically and are easy to identify.

    Args:
        schema:       The schema dict produced by

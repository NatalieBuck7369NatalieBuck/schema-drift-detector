"""Diff engine for comparing database schema snapshots.

This module provides utilities for computing structural differences between
two schema snapshots, producing annotated changelogs suitable for migration
generation or audit reporting.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class ChangeType(str, Enum):
    """Enumeration of possible schema change types."""

    TABLE_ADDED = "table_added"
    TABLE_REMOVED = "table_removed"
    COLUMN_ADDED = "column_added"
    COLUMN_REMOVED = "column_removed"
    COLUMN_MODIFIED = "column_modified"
    INDEX_ADDED = "index_added"
    INDEX_REMOVED = "index_removed"
    INDEX_MODIFIED = "index_modified"


@dataclass
class SchemaChange:
    """Represents a single detected schema change between two snapshots."""

    change_type: ChangeType
    table: str
    object_name: str | None = None
    before: Any = None
    after: Any = None
    description: str = ""

    def as_dict(self) -> dict[str, Any]:
        """Serialize the change to a plain dictionary."""
        return {
            "change_type": self.change_type.value,
            "table": self.table,
            "object_name": self.object_name,
            "before": self.before,
            "after": self.after,
            "description": self.description,
        }


@dataclass
class SchemaDiff:
    """Aggregated diff result between a baseline and current schema snapshot."""

    baseline_fingerprint: str
    current_fingerprint: str
    changes: list[SchemaChange] = field(default_factory=list)

    @property
    def has_changes(self) -> bool:
        """Return True when at least one change was detected."""
        return len(self.changes) > 0

    def summary(self) -> dict[str, int]:
        """Return a count of changes grouped by change type."""
        counts: dict[str, int] = {}
        for change in self.changes:
            counts[change.change_type.value] = counts.get(change.change_type.value, 0) + 1
        return counts

    def as_dict(self) -> dict[str, Any]:
        """Serialize the full diff to a plain dictionary."""
        return {
            "baseline_fingerprint": self.baseline_fingerprint,
            "current_fingerprint": self.current_fingerprint,
            "summary": self.summary(),
            "changes": [c.as_dict() for c in self.changes],
        }


def _diff_columns(
    table: str,
    baseline_cols: dict[str, Any],
    current_cols: dict[str, Any],
    changes: list[SchemaChange],
) -> None:
    """Detect column-level additions, removals, and modifications for a table."""
    for col_name, col_def in current_cols.items():
        if col_name not in baseline_cols:
            changes.append(
                SchemaChange(
                    change_type=ChangeType.COLUMN_ADDED,
                    table=table,
                    object_name=col_name,
                    after=col_def,
                    description=f"Column '{col_name}' added to table '{table}'.",
                )
            )
        elif baseline_cols[col_name] != col_def:
            changes.append(
                SchemaChange(
                    change_type=ChangeType.COLUMN_MODIFIED,
                    table=table,
                    object_name=col_name,
                    before=baseline_cols[col_name],
                    after=col_def,
                    description=f"Column '{col_name}' in table '{table}' was modified.",
                )
            )

    for col_name, col_def in baseline_cols.items():
        if col_name not in current_cols:
            changes.append(
                SchemaChange(
                    change_type=ChangeType.COLUMN_REMOVED,
                    table=table,
                    object_name=col_name,
                    before=col_def,
                    description=f"Column '{col_name}' removed from table '{table}'.",
                )
            )


def _diff_indexes(
    table: str,
    baseline_indexes: dict[str, Any],
    current_indexes: dict[str, Any],
    changes: list[SchemaChange],
) -> None:
    """Detect index-level additions, removals, and modifications for a table."""
    for idx_name, idx_def in current_indexes.items():
        if idx_name not in baseline_indexes:
            changes.append(
                SchemaChange(
                    change_type=ChangeType.INDEX_ADDED,
                    table=table,
                    object_name=idx_name,
                    after=idx_def,
                    description=f"Index '{idx_name}' added to table '{table}'.",
                )
            )
        elif baseline_indexes[idx_name] != idx_def:
            changes.append(
                SchemaChange(
                    change_type=ChangeType.INDEX_MODIFIED,
                    table=table,
                    object_name=idx_name,
                    before=baseline_indexes[idx_name],
                    after=idx_def,
                    description=f"Index '{idx_name}' on table '{table}' was modified.",
                )
            )

    for idx_name, idx_def in baseline_indexes.items():
        if idx_name not in current_indexes:
            changes.append(
                SchemaChange(
                    change_type=ChangeType.INDEX_REMOVED,
                    table=table,
                    object_name=idx_name,
                    before=idx_def,
                    description=f"Index '{idx_name}' removed from table '{table}'.",
                )
            )


def compute_diff(baseline: dict[str, Any], current: dict[str, Any]) -> SchemaDiff:
    """Compute a full schema diff between a baseline snapshot and a current snapshot.

    Both *baseline* and *current* are expected to be dictionaries in the format
    produced by ``snapshot.get_schema``, keyed by table name with sub-keys
    ``columns`` and ``indexes``.

    Args:
        baseline: The previously saved schema snapshot data.
        current: The freshly captured schema snapshot data.

    Returns:
        A :class:`SchemaDiff` instance containing all detected changes.
    """
    from schema_drift_detector.snapshot import schema_fingerprint  # avoid circular at module level

    changes: list[SchemaChange] = []

    baseline_tables = baseline.get("tables", {})
    current_tables = current.get("tables", {})

    # Detect added tables
    for table_name, table_def in current_tables.items():
        if table_name not in baseline_tables:
            changes.append(
                SchemaChange(
                    change_type=ChangeType.TABLE_ADDED,
                    table=table_name,
                    after=table_def,
                    description=f"Table '{table_name}' was created.",
                )
            )
            continue

        # Diff columns and indexes for existing tables
        _diff_columns(
            table_name,
            baseline_tables[table_name].get("columns", {}),
            table_def.get("columns", {}),
            changes,
        )
        _diff_indexes(
            table_name,
            baseline_tables[table_name].get("indexes", {}),
            table_def.get("indexes", {}),
            changes,
        )

    # Detect removed tables
    for table_name, table_def in baseline_tables.items():
        if table_name not in current_tables:
            changes.append(
                SchemaChange(
                    change_type=ChangeType.TABLE_REMOVED,
                    table=table_name,
                    before=table_def,
                    description=f"Table '{table_name}' was dropped.",
                )
            )

    return SchemaDiff(
        baseline_fingerprint=schema_fingerprint(baseline),
        current_fingerprint=schema_fingerprint(current),
        changes=changes,
    )

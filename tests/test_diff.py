"""Tests for the schema diff engine."""

import pytest
from schema_drift_detector.diff import (
    ChangeType,
    SchemaChange,
    SchemaDiff,
    has_changes,
    as_dict,
)


# ---------------------------------------------------------------------------
# Fixtures – minimal schema snapshots
# ---------------------------------------------------------------------------

BASE_SCHEMA = {
    "users": {
        "columns": {
            "id": {"type": "integer", "nullable": False, "default": None},
            "email": {"type": "varchar(255)", "nullable": False, "default": None},
            "created_at": {"type": "timestamp", "nullable": True, "default": "now()"},
        },
        "indexes": {"users_email_idx": {"columns": ["email"], "unique": True}},
        "constraints": {"users_pkey": {"type": "PRIMARY KEY", "columns": ["id"]}},
    },
    "posts": {
        "columns": {
            "id": {"type": "integer", "nullable": False, "default": None},
            "title": {"type": "text", "nullable": False, "default": None},
            "user_id": {"type": "integer", "nullable": False, "default": None},
        },
        "indexes": {},
        "constraints": {"posts_pkey": {"type": "PRIMARY KEY", "columns": ["id"]}},
    },
}

SCHEMA_TABLE_ADDED = {
    **BASE_SCHEMA,
    "comments": {
        "columns": {
            "id": {"type": "integer", "nullable": False, "default": None},
            "body": {"type": "text", "nullable": True, "default": None},
        },
        "indexes": {},
        "constraints": {},
    },
}

SCHEMA_TABLE_DROPPED = {k: v for k, v in BASE_SCHEMA.items() if k != "posts"}

SCHEMA_COLUMN_ADDED = {
    **BASE_SCHEMA,
    "users": {
        **BASE_SCHEMA["users"],
        "columns": {
            **BASE_SCHEMA["users"]["columns"],
            "bio": {"type": "text", "nullable": True, "default": None},
        },
    },
}

SCHEMA_COLUMN_DROPPED = {
    **BASE_SCHEMA,
    "users": {
        **BASE_SCHEMA["users"],
        "columns": {
            k: v
            for k, v in BASE_SCHEMA["users"]["columns"].items()
            if k != "created_at"
        },
    },
}

SCHEMA_COLUMN_ALTERED = {
    **BASE_SCHEMA,
    "users": {
        **BASE_SCHEMA["users"],
        "columns": {
            **BASE_SCHEMA["users"]["columns"],
            "email": {"type": "text", "nullable": True, "default": None},
        },
    },
}


# ---------------------------------------------------------------------------
# SchemaDiff construction
# ---------------------------------------------------------------------------

class TestSchemaDiff:
    def test_no_changes_when_identical(self):
        diff = SchemaDiff(BASE_SCHEMA, BASE_SCHEMA)
        assert not has_changes(diff)
        assert diff.changes == []

    def test_detects_added_table(self):
        diff = SchemaDiff(BASE_SCHEMA, SCHEMA_TABLE_ADDED)
        changes = diff.changes
        types = [c.change_type for c in changes]
        assert ChangeType.TABLE_ADDED in types
        added = [c for c in changes if c.change_type == ChangeType.TABLE_ADDED]
        assert any(c.table == "comments" for c in added)

    def test_detects_dropped_table(self):
        diff = SchemaDiff(BASE_SCHEMA, SCHEMA_TABLE_DROPPED)
        types = [c.change_type for c in diff.changes]
        assert ChangeType.TABLE_DROPPED in types
        dropped = [
            c for c in diff.changes if c.change_type == ChangeType.TABLE_DROPPED
        ]
        assert any(c.table == "posts" for c in dropped)

    def test_detects_added_column(self):
        diff = SchemaDiff(BASE_SCHEMA, SCHEMA_COLUMN_ADDED)
        types = [c.change_type for c in diff.changes]
        assert ChangeType.COLUMN_ADDED in types
        added = [c for c in diff.changes if c.change_type == ChangeType.COLUMN_ADDED]
        assert any(c.column == "bio" and c.table == "users" for c in added)

    def test_detects_dropped_column(self):
        diff = SchemaDiff(BASE_SCHEMA, SCHEMA_COLUMN_DROPPED)
        types = [c.change_type for c in diff.changes]
        assert ChangeType.COLUMN_DROPPED in types

    def test_detects_altered_column(self):
        diff = SchemaDiff(BASE_SCHEMA, SCHEMA_COLUMN_ALTERED)
        types = [c.change_type for c in diff.changes]
        assert ChangeType.COLUMN_ALTERED in types
        altered = [
            c for c in diff.changes if c.change_type == ChangeType.COLUMN_ALTERED
        ]
        assert any(c.column == "email" for c in altered)


# ---------------------------------------------------------------------------
# SchemaChange serialisation
# ---------------------------------------------------------------------------

class TestSchemaChange:
    def test_as_dict_contains_required_keys(self):
        change = SchemaChange(
            change_type=ChangeType.COLUMN_ADDED,
            table="users",
            column="bio",
            detail={"type": "text"},
        )
        d = as_dict(change)
        assert "change_type" in d
        assert "table" in d
        assert d["column"] == "bio"

    def test_as_dict_change_type_is_string(self):
        change = SchemaChange(
            change_type=ChangeType.TABLE_ADDED,
            table="comments",
        )
        d = as_dict(change)
        assert isinstance(d["change_type"], str)

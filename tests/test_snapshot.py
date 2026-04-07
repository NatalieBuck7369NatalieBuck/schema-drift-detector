"""Tests for schema snapshot capture and persistence."""

import json
import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from schema_drift_detector.snapshot import (
    get_schema,
    load_snapshot,
    save_snapshot,
    schema_fingerprint,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SAMPLE_SCHEMA = {
    "users": {
        "columns": {
            "id": {"type": "INTEGER", "nullable": False, "default": None},
            "email": {"type": "VARCHAR(255)", "nullable": False, "default": None},
            "created_at": {"type": "TIMESTAMP", "nullable": True, "default": "now()"},
        },
        "indexes": ["ix_users_email"],
        "primary_key": ["id"],
    },
    "orders": {
        "columns": {
            "id": {"type": "INTEGER", "nullable": False, "default": None},
            "user_id": {"type": "INTEGER", "nullable": False, "default": None},
            "total": {"type": "NUMERIC(10,2)", "nullable": True, "default": None},
        },
        "indexes": [],
        "primary_key": ["id"],
    },
}


# ---------------------------------------------------------------------------
# schema_fingerprint
# ---------------------------------------------------------------------------


class TestSchemaFingerprint:
    def test_same_schema_produces_same_fingerprint(self):
        fp1 = schema_fingerprint(SAMPLE_SCHEMA)
        fp2 = schema_fingerprint(SAMPLE_SCHEMA)
        assert fp1 == fp2

    def test_different_schemas_produce_different_fingerprints(self):
        modified = json.loads(json.dumps(SAMPLE_SCHEMA))  # deep copy
        modified["users"]["columns"]["phone"] = {
            "type": "VARCHAR(20)",
            "nullable": True,
            "default": None,
        }
        assert schema_fingerprint(SAMPLE_SCHEMA) != schema_fingerprint(modified)

    def test_fingerprint_is_hex_string(self):
        fp = schema_fingerprint(SAMPLE_SCHEMA)
        # Should be a valid hex string (MD5 or SHA-based)
        assert isinstance(fp, str)
        assert all(c in "0123456789abcdef" for c in fp)

    def test_key_order_does_not_affect_fingerprint(self):
        """Fingerprint must be stable regardless of dict insertion order."""
        reordered = {
            "orders": SAMPLE_SCHEMA["orders"],
            "users": SAMPLE_SCHEMA["users"],
        }
        assert schema_fingerprint(SAMPLE_SCHEMA) == schema_fingerprint(reordered)


# ---------------------------------------------------------------------------
# save_snapshot / load_snapshot
# ---------------------------------------------------------------------------


class TestSnapshotPersistence:
    def test_save_and_load_roundtrip(self, tmp_path):
        snapshot_file = tmp_path / "snap.json"
        save_snapshot(SAMPLE_SCHEMA, str(snapshot_file))
        loaded = load_snapshot(str(snapshot_file))
        assert loaded == SAMPLE_SCHEMA

    def test_saved_file_is_valid_json(self, tmp_path):
        snapshot_file = tmp_path / "snap.json"
        save_snapshot(SAMPLE_SCHEMA, str(snapshot_file))
        with open(snapshot_file) as fh:
            parsed = json.load(fh)
        assert isinstance(parsed, dict)

    def test_load_missing_file_raises(self, tmp_path):
        missing = tmp_path / "nonexistent.json"
        with pytest.raises(FileNotFoundError):
            load_snapshot(str(missing))

    def test_load_invalid_json_raises(self, tmp_path):
        bad_file = tmp_path / "bad.json"
        bad_file.write_text("not json {{{")
        with pytest.raises(json.JSONDecodeError):
            load_snapshot(str(bad_file))

    def test_save_creates_parent_directories(self, tmp_path):
        nested = tmp_path / "a" / "b" / "snap.json"
        save_snapshot(SAMPLE_SCHEMA, str(nested))
        assert nested.exists()


# ---------------------------------------------------------------------------
# get_schema  (requires a live DB connection — mocked here)
# ---------------------------------------------------------------------------


class TestGetSchema:
    @patch("schema_drift_detector.snapshot.create_engine")
    def test_returns_dict_keyed_by_table_name(self, mock_create_engine):
        """get_schema should return a dict of table definitions."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        # Simulate inspector returning two tables
        mock_inspector = MagicMock()
        mock_inspector.get_table_names.return_value = ["users", "orders"]
        mock_inspector.get_columns.side_effect = [
            [{"name": "id", "type": MagicMock(__str__=lambda s: "INTEGER"), "nullable": False, "default": None}],
            [{"name": "id", "type": MagicMock(__str__=lambda s: "INTEGER"), "nullable": False, "default": None}],
        ]
        mock_inspector.get_indexes.return_value = []
        mock_inspector.get_pk_constraint.return_value = {"constrained_columns": ["id"]}

        with patch("schema_drift_detector.snapshot.inspect", return_value=mock_inspector):
            schema = get_schema("sqlite:///fake.db")

        assert "users" in schema
        assert "orders" in schema

    @patch("schema_drift_detector.snapshot.create_engine")
    def test_empty_database_returns_empty_dict(self, mock_create_engine):
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        mock_inspector = MagicMock()
        mock_inspector.get_table_names.return_value = []

        with patch("schema_drift_detector.snapshot.inspect", return_value=mock_inspector):
            schema = get_schema("sqlite:///empty.db")

        assert schema == {}

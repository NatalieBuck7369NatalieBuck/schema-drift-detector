"""Reporter module for schema-drift-detector.

Provides human-readable and machine-readable output formats
for schema diffs and changelogs.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import IO, Optional

from .diff import ChangeType, SchemaDiff, SchemaChange


# ANSI colour codes for terminal output
_COLOURS = {
    "red": "\033[31m",
    "green": "\033[32m",
    "yellow": "\033[33m",
    "cyan": "\033[36m",
    "bold": "\033[1m",
    "reset": "\033[0m",
}

# Map each ChangeType to a display symbol and colour
_CHANGE_STYLE: dict[ChangeType, tuple[str, str]] = {
    ChangeType.ADDED: ("+", "green"),
    ChangeType.REMOVED: ("-", "red"),
    ChangeType.MODIFIED: ("~", "yellow"),
    ChangeType.RENAMED: (">", "cyan"),
}


def _colour(text: str, colour: str, use_colour: bool = True) -> str:
    """Wrap *text* in the given ANSI colour escape, if enabled."""
    if not use_colour:
        return text
    code = _COLOURS.get(colour, "")
    reset = _COLOURS["reset"]
    return f"{code}{text}{reset}"


def _format_change(change: SchemaChange, use_colour: bool = True) -> str:
    """Return a single-line summary string for one SchemaChange."""
    symbol, colour = _CHANGE_STYLE.get(change.change_type, ("?", "reset"))
    prefix = _colour(f"  [{symbol}]", colour, use_colour)
    label = f"{change.object_type.upper()} {change.object_name}"
    detail = f": {change.detail}" if change.detail else ""
    return f"{prefix} {label}{detail}"


def render_text(
    diff: SchemaDiff,
    stream: IO[str],
    use_colour: bool = True,
    show_fingerprints: bool = False,
) -> None:
    """Write a human-readable diff report to *stream*.

    Args:
        diff: The SchemaDiff produced by the diff module.
        stream: Any writable text stream (e.g. sys.stdout or an open file).
        use_colour: Whether to emit ANSI colour codes.
        show_fingerprints: Whether to include schema fingerprint hashes.
    """
    bold = lambda t: _colour(t, "bold", use_colour)  # noqa: E731

    stream.write(bold("Schema Drift Report") + "\n")
    stream.write(f"Generated : {datetime.utcnow().isoformat(timespec='seconds')}Z\n")
    stream.write(f"Baseline  : {diff.baseline_snapshot}\n")
    stream.write(f"Current   : {diff.current_snapshot}\n")
    if show_fingerprints:
        stream.write(f"Fingerprint (baseline): {diff.baseline_fingerprint}\n")
        stream.write(f"Fingerprint (current) : {diff.current_fingerprint}\n")
    stream.write("\n")

    if not diff.has_changes():
        stream.write(_colour("No schema changes detected.\n", "green", use_colour))
        return

    # Group changes by object type for readability
    by_type: dict[str, list[SchemaChange]] = {}
    for change in diff.changes:
        by_type.setdefault(change.object_type, []).append(change)

    for obj_type, changes in sorted(by_type.items()):
        stream.write(bold(f"{obj_type.capitalize()}s ({len(changes)} change(s)):") + "\n")
        for change in changes:
            stream.write(_format_change(change, use_colour) + "\n")
        stream.write("\n")

    totals = diff.summary()
    stream.write(
        bold("Summary: ")
        + f"{totals.get('added', 0)} added, "
        + f"{totals.get('removed', 0)} removed, "
        + f"{totals.get('modified', 0)} modified, "
        + f"{totals.get('renamed', 0)} renamed\n"
    )


def render_json(
    diff: SchemaDiff,
    stream: IO[str],
    indent: int = 2,
) -> None:
    """Serialise the diff to JSON and write it to *stream*.

    The output is suitable for piping into other tools or storing as an
    audit record alongside the snapshot files.

    Args:
        diff: The SchemaDiff to serialise.
        stream: Any writable text stream.
        indent: JSON indentation level.
    """
    payload = {
        "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "baseline_snapshot": diff.baseline_snapshot,
        "current_snapshot": diff.current_snapshot,
        "baseline_fingerprint": diff.baseline_fingerprint,
        "current_fingerprint": diff.current_fingerprint,
        "has_changes": diff.has_changes(),
        "summary": diff.summary(),
        "changes": [c.as_dict() for c in diff.changes],
    }
    json.dump(payload, stream, indent=indent)
    stream.write("\n")


def render(
    diff: SchemaDiff,
    stream: IO[str],
    fmt: str = "text",
    use_colour: bool = True,
    show_fingerprints: bool = False,
) -> None:
    """Dispatch to the appropriate renderer based on *fmt*.

    Args:
        diff: The SchemaDiff to render.
        stream: Output stream.
        fmt: Output format — ``"text"`` or ``"json"``.
        use_colour: Passed to the text renderer.
        show_fingerprints: Passed to the text renderer.

    Raises:
        ValueError: If *fmt* is not a recognised format.
    """
    if fmt == "text":
        render_text(diff, stream, use_colour=use_colour, show_fingerprints=show_fingerprints)
    elif fmt == "json":
        render_json(diff, stream)
    else:
        raise ValueError(f"Unknown output format: {fmt!r}. Choose 'text' or 'json'.")

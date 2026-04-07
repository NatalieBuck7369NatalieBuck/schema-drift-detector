"""CLI entry point for schema-drift-detector.

Provides commands to snapshot database schemas, compare snapshots,
and generate annotated migration changelogs.
"""

import sys
import click
from pathlib import Path

from schema_drift_detector.snapshot import capture_snapshot, load_snapshot
from schema_drift_detector.differ import compute_diff
from schema_drift_detector.reporter import generate_changelog


@click.group()
@click.version_option(version="0.1.0", prog_name="schema-drift-detector")
def cli():
    """schema-drift-detector: Monitor database schema changes over time.

    Capture schema snapshots, detect drift between versions, and generate
    annotated migration changelogs.
    """
    pass


@cli.command("snapshot")
@click.option(
    "--url",
    required=True,
    envvar="DATABASE_URL",
    help="Database connection URL (e.g. postgresql://user:pass@host/db).",
)
@click.option(
    "--output",
    "-o",
    default="./snapshots",
    show_default=True,
    help="Directory to store schema snapshots.",
)
@click.option(
    "--label",
    "-l",
    default=None,
    help="Optional human-readable label for this snapshot.",
)
def snapshot_cmd(url: str, output: str, label: str | None):
    """Capture the current database schema as a snapshot."""
    output_dir = Path(output)
    output_dir.mkdir(parents=True, exist_ok=True)

    click.echo(f"Connecting to database and capturing schema snapshot...")
    try:
        snapshot_path = capture_snapshot(url=url, output_dir=output_dir, label=label)
        click.secho(f"Snapshot saved: {snapshot_path}", fg="green")
    except Exception as exc:
        click.secho(f"Error capturing snapshot: {exc}", fg="red", err=True)
        sys.exit(1)


@cli.command("diff")
@click.argument("snapshot_a", type=click.Path(exists=True, path_type=Path))
@click.argument("snapshot_b", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["text", "json", "markdown"], case_sensitive=False),
    default="text",
    show_default=True,
    help="Output format for the diff report.",
)
@click.option(
    "--output",
    "-o",
    default=None,
    help="Write report to file instead of stdout.",
)
def diff_cmd(snapshot_a: Path, snapshot_b: Path, output_format: str, output: str | None):
    """Compare two schema snapshots and show the diff.

    SNAPSHOT_A is the baseline (older) snapshot file.
    SNAPSHOT_B is the target (newer) snapshot file.
    """
    click.echo(f"Loading snapshots...")
    try:
        schema_a = load_snapshot(snapshot_a)
        schema_b = load_snapshot(snapshot_b)
    except Exception as exc:
        click.secho(f"Error loading snapshots: {exc}", fg="red", err=True)
        sys.exit(1)

    click.echo("Computing schema diff...")
    diff = compute_diff(schema_a, schema_b)

    if not diff.has_changes():
        click.secho("No schema changes detected.", fg="yellow")
        return

    report = generate_changelog(diff, fmt=output_format)

    if output:
        Path(output).write_text(report, encoding="utf-8")
        click.secho(f"Report written to {output}", fg="green")
    else:
        click.echo(report)


@cli.command("history")
@click.option(
    "--snapshots-dir",
    default="./snapshots",
    show_default=True,
    help="Directory containing schema snapshots.",
)
def history_cmd(snapshots_dir: str):
    """List all captured snapshots with timestamps and labels."""
    snapshots_path = Path(snapshots_dir)
    if not snapshots_path.exists():
        click.secho(f"Snapshots directory not found: {snapshots_dir}", fg="red", err=True)
        sys.exit(1)

    snapshot_files = sorted(snapshots_path.glob("*.json"))
    if not snapshot_files:
        click.echo("No snapshots found.")
        return

    click.echo(f"{'#':<4} {'Filename':<40} {'Captured At':<25} {'Label'}")
    click.echo("-" * 80)
    for idx, path in enumerate(snapshot_files, start=1):
        try:
            snap = load_snapshot(path)
            captured_at = snap.get("captured_at", "unknown")
            label = snap.get("label") or "-"
        except Exception:
            captured_at, label = "(unreadable)", "-"
        click.echo(f"{idx:<4} {path.name:<40} {captured_at:<25} {label}")


def main():
    """Package entry point."""
    cli()


if __name__ == "__main__":
    main()

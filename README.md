# schema-drift-detector

> CLI tool that monitors database schema changes over time and generates migration diffs with annotated changelogs.

---

## Installation

```bash
pip install schema-drift-detector
```

Or install from source:

```bash
git clone https://github.com/yourname/schema-drift-detector.git
cd schema-drift-detector
pip install -e .
```

---

## Usage

Take a snapshot of your current database schema:

```bash
sdd snapshot --db postgresql://user:pass@localhost/mydb --label baseline
```

After making schema changes, generate a diff against the previous snapshot:

```bash
sdd diff --db postgresql://user:pass@localhost/mydb --against baseline
```

Export an annotated changelog to a file:

```bash
sdd diff --db postgresql://user:pass@localhost/mydb --against baseline --output changelog.md
```

**Example output:**

```
[ADDED]   table: user_sessions
[REMOVED] column: users.legacy_token
[ALTERED] column: orders.status → type changed from VARCHAR(50) to VARCHAR(100)
```

---

## Supported Databases

- PostgreSQL
- MySQL / MariaDB
- SQLite

---

## License

MIT © 2024 [yourname](https://github.com/yourname)
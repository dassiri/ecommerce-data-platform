"""Single, reproducible initialization mechanism for the Raw/Landing database.

Step 3.3.2 scope: this script creates schema/table STRUCTURE ONLY. It does
not load any CSV data, compute checksums, or run any ingestion logic.

Usage:
    python -m database.init_db

Safe to rerun: raw_schema.sql uses `CREATE SCHEMA IF NOT EXISTS` and
`CREATE TABLE IF NOT EXISTS` throughout, so running this script against an
already-initialized database is a no-op (idempotent).

This is intentionally the ONE initialization entry point for the raw
schema — do not create a second/competing mechanism (e.g. a duplicate SQL
runner, an ORM-driven `create_all`, etc.) alongside this one.
"""

from __future__ import annotations

import os
import sys

from database.connection import (
    DatabaseConfigError,
    DatabaseConnectionError,
    transaction,
)

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
SCHEMA_SQL_PATH = os.path.join(_THIS_DIR, "schema", "raw_schema.sql")


def load_schema_sql(path: str = SCHEMA_SQL_PATH) -> str:
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Raw schema SQL file not found: {path}")
    with open(path, "r", encoding="utf-8") as fh:
        return fh.read()


def init_raw_schema() -> None:
    """Apply raw_schema.sql inside a single transaction.

    Commits only if every statement in the script succeeds; rolls back
    entirely on any error, so the database is never left half-initialized.
    """
    sql_script = load_schema_sql()
    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute(sql_script)


def main() -> int:
    try:
        init_raw_schema()
    except (DatabaseConfigError, DatabaseConnectionError) as exc:
        print(f"[init_db] FAILED (configuration/connection): {exc}", file=sys.stderr)
        return 1
    except FileNotFoundError as exc:
        print(f"[init_db] FAILED (schema file missing): {exc}", file=sys.stderr)
        return 1
    except Exception as exc:  # noqa: BLE001 - top-level CLI error boundary
        print(f"[init_db] FAILED (unexpected error): {exc}", file=sys.stderr)
        return 1

    print("[init_db] raw schema initialized successfully (schema + 7 tables).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Read/write helpers for `raw.ingestion_audit` (Step 3.3.3).

Scope: this module only knows how to insert an audit row and look up the
checksum of the latest *successful* run for a dataset. It does not decide
*when* those things should happen — that orchestration lives in
`ingestion/raw_loader.py`.

Table structure (`raw.ingestion_audit`) is owned by
`database/schema/raw_schema.sql` (Step 3.3.2) — this module does not
redefine it, only reads/writes to it.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass
class AuditRecord:
    """One row of `raw.ingestion_audit`."""

    ingestion_run_id: str
    batch_id: str
    dataset_name: str
    source_file: str
    file_checksum: str | None
    started_at: datetime
    completed_at: datetime | None
    duration_seconds: float | None
    rows_read: int | None
    rows_loaded: int | None
    validation_status: str | None
    load_status: str | None
    status: str
    error_message: str | None


_INSERT_SQL = """
    INSERT INTO raw.ingestion_audit (
        ingestion_run_id, batch_id, dataset_name, source_file, file_checksum,
        started_at, completed_at, duration_seconds,
        rows_read, rows_loaded,
        validation_status, load_status, status, error_message
    ) VALUES (
        %(ingestion_run_id)s, %(batch_id)s, %(dataset_name)s, %(source_file)s,
        %(file_checksum)s,
        %(started_at)s, %(completed_at)s, %(duration_seconds)s,
        %(rows_read)s, %(rows_loaded)s,
        %(validation_status)s, %(load_status)s, %(status)s, %(error_message)s
    )
"""

_LATEST_SUCCESSFUL_CHECKSUM_SQL = """
    SELECT file_checksum
    FROM raw.ingestion_audit
    WHERE dataset_name = %(dataset_name)s
      AND status = 'SUCCESS'
      AND file_checksum IS NOT NULL
    ORDER BY completed_at DESC NULLS LAST, audit_id DESC
    LIMIT 1
"""


def insert_audit_record(cur, record: AuditRecord) -> None:
    """Insert one audit row using an already-open cursor/transaction."""
    cur.execute(
        _INSERT_SQL,
        {
            "ingestion_run_id": record.ingestion_run_id,
            "batch_id": record.batch_id,
            "dataset_name": record.dataset_name,
            "source_file": record.source_file,
            "file_checksum": record.file_checksum,
            "started_at": record.started_at,
            "completed_at": record.completed_at,
            "duration_seconds": record.duration_seconds,
            "rows_read": record.rows_read,
            "rows_loaded": record.rows_loaded,
            "validation_status": record.validation_status,
            "load_status": record.load_status,
            "status": record.status,
            "error_message": record.error_message,
        },
    )


def get_latest_successful_checksum(cur, dataset_name: str) -> str | None:
    """Return the file_checksum of the most recent SUCCESS run for `dataset_name`.

    Returns None if there is no prior successful run (first-ever load).
    This is the comparison basis for SKIPPED_UNCHANGED (DEC-003) — it is
    deliberately scoped to SUCCESS runs only, so a previously FAILED or
    SKIPPED_UNCHANGED run never masks the need to (re)load.
    """
    cur.execute(_LATEST_SUCCESSFUL_CHECKSUM_SQL, {"dataset_name": dataset_name})
    row = cur.fetchone()
    return row[0] if row else None

"""Step 3.3.3 — Python Raw ingestion pipeline.

A single, reusable, configuration-driven framework that ingests all six
source CSVs into their `raw.*` PostgreSQL tables — not six duplicated
per-dataset loaders. Every dataset flows through the same function
(`load_dataset`); what differs between datasets is only data (the
`RAW_TABLE_NAMES` config and each dataset's YAML contract), not code.

Flow (per the approved Step 3.3.1 design):
    CSV -> File Discovery -> Contract Validation -> SHA-256 Checksum
        -> Raw PostgreSQL Load -> Post-Load Validation -> Audit

Approved decisions this module implements:
    - Daily Full Batch, Truncate + Reload (DEC-001).
    - No PK/FK constraints on the six Raw tables (DEC-002) — enforced by
      `database/schema/raw_schema.sql`; this module never adds its own.
    - SHA-256 checksum-based idempotency, NOT CDC (DEC-003).
    - Four Raw metadata columns (DEC-004): ingestion_timestamp, source_file,
      batch_id, ingestion_run_id.
    - YAML contracts remain the source of truth (reuses
      `ingestion.validation.contract_loader` / `schema_validator` as-is).
    - Raw is a current snapshot; no historical row retention in Raw.
    - Max 3 retries, exponential backoff, transient failures only (DEC-007).
    - Each dataset uses its own transaction; one failed dataset must not
      roll back another dataset's successful load.

Explicitly NOT implemented here (future steps): CDC, incremental loading,
deduplication/cleaning/transformation of source business columns,
orchestration (Airflow/dbt), production/cloud deployment.
"""

from __future__ import annotations

import csv
import logging
import os
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, TypeVar

from database.connection import (
    DatabaseConfigError,
    DatabaseConnectionError,
    MissingDependencyError,
    transaction,
)
from ingestion.audit import AuditRecord, get_latest_successful_checksum, insert_audit_record
from ingestion.checksum import compute_file_checksum
from ingestion.validation.contract_loader import DataContract, load_all_contracts
from ingestion.validation.schema_validator import validate_file

logger = logging.getLogger("ingestion.raw_loader")

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_THIS_DIR)

DEFAULT_DATA_DIR = os.path.join(_PROJECT_ROOT, "data", "raw_source")
DEFAULT_CONTRACTS_DIR = os.path.join(_THIS_DIR, "contracts")

# -----------------------------------------------------------------------
# Configuration: dataset_name -> raw table name. This is the ONE place
# that maps datasets to tables; adding/removing a dataset means editing
# this dict and dropping a contract file in ingestion/contracts/ — not
# writing a new loader function. (Requirement A.)
# -----------------------------------------------------------------------
RAW_TABLE_NAMES: dict[str, str] = {
    "users": "users",
    "products": "products",
    "orders": "orders",
    "order_items": "order_items",
    "reviews": "reviews",
    "events": "events",
}
RAW_SCHEMA_NAME = "raw"

STATUS_SUCCESS = "SUCCESS"
STATUS_SKIPPED_UNCHANGED = "SKIPPED_UNCHANGED"
STATUS_FAILED = "FAILED"

MAX_RETRY_ATTEMPTS = 3
RETRY_BACKOFF_BASE_SECONDS = 1.0

_T = TypeVar("_T")


class DeterministicIngestionError(RuntimeError):
    """A non-retryable failure (validation, contract, row-count mismatch)."""


# ---------------------------------------------------------------------------
# 1. Source discovery / expected-file validation
# ---------------------------------------------------------------------------


@dataclass
class DiscoveryResult:
    found: dict[str, str]  # dataset_name -> absolute file path
    missing: list[str]  # expected filenames not present
    unknown: list[str]  # *.csv files present but not declared by any contract


def discover_source_files(
    data_dir: str, contracts: dict[str, DataContract]
) -> DiscoveryResult:
    """Match files in `data_dir` against each contract's declared source_file.

    Never lists the six real CSVs' *contents* — only checks presence/absence
    by name, so this is safe to run against `data/raw_source/` without ever
    touching or altering the immutable source files.
    """
    expected = {name: contract.source_file for name, contract in contracts.items()}

    found: dict[str, str] = {}
    missing: list[str] = []
    for dataset_name, filename in expected.items():
        path = os.path.join(data_dir, filename)
        if os.path.isfile(path):
            found[dataset_name] = path
        else:
            missing.append(filename)

    try:
        present_csvs = {f for f in os.listdir(data_dir) if f.lower().endswith(".csv")}
    except FileNotFoundError:
        present_csvs = set()

    unknown = sorted(present_csvs - set(expected.values()))

    return DiscoveryResult(found=found, missing=sorted(missing), unknown=unknown)


# ---------------------------------------------------------------------------
# 2. Retry / backoff (transient infra/DB failures only — DEC-007)
# ---------------------------------------------------------------------------


def _is_transient_db_error(exc: BaseException) -> bool:
    """True only for transient infrastructure/database failures.

    Deterministic failures (validation errors, row-count mismatches,
    missing dependencies, configuration errors, programming errors) must
    never be retried — only connection drops, timeouts, and similar
    operational errors.
    """
    if isinstance(
        exc,
        (
            MissingDependencyError,
            DatabaseConfigError,
            DeterministicIngestionError,
            ImportError,
        ),
    ):
        return False
    if isinstance(exc, DatabaseConnectionError):
        return True
    try:
        import psycopg2
    except ImportError:
        return False
    return isinstance(exc, psycopg2.OperationalError)


def _run_with_retry(
    func: Callable[[], _T],
    *,
    dataset_name: str,
    max_attempts: int = MAX_RETRY_ATTEMPTS,
    backoff_base: float = RETRY_BACKOFF_BASE_SECONDS,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> _T:
    """Run `func`, retrying only on transient DB errors, up to `max_attempts`.

    Exponential backoff: backoff_base * 2^(attempt-1) seconds between
    attempts. Any non-transient exception propagates immediately on the
    first failure (deterministic failures are not retried).
    """
    attempt = 1
    while True:
        try:
            return func()
        except Exception as exc:
            if not _is_transient_db_error(exc):
                raise
            if attempt >= max_attempts:
                logger.error(
                    "dataset=%s transient failure, exhausted %d attempt(s): %s",
                    dataset_name,
                    max_attempts,
                    exc,
                )
                raise
            delay = backoff_base * (2 ** (attempt - 1))
            logger.warning(
                "dataset=%s transient failure on attempt %d/%d, retrying in "
                "%.1fs: %s",
                dataset_name,
                attempt,
                max_attempts,
                delay,
                exc,
            )
            sleep_fn(delay)
            attempt += 1


# ---------------------------------------------------------------------------
# 3. Row transformation for load (source fidelity — no clean/dedupe/rename)
# ---------------------------------------------------------------------------


def _iter_rows_for_insert(
    contract: DataContract,
    file_path: str,
    *,
    ingestion_timestamp: datetime,
    source_file: str,
    batch_id: str,
    ingestion_run_id: str,
):
    """Yield one tuple per CSV row: source values (as-is) + 4 metadata values.

    Source fidelity (requirement 9): values are passed through unchanged
    except that an empty CSV field becomes SQL NULL (required for the
    column's declared PostgreSQL type — e.g. an empty string is not a valid
    DATE/NUMERIC literal). No dedup, clean, transform, aggregate, or rename
    of business columns happens here or anywhere in this module.
    """
    columns = contract.column_names
    with open(file_path, "r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            values = [(row.get(col) or None) for col in columns]
            values.extend([ingestion_timestamp, source_file, batch_id, ingestion_run_id])
            yield tuple(values)


def _bulk_insert(
    cur,
    table_name: str,
    contract: DataContract,
    file_path: str,
    *,
    ingestion_timestamp: datetime,
    source_file: str,
    batch_id: str,
    ingestion_run_id: str,
) -> None:
    from psycopg2 import sql
    from psycopg2.extras import execute_values

    all_columns = list(contract.column_names) + [
        "ingestion_timestamp",
        "source_file",
        "batch_id",
        "ingestion_run_id",
    ]
    insert_stmt = sql.SQL("INSERT INTO {table} ({cols}) VALUES %s").format(
        table=sql.Identifier(RAW_SCHEMA_NAME, table_name),
        cols=sql.SQL(", ").join(sql.Identifier(c) for c in all_columns),
    )
    rows = list(
        _iter_rows_for_insert(
            contract,
            file_path,
            ingestion_timestamp=ingestion_timestamp,
            source_file=source_file,
            batch_id=batch_id,
            ingestion_run_id=ingestion_run_id,
        )
    )
    execute_values(cur, insert_stmt, rows, page_size=1000)


# ---------------------------------------------------------------------------
# 4. Audit writing (always its own transaction — independent of load outcome)
# ---------------------------------------------------------------------------


def _write_audit_safe(record: AuditRecord) -> bool:
    """Insert an audit row in its own transaction; never raise.

    Deliberately decoupled from the load transaction: if a load fails and
    its transaction rolls back, the audit record documenting *that failure*
    must still survive — so it is written in a separate connection/
    transaction, after the load transaction has already been resolved.
    Audit writing is best-effort: if PostgreSQL itself is unreachable, the
    caller's ingestion result (already determined) is not affected — this
    only logs the audit-write failure.
    """
    try:
        with transaction() as conn:
            with conn.cursor() as cur:
                insert_audit_record(cur, record)
        return True
    except (DatabaseConfigError, DatabaseConnectionError) as exc:
        logger.error(
            "dataset=%s could not write audit record (DB unavailable): %s",
            record.dataset_name,
            exc,
        )
        return False
    except Exception as exc:  # noqa: BLE001 - audit writing must never crash the run
        logger.error(
            "dataset=%s could not write audit record: %s", record.dataset_name, exc
        )
        return False


# ---------------------------------------------------------------------------
# 5. Per-dataset result + orchestration
# ---------------------------------------------------------------------------


@dataclass
class DatasetIngestionResult:
    dataset_name: str
    status: str
    rows_read: int | None = None
    rows_loaded: int | None = None
    checksum: str | None = None
    batch_id: str | None = None
    ingestion_run_id: str | None = None
    error_message: str | None = None
    duration_seconds: float | None = None


def load_dataset(
    dataset_name: str,
    contract: DataContract,
    file_path: str,
    *,
    batch_id: str,
    ingestion_run_id: str,
) -> DatasetIngestionResult:
    """Ingest one dataset end-to-end: validate -> checksum -> load -> audit.

    Uses its own transaction (via `transaction()`), independent of every
    other dataset — an exception here never touches another dataset's
    connection/transaction (requirement 8). Always writes an audit record,
    whatever the outcome (SUCCESS, SKIPPED_UNCHANGED, or FAILED).
    """
    table_name = RAW_TABLE_NAMES[dataset_name]
    source_file = os.path.basename(file_path)
    started_at = datetime.now(timezone.utc)
    t0 = time.monotonic()

    result = DatasetIngestionResult(
        dataset_name=dataset_name,
        status=STATUS_FAILED,
        batch_id=batch_id,
        ingestion_run_id=ingestion_run_id,
    )

    # --- Contract validation (deterministic; never retried) ---------------
    validation_result = validate_file(contract, file_path)
    result.rows_read = validation_result.row_count

    if not validation_result.is_valid:
        issue_preview = "; ".join(
            f"{i.issue_type}(col={i.column}, row={i.row_number})"
            for i in validation_result.issues[:10]
        )
        result.error_message = (
            f"Contract validation failed: {len(validation_result.issues)} "
            f"issue(s). First issues: {issue_preview}"
        )
        result.duration_seconds = time.monotonic() - t0
        _write_audit_safe(
            AuditRecord(
                ingestion_run_id=ingestion_run_id,
                batch_id=batch_id,
                dataset_name=dataset_name,
                source_file=source_file,
                file_checksum=None,
                started_at=started_at,
                completed_at=datetime.now(timezone.utc),
                duration_seconds=result.duration_seconds,
                rows_read=result.rows_read,
                rows_loaded=None,
                validation_status="FAIL",
                load_status=None,
                status=STATUS_FAILED,
                error_message=result.error_message,
            )
        )
        return result

    # --- Checksum -----------------------------------------------------------
    checksum = compute_file_checksum(file_path)
    result.checksum = checksum
    ingestion_timestamp = datetime.now(timezone.utc)

    def _do_load() -> tuple[str, int | None]:
        with transaction() as conn:
            with conn.cursor() as cur:
                latest_checksum = get_latest_successful_checksum(cur, dataset_name)
                if latest_checksum is not None and latest_checksum == checksum:
                    return STATUS_SKIPPED_UNCHANGED, None

                cur.execute(
                    _truncate_sql(table_name),
                )
                _bulk_insert(
                    cur,
                    table_name,
                    contract,
                    file_path,
                    ingestion_timestamp=ingestion_timestamp,
                    source_file=source_file,
                    batch_id=batch_id,
                    ingestion_run_id=ingestion_run_id,
                )
                cur.execute(_count_sql(table_name))
                rows_loaded = cur.fetchone()[0]
                if rows_loaded != validation_result.row_count:
                    raise DeterministicIngestionError(
                        f"Row-count reconciliation failed for '{dataset_name}': "
                        f"source had {validation_result.row_count} row(s), "
                        f"raw.{table_name} has {rows_loaded} row(s) after load. "
                        "Transaction will be rolled back."
                    )
                return STATUS_SUCCESS, rows_loaded

    # --- Load (retried only on transient DB failures) -----------------------
    try:
        status, rows_loaded = _run_with_retry(_do_load, dataset_name=dataset_name)
        result.status = status
        result.rows_loaded = rows_loaded
        result.duration_seconds = time.monotonic() - t0
        _write_audit_safe(
            AuditRecord(
                ingestion_run_id=ingestion_run_id,
                batch_id=batch_id,
                dataset_name=dataset_name,
                source_file=source_file,
                file_checksum=checksum,
                started_at=started_at,
                completed_at=datetime.now(timezone.utc),
                duration_seconds=result.duration_seconds,
                rows_read=result.rows_read,
                rows_loaded=result.rows_loaded,
                validation_status="PASS",
                load_status=(
                    "LOADED" if status == STATUS_SUCCESS else "SKIPPED_UNCHANGED"
                ),
                status=status,
                error_message=None,
            )
        )
        return result
    except Exception as exc:  # noqa: BLE001 - top-level per-dataset error boundary
        result.status = STATUS_FAILED
        result.error_message = str(exc)
        result.duration_seconds = time.monotonic() - t0
        _write_audit_safe(
            AuditRecord(
                ingestion_run_id=ingestion_run_id,
                batch_id=batch_id,
                dataset_name=dataset_name,
                source_file=source_file,
                file_checksum=checksum,
                started_at=started_at,
                completed_at=datetime.now(timezone.utc),
                duration_seconds=result.duration_seconds,
                rows_read=result.rows_read,
                rows_loaded=None,
                validation_status="PASS",
                load_status="FAILED",
                status=STATUS_FAILED,
                error_message=result.error_message,
            )
        )
        return result


def _truncate_sql(table_name: str):
    from psycopg2 import sql

    return sql.SQL("TRUNCATE TABLE {table}").format(
        table=sql.Identifier(RAW_SCHEMA_NAME, table_name)
    )


def _count_sql(table_name: str):
    from psycopg2 import sql

    return sql.SQL("SELECT count(*) FROM {table}").format(
        table=sql.Identifier(RAW_SCHEMA_NAME, table_name)
    )


# ---------------------------------------------------------------------------
# 6. Run-level orchestration (all 6 datasets, one batch_id/run_id)
# ---------------------------------------------------------------------------


def generate_batch_id(run_date: datetime | None = None) -> str:
    """Batch id groups every dataset loaded by the same daily batch run."""
    run_date = run_date or datetime.now(timezone.utc)
    return f"BATCH-{run_date:%Y%m%d}"


def generate_ingestion_run_id() -> str:
    """Run id identifies one specific pipeline execution/attempt."""
    return f"RUN-{uuid.uuid4()}"


@dataclass
class RunSummary:
    batch_id: str
    ingestion_run_id: str
    started_at: datetime
    completed_at: datetime | None = None
    dataset_results: dict[str, DatasetIngestionResult] = field(default_factory=dict)
    missing_files: list[str] = field(default_factory=list)
    unknown_files: list[str] = field(default_factory=list)

    @property
    def all_succeeded(self) -> bool:
        return all(
            r.status in (STATUS_SUCCESS, STATUS_SKIPPED_UNCHANGED)
            for r in self.dataset_results.values()
        ) and not self.missing_files


def run_ingestion(
    data_dir: str = DEFAULT_DATA_DIR,
    contracts_dir: str = DEFAULT_CONTRACTS_DIR,
    *,
    batch_id: str | None = None,
    ingestion_run_id: str | None = None,
) -> RunSummary:
    """Run the full Daily Full Batch ingestion for all six datasets.

    Every dataset is independent (requirement 8): a missing file or a
    failed load for one dataset does not stop or roll back any other
    dataset. Returns a RunSummary with a per-dataset result.
    """
    contracts = load_all_contracts(contracts_dir)
    discovery = discover_source_files(data_dir, contracts)

    resolved_batch_id = batch_id or generate_batch_id()
    resolved_run_id = ingestion_run_id or generate_ingestion_run_id()

    summary = RunSummary(
        batch_id=resolved_batch_id,
        ingestion_run_id=resolved_run_id,
        started_at=datetime.now(timezone.utc),
        missing_files=discovery.missing,
        unknown_files=discovery.unknown,
    )

    if discovery.unknown:
        logger.warning(
            "run_id=%s unknown file(s) in %s (ignored, not ingested): %s",
            resolved_run_id,
            data_dir,
            discovery.unknown,
        )

    for dataset_name, contract in contracts.items():
        if dataset_name not in discovery.found:
            logger.error(
                "run_id=%s dataset=%s FAILED: expected source file missing (%s)",
                resolved_run_id,
                dataset_name,
                contract.source_file,
            )
            missing_started = datetime.now(timezone.utc)
            result = DatasetIngestionResult(
                dataset_name=dataset_name,
                status=STATUS_FAILED,
                batch_id=resolved_batch_id,
                ingestion_run_id=resolved_run_id,
                error_message=f"Expected source file missing: {contract.source_file}",
                duration_seconds=0.0,
            )
            _write_audit_safe(
                AuditRecord(
                    ingestion_run_id=resolved_run_id,
                    batch_id=resolved_batch_id,
                    dataset_name=dataset_name,
                    source_file=contract.source_file,
                    file_checksum=None,
                    started_at=missing_started,
                    completed_at=missing_started,
                    duration_seconds=0.0,
                    rows_read=None,
                    rows_loaded=None,
                    validation_status="FAIL",
                    load_status=None,
                    status=STATUS_FAILED,
                    error_message=result.error_message,
                )
            )
            summary.dataset_results[dataset_name] = result
            continue

        file_path = discovery.found[dataset_name]
        logger.info(
            "run_id=%s dataset=%s starting ingestion (file=%s)",
            resolved_run_id,
            dataset_name,
            file_path,
        )
        result = load_dataset(
            dataset_name,
            contract,
            file_path,
            batch_id=resolved_batch_id,
            ingestion_run_id=resolved_run_id,
        )
        summary.dataset_results[dataset_name] = result
        logger.info(
            "run_id=%s dataset=%s finished status=%s rows_read=%s rows_loaded=%s",
            resolved_run_id,
            dataset_name,
            result.status,
            result.rows_read,
            result.rows_loaded,
        )

    summary.completed_at = datetime.now(timezone.utc)
    return summary


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def main() -> int:
    _configure_logging()
    try:
        summary = run_ingestion()
    except (DatabaseConfigError,) as exc:
        logger.error("run FAILED (configuration): %s", exc)
        return 1
    except Exception as exc:  # noqa: BLE001 - top-level CLI error boundary
        logger.error("run FAILED (unexpected error): %s", exc)
        return 1

    print(f"batch_id={summary.batch_id} ingestion_run_id={summary.ingestion_run_id}")
    if summary.missing_files:
        print(f"MISSING FILES: {summary.missing_files}")
    if summary.unknown_files:
        print(f"UNKNOWN FILES (ignored): {summary.unknown_files}")
    for name, result in summary.dataset_results.items():
        print(
            f"  {name}: {result.status} "
            f"rows_read={result.rows_read} rows_loaded={result.rows_loaded} "
            f"error={result.error_message}"
        )
    return 0 if summary.all_succeeded else 1


if __name__ == "__main__":
    raise SystemExit(main())

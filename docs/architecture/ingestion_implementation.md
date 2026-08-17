# Step 3.3.3 — Python Raw Ingestion Implementation

This document describes what was actually built in this step: a
configuration-driven Python framework that loads the six source CSVs into
their `raw.*` PostgreSQL tables. It documents the implementation as it
exists in the repository today — not future/planned work.

See also: `docs/architecture/raw_schema.md` (Step 3.3.2 — table structure)
and `docs/decision_log/DECISIONS.md` (approved decisions, including the
ones this step made concrete).

## 1. Scope

Implemented in this step:

- `ingestion/checksum.py` — SHA-256 checksum of complete source-file bytes.
- `ingestion/audit.py` — `raw.ingestion_audit` insert + latest-successful-
  checksum lookup helpers.
- `ingestion/raw_loader.py` — the ingestion framework itself: discovery,
  per-dataset orchestration (`load_dataset`), retry/backoff, and the
  run-level entry point (`run_ingestion` / `python -m ingestion.raw_loader`).
- `tests/test_raw_loader.py` — unit tests (no DB required) and live-database
  integration tests (skip, not fail, if PostgreSQL is unreachable).

Explicitly NOT implemented (future steps, per the approved scope):

- CDC or incremental loading — this is checksum-based idempotency only.
- Deduplication, cleaning, transformation, aggregation, or renaming of any
  source business column.
- Orchestration (Airflow, dbt, cron) — this step provides one runnable
  entry point (`python -m ingestion.raw_loader`), not a scheduler.
- Production or cloud deployment of any kind.

## 2. Reused, not duplicated

Per requirement C/D, this step reuses existing components as-is and adds no
competing implementation of any of them:

| Component | Reused from | This step's role |
|---|---|---|
| DB connection / transaction | `database/connection.py` (`transaction()`, `get_connection()`) | Called, never reimplemented. |
| Raw schema / table structure | `database/schema/raw_schema.sql`, `database/init_db.py` | Not modified. `load_dataset` truncates/loads into tables it assumes already exist (created by `init_db.py`). |
| Contract loading | `ingestion/validation/contract_loader.py` | `load_all_contracts` / `load_contract` called as-is. No second YAML parser. |
| Schema/contract validation | `ingestion/validation/schema_validator.py` | `validate_file` called as-is, first step of `load_dataset`. No duplicate validation logic. |

## 3. Source-to-table mapping (config, not six loaders)

`ingestion/raw_loader.RAW_TABLE_NAMES` is the single mapping of
`dataset_name -> raw table name`; combined with each dataset's YAML
contract (which already declares `dataset_name` and `source_file`), this
is the entire "configuration" driving the framework. There is one function,
`load_dataset(dataset_name, contract, file_path, ...)`, called once per
dataset — not six separate loader functions.

| dataset_name | source_file | raw table |
|---|---|---|
| users | users.csv | raw.users |
| products | products.csv | raw.products |
| orders | orders.csv | raw.orders |
| order_items | order_items.csv | raw.order_items |
| reviews | reviews.csv | raw.reviews |
| events | events.csv | raw.events |

## 4. Ingestion flow

For the whole run (`run_ingestion`):

1. Load all six contracts (`contract_loader.load_all_contracts`).
2. `discover_source_files` — match `data/raw_source/*.csv` against each
   contract's declared `source_file` by name only (never opens/reads
   content at this stage). Produces `found`, `missing`, and `unknown`
   (present but undeclared) file lists.
3. Generate one `batch_id` and one `ingestion_run_id` for the whole run
   (shared by every dataset in it — see §6).
4. For each dataset with a file found, call `load_dataset` (§5). A dataset
   with a missing file never reaches `load_dataset` — it is recorded as
   `FAILED` directly, with an audit row, and every other dataset still
   proceeds independently.
5. Unknown files are logged as a warning and otherwise ignored — they are
   never loaded into any table (there is no contract to validate them
   against).

For one dataset (`load_dataset`):

```
validate_file(contract, file_path)      # contract/schema validation
        |
   invalid? --> FAILED, audit row, return  (no checksum, no DB touched)
        |
   valid
        |
compute_file_checksum(file_path)         # SHA-256 of full file bytes
        |
   (inside one transaction, with retry on transient DB errors only)
   compare checksum to latest SUCCESS checksum for this dataset
        |
   unchanged? --> SKIPPED_UNCHANGED, audit row, return (no truncate/load)
        |
   changed
        |
   TRUNCATE raw.<table>
   bulk INSERT every source column + 4 metadata columns, unmodified
   SELECT count(*) and compare to source row_count
        |
   mismatch? --> raise, transaction rolls back (prior snapshot preserved),
                 audit row (FAILED), return
        |
   match --> commit, audit row (SUCCESS), return
```

## 5. Checksum implementation

`ingestion/checksum.py::compute_file_checksum` streams the file in 1 MB
chunks and returns `hashlib.sha256(...).hexdigest()` of the **complete raw
bytes** of the file — not of parsed rows, not normalized in any way. This
means the checksum is sensitive to any byte-level change (including one
that wouldn't affect any row's values), which is the correct behavior for
"did this exact file change since last time" — the explicitly approved
idempotency mechanism (requirement 3), not CDC.

## 6. Idempotency implementation

- `batch_id = f"BATCH-{run_date:%Y%m%d}"` — one per calendar day, shared by
  every dataset processed in the same `run_ingestion()` call. Matches the
  Raw-schema doc's definition: "groups all rows loaded in the same
  ingestion batch."
- `ingestion_run_id = f"RUN-{uuid4()}"` — one per pipeline **execution**,
  also shared by every dataset in that execution, but unique per attempt
  (so two runs on the same day, e.g. a manual rerun, get the same
  `batch_id` but different `ingestion_run_id`s). Matches: "identifies the
  specific ingestion run/execution."
- Before truncating, `load_dataset` looks up the checksum of the most
  recent **SUCCESS**-status audit row for that dataset
  (`audit.get_latest_successful_checksum`). If it matches the file's
  current checksum, the dataset is marked `SKIPPED_UNCHANGED` and neither
  `TRUNCATE` nor any `INSERT` runs. A prior `FAILED` or
  `SKIPPED_UNCHANGED` run is never used as the comparison basis — only a
  true prior success counts, so a failure can never mask the need to
  reload.

## 7. Transaction strategy

Each dataset's load runs inside exactly one `database.connection.transaction()`
block (its own connection, its own commit/rollback) opened inside
`load_dataset`. Datasets are processed sequentially in a Python loop in
`run_ingestion`, each with its own `transaction()` call — so an exception
in one dataset's transaction cannot affect another dataset's already-
committed (or not-yet-started) transaction. This is verified by
`tests/test_raw_loader.py::TestAuditRecordsAndIndependentTransactions`.

Audit-row writes (`_write_audit_safe`) are **deliberately a separate
transaction** from the load itself. If the load transaction rolls back
(validation failure, row-count mismatch, exhausted retries), the audit row
documenting that failure still needs to survive — so it is written after
the load transaction has already resolved (committed or rolled back), in
its own connection.

## 8. Retry strategy

`_run_with_retry` wraps only the DB-touching portion of a dataset's load
(the `_do_load` closure: checksum comparison, truncate, insert,
reconciliation). It retries up to `MAX_RETRY_ATTEMPTS = 3` times, with
exponential backoff (`RETRY_BACKOFF_BASE_SECONDS * 2^(attempt-1)`, i.e.
1s, then 2s), and only for exceptions classified as transient by
`_is_transient_db_error`:

- `database.connection.DatabaseConnectionError`
- `psycopg2.OperationalError` (checked via a lazy import, so this module
  still imports cleanly in environments without `psycopg2` installed)

Any other exception (contract validation failure, a row-count-mismatch
`DeterministicIngestionError`, programming errors) propagates immediately
on the first occurrence — never retried, per requirement 7.

## 9. Error handling

- **Contract/schema validation failure** — deterministic, not retried,
  dataset marked `FAILED`, audit row written, checksum never computed.
- **Missing source file** — deterministic, dataset marked `FAILED`
  immediately in `run_ingestion` (never reaches `load_dataset`), audit row
  written with `rows_read=NULL`, `file_checksum=NULL`.
- **Unknown (undeclared) file present** — logged as a warning at the
  run level; does not fail the run and is never loaded.
- **Row-count reconciliation mismatch** — raised as
  `DeterministicIngestionError` **inside** the load transaction, so the
  `TRUNCATE` (transactional in PostgreSQL) and any partial `INSERT` are
  rolled back together; the table is left exactly as it was before this
  attempt, not empty or partially loaded.
- **Transient DB failure** (connection drop, `OperationalError`) — retried
  per §8; if retries are exhausted, the dataset is marked `FAILED` with the
  underlying error message.
- **Audit-write failure itself** (e.g. DB unreachable when trying to write
  the audit row) — logged, swallowed, never raised; it does not change or
  mask the dataset's already-determined result.

## 10. Audit implementation

`ingestion/audit.py` provides `insert_audit_record` (parameterized INSERT
into `raw.ingestion_audit`, using the table created in Step 3.3.2 — no
schema changes) and `get_latest_successful_checksum`. `load_dataset` and
`run_ingestion` write exactly one audit row per dataset per run, in every
outcome: `SUCCESS`, `SKIPPED_UNCHANGED`, or `FAILED` (whether the failure
was a missing file, a validation failure, or an exhausted-retries DB
failure).

## 11. Logging

`ingestion/raw_loader.py` uses the standard `logging` module
(`logging.getLogger("ingestion.raw_loader")`). `run_ingestion` logs
start/finish per dataset (INFO), unknown files (WARNING), and missing
files / retries / exhausted-retries (WARNING/ERROR). `main()` configures
`logging.basicConfig` with a structured `%(asctime)s %(levelname)s %(name)s
%(message)s` format when run as a CLI (`python -m ingestion.raw_loader`).

## 12. Tests

`tests/test_raw_loader.py` (14 tests):

- No DB required (10 tests, always run): checksum stability/correctness/
  sensitivity, file discovery (found/missing/unknown), retry-and-succeed,
  retry-exhausted, deterministic-not-retried, source-fidelity (row values
  pass through unchanged; empty field becomes NULL), deterministic
  validation failure returns `FAILED` without raising, audit-write failure
  is swallowed not raised.
- Live-database required (4 tests, `SkipTest` if PostgreSQL unreachable):
  successful load + metadata columns, row-count reconciliation, second-run
  `SKIPPED_UNCHANGED`, rollback-on-mismatch preserves the prior snapshot,
  audit records for both success and failure, one dataset's failure not
  rolling back another's success.

Existing tests (`test_contract_loader.py`, `test_schema_validator.py`,
`test_raw_schema.py`) were **not modified**.

## 13. Real-data ingestion — IMPLEMENTED, NOT EXECUTED

See `docs/decision_log/DECISIONS.md` and the final report for the
authoritative IMPLEMENTED / TESTED / EXECUTED breakdown. In summary: this
sandbox has no reachable PostgreSQL instance, no `psql` binary, no
`psycopg2` installed, and no network access to install either — confirmed
by direct attempts (see final report §13). All PostgreSQL-touching code
paths are implemented and covered by tests that are written to run against
a real database, but none of them have actually executed against one in
this environment. No ingestion results, row counts, or idempotency
evidence beyond what pure-Python logic (discovery, checksum, retry,
fidelity, contract validation against the real CSVs) can demonstrate
without a database are claimed.

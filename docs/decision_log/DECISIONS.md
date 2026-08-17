# Decision Log

This is the first time decisions are being collected into a single,
dedicated log file. Decisions DEC-001 through DEC-004 and DEC-007 through
DEC-009 were approved in earlier steps (3.3.1 / 3.3.2) and are already
documented in `docs/architecture/raw_schema.md`; they are summarized below
for one place to look, without re-litigating or changing them. New
decisions made *in this step* (3.3.3) continue the numbering from DEC-010.

## Earlier decisions (3.3.1 / 3.3.2), referenced here for continuity

| ID | Decision |
|---|---|
| DEC-001 | Daily Full Batch: truncate + reload. Raw holds a current snapshot, not history. |
| DEC-002 | No PK/FK constraints on the six Raw source tables — Raw preserves source fidelity as-is. |
| DEC-003 | SHA-256 checksum-based idempotency (`SKIPPED_UNCHANGED` when unchanged). Explicitly NOT CDC. |
| DEC-004 | Four Raw metadata columns: `ingestion_timestamp`, `source_file`, `batch_id`, `ingestion_run_id`. |
| DEC-007 | Retry policy: max 3 attempts, exponential backoff, transient infra/DB failures only. |
| DEC-008 | Business timestamp columns stored as `TIMESTAMP` (no timezone) — no timezone is asserted that wasn't in the source. |
| DEC-009 | `raw.ingestion_audit` is allowed a surrogate `BIGSERIAL PRIMARY KEY` — it is operational metadata, not a source mirror, so DEC-002's fidelity rationale doesn't apply to it. |

Full rationale for all of the above: `docs/architecture/raw_schema.md`.

## Step 3.3.3 — Python Ingestion Implementation

### DEC-010 — `batch_id` is date-based; `ingestion_run_id` is a UUID per execution

- **Decision:** `batch_id = f"BATCH-{run_date:%Y%m%d}"`, one per calendar
  day, shared by all six datasets processed together.
  `ingestion_run_id = f"RUN-{uuid4()}"`, unique per pipeline execution
  (also shared across all six datasets in that one execution).
- **Rationale:** matches the two columns' documented meanings in
  `raw_schema.md` — `batch_id` "groups all rows loaded in the same
  ingestion batch" (the day), `ingestion_run_id` "identifies the specific
  ingestion run/execution" (the attempt). A same-day rerun gets a new
  `ingestion_run_id` but the same `batch_id`.
- **Status:** Implemented. Not independently re-approved beyond this log —
  flagged for review alongside the rest of this step (see final report
  §18).

### DEC-011 — Audit-record writes are a separate transaction from the load

- **Decision:** `_write_audit_safe` always opens its own
  `database.connection.transaction()`, independent of the transaction used
  to truncate/load a dataset.
- **Rationale:** if the load transaction rolls back (validation failure,
  row-count mismatch, exhausted retries), the audit row documenting that
  outcome must still be written and committed — otherwise a failure would
  be invisible in `raw.ingestion_audit`.
- **Status:** Implemented, unit-tested (`test_audit_write_failure_is_logged_not_raised`).

### DEC-012 — Row-count mismatch rolls back the entire load, including the TRUNCATE

- **Decision:** row-count reconciliation happens *inside* the same
  transaction as the `TRUNCATE` + `INSERT`. A mismatch raises, which rolls
  back the whole transaction — so a failed reload leaves the table exactly
  as it was before the attempt (the prior successful snapshot), not empty
  or partially loaded.
- **Rationale:** PostgreSQL's `TRUNCATE` is transactional; using that
  guarantee means a failed load never leaves Raw in a worse state than
  before the attempt.
- **Status:** Implemented, designed to be verified by
  `TestRollbackOnMismatch` (live-DB test; not executed in this environment
  — see final report §13/§16).

### DEC-013 — `SKIPPED_UNCHANGED` comparison uses only the latest SUCCESS checksum

- **Decision:** `get_latest_successful_checksum` filters to
  `status = 'SUCCESS'` only when looking up the checksum to compare
  against.
- **Rationale:** a prior `FAILED` or `SKIPPED_UNCHANGED` audit row must
  never be used as the "last known good" checksum — only a genuine prior
  success counts, so a failure can never mask the need to reload on the
  next run.
- **Status:** Implemented.

### DEC-014 — Transient-failure classification is narrow and explicit

- **Decision:** only genuine transient `database.connection.DatabaseConnectionError`
  (excluding `MissingDependencyError`) and `psycopg2.OperationalError` are
  treated as retryable; every other exception fails immediately without
  retry, including:
  - contract validation failures
  - `DeterministicIngestionError` (e.g. row-count mismatch)
  - `database.connection.DatabaseConfigError`
  - `database.connection.MissingDependencyError` (missing psycopg2 dependency
    is deterministic and must not be retried)
  - bare `ImportError` if it reaches the retry classifier
  - programming errors
- **Rationale:** requirement 7 is explicit that deterministic
  validation/schema failures must not be retried — retrying them would
  waste attempts on failures no retry can fix. A missing database driver
  is an environment/setup problem, not a transient outage; retrying it
  would delay failure without improving outcomes.
- **Implementation:** `get_connection()` raises `MissingDependencyError`
  (subclass of `DatabaseConnectionError`) when psycopg2 is unavailable;
  `_is_transient_db_error` in `ingestion/raw_loader.py` excludes
  `MissingDependencyError`, `DatabaseConfigError`, `DeterministicIngestionError`,
  and bare `ImportError` from retry.
- **Status:** Implemented, unit-tested (no DB required —
  `TestRetryBehavior`).

## Open items requiring explicit approval

None of DEC-010 through DEC-014 change any previously approved decision
(DEC-001 through DEC-009); they are implementation choices made to realize
those decisions in code. They have not been separately signed off beyond
being recorded here — see final report §18 for the full list of what
still needs approval before Step 3.3.4.

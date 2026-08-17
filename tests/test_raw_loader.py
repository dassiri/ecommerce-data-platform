"""Tests for Step 3.3.3 — Python Raw Ingestion Implementation.

Two kinds of tests live here:

1. Pure-logic tests (discovery, checksum, retry/backoff, source fidelity,
   deterministic-validation-failure handling) — no PostgreSQL required,
   always run.

2. Live-database tests (successful load, row reconciliation, metadata
   columns, SKIPPED_UNCHANGED, rollback, audit records, independent
   transactions) — require a reachable PostgreSQL instance. Following the
   same convention as tests/test_raw_schema.py, these SKIP (not fail) when
   no database is reachable, and report why.

   NOTE: the live-database tests intentionally TRUNCATE + reload
   raw.users using small synthetic fixtures (tests/fixtures/users_*.csv),
   never the six real source CSVs (requirement H). Run them against a
   dev/test database, not a database you rely on containing the real
   10,000-row snapshot at the time you run this suite — a subsequent real
   ingestion run (`python -m ingestion.raw_loader`) will restore it.
"""

from __future__ import annotations

import os
import unittest
from datetime import datetime, timezone
from unittest import mock

from database.connection import (
    DatabaseConfigError,
    DatabaseConnectionError,
    close_connection,
    get_connection,
)
from database.init_db import init_raw_schema
from ingestion.checksum import compute_file_checksum
from ingestion.validation.contract_loader import load_contract
import ingestion.raw_loader as rl

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONTRACTS_DIR = os.path.join(PROJECT_ROOT, "ingestion", "contracts")
FIXTURES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures")
USERS_CONTRACT_PATH = os.path.join(CONTRACTS_DIR, "users_contract.yml")


def _users_contract():
    return load_contract(USERS_CONTRACT_PATH)


# ---------------------------------------------------------------------------
# 1. Checksum
# ---------------------------------------------------------------------------


class TestChecksum(unittest.TestCase):
    def test_checksum_is_stable_for_unchanged_file(self):
        path = os.path.join(FIXTURES_DIR, "users_valid.csv")
        self.assertEqual(compute_file_checksum(path), compute_file_checksum(path))

    def test_checksum_matches_known_sha256(self):
        import hashlib

        path = os.path.join(FIXTURES_DIR, "users_valid.csv")
        with open(path, "rb") as fh:
            expected = hashlib.sha256(fh.read()).hexdigest()
        self.assertEqual(compute_file_checksum(path), expected)

    def test_checksum_changes_when_file_content_changes(self):
        path_a = os.path.join(FIXTURES_DIR, "users_valid.csv")
        path_b = os.path.join(FIXTURES_DIR, "_checksum_variant.csv")
        with open(path_a, "r", encoding="utf-8") as fh:
            content = fh.read()
        with open(path_b, "w", encoding="utf-8") as fh:
            fh.write(content + "\n")  # trivial byte-level change
        try:
            self.assertNotEqual(
                compute_file_checksum(path_a), compute_file_checksum(path_b)
            )
        finally:
            os.remove(path_b)


# ---------------------------------------------------------------------------
# 2. Discovery
# ---------------------------------------------------------------------------


class TestDiscovery(unittest.TestCase):
    def setUp(self):
        self.contracts = {"users": _users_contract()}

    def test_finds_expected_file(self):
        result = rl.discover_source_files(FIXTURES_DIR, self.contracts)
        # users.csv itself isn't in fixtures/ (only users_*.csv variants),
        # so this should be reported missing, not found.
        self.assertEqual(result.found, {})
        self.assertIn("users.csv", result.missing)

    def test_reports_unknown_files(self):
        result = rl.discover_source_files(FIXTURES_DIR, self.contracts)
        self.assertIn("users_valid.csv", result.unknown)
        self.assertIn("users_invalid_values.csv", result.unknown)

    def test_finds_file_when_present(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            open(os.path.join(tmp, "users.csv"), "w").close()
            result = rl.discover_source_files(tmp, self.contracts)
            self.assertEqual(result.found["users"], os.path.join(tmp, "users.csv"))
            self.assertEqual(result.missing, [])

    def test_missing_data_dir_reports_all_missing_not_a_crash(self):
        result = rl.discover_source_files("/no/such/dir", self.contracts)
        self.assertEqual(result.found, {})
        self.assertEqual(result.missing, ["users.csv"])
        self.assertEqual(result.unknown, [])


# ---------------------------------------------------------------------------
# 3. Retry / backoff (no DB required — uses DatabaseConnectionError directly)
# ---------------------------------------------------------------------------


class TestRetryBehavior(unittest.TestCase):
    def test_transient_error_is_retried_then_succeeds(self):
        calls = {"n": 0}

        def flaky():
            calls["n"] += 1
            if calls["n"] < 3:
                raise DatabaseConnectionError("simulated transient failure")
            return "ok"

        sleeps = []
        result = rl._run_with_retry(
            flaky, dataset_name="users", sleep_fn=sleeps.append
        )
        self.assertEqual(result, "ok")
        self.assertEqual(calls["n"], 3)
        self.assertEqual(sleeps, [1.0, 2.0])  # exponential backoff

    def test_transient_error_exhausts_retries_and_raises(self):
        calls = {"n": 0}

        def always_fails():
            calls["n"] += 1
            raise DatabaseConnectionError("still down")

        with self.assertRaises(DatabaseConnectionError):
            rl._run_with_retry(
                always_fails, dataset_name="users", sleep_fn=lambda s: None
            )
        self.assertEqual(calls["n"], rl.MAX_RETRY_ATTEMPTS)

    def test_deterministic_error_is_not_retried(self):
        calls = {"n": 0}

        def deterministic_failure():
            calls["n"] += 1
            raise rl.DeterministicIngestionError("row count mismatch")

        with self.assertRaises(rl.DeterministicIngestionError):
            rl._run_with_retry(
                deterministic_failure, dataset_name="users", sleep_fn=lambda s: None
            )
        self.assertEqual(calls["n"], 1)  # never retried


# ---------------------------------------------------------------------------
# 4. Source fidelity (no clean/dedupe/rename of business columns)
# ---------------------------------------------------------------------------


class TestSourceFidelity(unittest.TestCase):
    def test_row_values_pass_through_unchanged_except_empty_to_null(self):
        contract = _users_contract()
        path = os.path.join(FIXTURES_DIR, "users_valid.csv")
        ts = datetime(2026, 8, 17, tzinfo=timezone.utc)
        rows = list(
            rl._iter_rows_for_insert(
                contract,
                path,
                ingestion_timestamp=ts,
                source_file="users.csv",
                batch_id="BATCH-20260817",
                ingestion_run_id="RUN-test",
            )
        )
        self.assertEqual(len(rows), 3)
        # First data row, source columns only (metadata appended after).
        first = rows[0]
        self.assertEqual(
            first[:6],
            ("U000001", "Test One", "test1@example.com", "Male", "Testville", "2024-01-01"),
        )
        # Metadata columns appended, untouched values.
        self.assertEqual(first[6:], (ts, "users.csv", "BATCH-20260817", "RUN-test"))

    def test_empty_field_becomes_null_not_empty_string(self):
        contract = _users_contract()
        path = os.path.join(FIXTURES_DIR, "users_invalid_values.csv")
        ts = datetime.now(timezone.utc)
        rows = list(
            rl._iter_rows_for_insert(
                contract,
                path,
                ingestion_timestamp=ts,
                source_file="users.csv",
                batch_id="b",
                ingestion_run_id="r",
            )
        )
        # name column is empty in this fixture row.
        self.assertIsNone(rows[0][1])


# ---------------------------------------------------------------------------
# 5. Deterministic validation failure (no DB needed to observe the result;
#    audit-write failure due to no DB must be swallowed, not raised)
# ---------------------------------------------------------------------------


class TestDeterministicValidationFailure(unittest.TestCase):
    def test_invalid_file_returns_failed_without_raising(self):
        contract = _users_contract()
        path = os.path.join(FIXTURES_DIR, "users_invalid_values.csv")
        result = rl.load_dataset(
            "users",
            contract,
            path,
            batch_id="BATCH-TEST",
            ingestion_run_id="RUN-TEST",
        )
        self.assertEqual(result.status, rl.STATUS_FAILED)
        self.assertIsNotNone(result.error_message)
        self.assertIsNone(result.checksum)  # never reached checksum step

    def test_audit_write_failure_is_logged_not_raised(self):
        # Force the DB-config path to fail deterministically, independent of
        # whether a real .env / Postgres happens to be present in this
        # environment, and confirm _write_audit_safe swallows it.
        with mock.patch(
            "ingestion.raw_loader.transaction",
            side_effect=DatabaseConfigError("no config"),
        ):
            ok = rl._write_audit_safe(
                rl.AuditRecord(
                    ingestion_run_id="r",
                    batch_id="b",
                    dataset_name="users",
                    source_file="users.csv",
                    file_checksum=None,
                    started_at=datetime.now(timezone.utc),
                    completed_at=None,
                    duration_seconds=None,
                    rows_read=None,
                    rows_loaded=None,
                    validation_status="FAIL",
                    load_status=None,
                    status=rl.STATUS_FAILED,
                    error_message="x",
                )
            )
        self.assertFalse(ok)


# ---------------------------------------------------------------------------
# 6. Live-database tests (skipped if PostgreSQL is unreachable)
# ---------------------------------------------------------------------------


def _try_connect():
    try:
        conn = get_connection()
    except (DatabaseConfigError, DatabaseConnectionError):
        return None
    return conn


class LiveDbTestBase(unittest.TestCase):
    conn = None

    @classmethod
    def setUpClass(cls):
        cls.conn = _try_connect()
        if cls.conn is None:
            raise unittest.SkipTest(
                "No reachable PostgreSQL instance (check POSTGRES_HOST / "
                "POSTGRES_PORT / POSTGRES_DB / POSTGRES_USER / "
                "POSTGRES_PASSWORD and that Docker/Postgres is running). "
                "Live raw-ingestion tests are skipped, not failed, in this "
                "environment."
            )
        init_raw_schema()

    @classmethod
    def tearDownClass(cls):
        close_connection(cls.conn)

    def _row_count(self, table_name: str) -> int:
        with self.conn.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM raw.{table_name}")
            return cur.fetchone()[0]

    def _audit_rows(self, run_id: str) -> list[tuple]:
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT dataset_name, status, rows_read, rows_loaded, "
                "file_checksum FROM raw.ingestion_audit "
                "WHERE ingestion_run_id = %s ORDER BY audit_id",
                (run_id,),
            )
            return cur.fetchall()


class TestSuccessfulLoadAndMetadata(LiveDbTestBase):
    def test_successful_load_populates_rows_and_metadata(self):
        contract = _users_contract()
        path = os.path.join(FIXTURES_DIR, "users_valid.csv")
        run_id = f"RUN-test-success-{id(self)}"
        result = rl.load_dataset(
            "users", contract, path, batch_id="BATCH-TEST", ingestion_run_id=run_id
        )
        self.assertEqual(result.status, rl.STATUS_SUCCESS)
        self.assertEqual(result.rows_read, 3)
        self.assertEqual(result.rows_loaded, 3)
        self.assertEqual(self._row_count("users"), 3)

        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT batch_id, source_file, ingestion_run_id "
                "FROM raw.users"
            )
            rows = cur.fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0], ("BATCH-TEST", "users_valid.csv", run_id))

    def test_row_count_reconciliation_matches_source(self):
        contract = _users_contract()
        path = os.path.join(FIXTURES_DIR, "users_valid.csv")
        result = rl.load_dataset(
            "users",
            contract,
            path,
            batch_id="BATCH-TEST",
            ingestion_run_id=f"RUN-recon-{id(self)}",
        )
        self.assertEqual(result.rows_read, result.rows_loaded)


class TestSkippedUnchanged(LiveDbTestBase):
    def test_second_identical_run_is_skipped_unchanged(self):
        contract = _users_contract()
        path = os.path.join(FIXTURES_DIR, "users_valid.csv")

        first = rl.load_dataset(
            "users",
            contract,
            path,
            batch_id="BATCH-A",
            ingestion_run_id=f"RUN-first-{id(self)}",
        )
        self.assertEqual(first.status, rl.STATUS_SUCCESS)

        second = rl.load_dataset(
            "users",
            contract,
            path,
            batch_id="BATCH-A",
            ingestion_run_id=f"RUN-second-{id(self)}",
        )
        self.assertEqual(second.status, rl.STATUS_SKIPPED_UNCHANGED)
        # Data untouched by the skip — still exactly the first load's rows.
        self.assertEqual(self._row_count("users"), 3)


class TestRollbackOnMismatch(LiveDbTestBase):
    def test_row_count_mismatch_rolls_back_and_preserves_prior_snapshot(self):
        contract = _users_contract()
        path = os.path.join(FIXTURES_DIR, "users_valid.csv")

        # Seed a known-good snapshot first.
        seed = rl.load_dataset(
            "users",
            contract,
            path,
            batch_id="BATCH-SEED",
            ingestion_run_id=f"RUN-seed-{id(self)}",
        )
        self.assertEqual(seed.status, rl.STATUS_SUCCESS)
        seeded_count = self._row_count("users")
        self.assertEqual(seeded_count, 3)

        # Force a reconciliation mismatch by making _bulk_insert insert one
        # fewer row than the source actually has (simulated fault).
        real_bulk_insert = rl._bulk_insert

        def _faulty_bulk_insert(cur, table_name, contract_, file_path, **kwargs):
            # Insert everything, then delete one row back out inside the
            # SAME transaction, so reconciliation sees a mismatch.
            real_bulk_insert(cur, table_name, contract_, file_path, **kwargs)
            cur.execute(f"DELETE FROM raw.{table_name} WHERE ctid = ("
                        f"SELECT ctid FROM raw.{table_name} LIMIT 1)")

        with mock.patch("ingestion.raw_loader._bulk_insert", _faulty_bulk_insert):
            # Different checksum needed or it would be SKIPPED_UNCHANGED.
            variant_path = os.path.join(FIXTURES_DIR, "_rollback_variant.csv")
            with open(path, "r", encoding="utf-8") as fh:
                content = fh.read()
            with open(variant_path, "w", encoding="utf-8") as fh:
                fh.write(content)  # identical content, new file -> still
                # same checksum as `path`; force difference by editing city.
            try:
                with open(variant_path, "w", encoding="utf-8") as fh:
                    fh.write(content.replace("Testville", "Otherville"))

                result = rl.load_dataset(
                    "users",
                    contract,
                    variant_path,
                    batch_id="BATCH-FAULT",
                    ingestion_run_id=f"RUN-fault-{id(self)}",
                )
            finally:
                os.remove(variant_path)

        self.assertEqual(result.status, rl.STATUS_FAILED)
        self.assertIn("Row-count reconciliation failed", result.error_message)
        # Rollback must have restored the prior snapshot, not left a
        # truncated/partial table.
        self.assertEqual(self._row_count("users"), seeded_count)


class TestAuditRecordsAndIndependentTransactions(LiveDbTestBase):
    def test_audit_record_written_for_success(self):
        contract = _users_contract()
        path = os.path.join(FIXTURES_DIR, "users_valid.csv")
        run_id = f"RUN-audit-{id(self)}"
        rl.load_dataset(
            "users", contract, path, batch_id="BATCH-AUDIT", ingestion_run_id=run_id
        )
        rows = self._audit_rows(run_id)
        self.assertEqual(len(rows), 1)
        dataset_name, status, rows_read, rows_loaded, checksum = rows[0]
        self.assertEqual(dataset_name, "users")
        self.assertEqual(status, rl.STATUS_SUCCESS)
        self.assertEqual(rows_read, 3)
        self.assertEqual(rows_loaded, 3)
        self.assertIsNotNone(checksum)

    def test_audit_record_written_for_failure(self):
        contract = _users_contract()
        path = os.path.join(FIXTURES_DIR, "users_invalid_values.csv")
        run_id = f"RUN-audit-fail-{id(self)}"
        rl.load_dataset(
            "users", contract, path, batch_id="BATCH-AUDIT", ingestion_run_id=run_id
        )
        rows = self._audit_rows(run_id)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][1], rl.STATUS_FAILED)

    def test_one_failed_dataset_does_not_roll_back_a_successful_one(self):
        # "users" succeeds; a second, independent load attempt for the same
        # dataset with bad data fails. Each uses its own transaction
        # (its own `with transaction()` block inside load_dataset), so the
        # successful rows from the first call must survive the second's
        # failure.
        contract = _users_contract()
        good_path = os.path.join(FIXTURES_DIR, "users_valid.csv")
        bad_path = os.path.join(FIXTURES_DIR, "users_invalid_values.csv")

        good_result = rl.load_dataset(
            "users",
            contract,
            good_path,
            batch_id="BATCH-INDEP",
            ingestion_run_id=f"RUN-good-{id(self)}",
        )
        self.assertEqual(good_result.status, rl.STATUS_SUCCESS)
        count_after_good = self._row_count("users")

        bad_result = rl.load_dataset(
            "users",
            contract,
            bad_path,
            batch_id="BATCH-INDEP",
            ingestion_run_id=f"RUN-bad-{id(self)}",
        )
        self.assertEqual(bad_result.status, rl.STATUS_FAILED)

        # The earlier successful transaction already committed; the later
        # failure (which never reached the DB, since it fails contract
        # validation first) cannot and does not undo it.
        self.assertEqual(self._row_count("users"), count_after_good)


if __name__ == "__main__":
    unittest.main()
